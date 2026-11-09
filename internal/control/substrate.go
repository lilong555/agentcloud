package control

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"github.com/lilong555/agentcloud/internal/observability"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SubstrateConfig struct {
	Client                                                                                     *substrateapi.Client
	Atespace, Template, GatewayURL, GatewayServerName, CAFile, CertFile, KeyFile, RuntimeToken string
	CoordinatorURL                                                                             string
}
type SubstrateBackend struct {
	cfg  SubstrateConfig
	http *http.Client
}

func NewSubstrateBackend(cfg SubstrateConfig) (*SubstrateBackend, error) {
	parsed, err := url.Parse(cfg.GatewayURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Substrate gateway requires an HTTPS origin")
	}
	if cfg.Client == nil || cfg.Atespace == "" || cfg.Template == "" {
		return nil, errors.New("Substrate client, atespace and template are required")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: cfg.GatewayServerName}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("gateway CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, errors.New("Substrate gateway requires mTLS credentials")
	}
	tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		return &cert, err
	}
	return &SubstrateBackend{cfg: cfg, http: &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (b *SubstrateBackend) Name() string { return "substrate" }
func (b *SubstrateBackend) call(ctx context.Context, name, path string, input, output any) (err error) {
	ctx, span := observability.Start(ctx, "substrate.runtime "+path, trace.SpanKindClient)
	defer func() { observability.Finish(span, err) }()
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	method := http.MethodPost
	if input == nil {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(b.cfg.GatewayURL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("ate-target-actor", b.cfg.Atespace+"/"+name)
	req.Header.Set("Content-Type", "application/json")
	observability.Inject(ctx, req.Header)
	if b.cfg.RuntimeToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.cfg.RuntimeToken)
	}
	response, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 2<<20 {
		return errors.New("runtime response exceeds 2 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("runtime HTTP %d: %.512s", response.StatusCode, data)
	}
	if output != nil {
		return json.Unmarshal(data, output)
	}
	return nil
}
func fromSubstrate(s substrateapi.Summary) Actor {
	return Actor{AssignedWorker: s.AssignedWorker, Name: s.Name, UID: s.UID, Version: s.Version, SnapshotUUID: s.SnapshotUUID, SnapshotGeneration: int64(s.SnapshotGeneration), ActualFidelity: s.ActualFidelity, SnapshotFidelity: s.SnapshotFidelity}
}
func (b *SubstrateBackend) attach(ctx context.Context, s *Session, a Actor, epoch uint64) error {
	// The ready endpoint reports process readiness while the tool gate remains closed.
	for {
		err := b.call(ctx, a.Name, "/readyz", nil, nil)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	manifest := ""
	if s.Manifest != nil {
		manifest = s.Manifest.ID
	}
	var receipt struct {
		Epoch            uint64 `json:"epoch"`
		Gate             string `json:"gate"`
		ActorUID         string `json:"actor_uid"`
		IdentityAttested bool   `json:"identity_attested"`
	}
	if err := b.call(ctx, a.Name, "/runtime/v1/attach", map[string]any{"epoch": epoch, "actor_uid": a.UID, "manifest_id": manifest}, &receipt); err != nil {
		return err
	}
	if receipt.Epoch != epoch || receipt.Gate != "OPEN" || receipt.ActorUID != a.UID || !receipt.IdentityAttested {
		return errors.New("runtime attach identity mismatch")
	}
	return nil
}
func (b *SubstrateBackend) Ensure(ctx context.Context, s *Session) (Actor, error) {
	name := s.Actor.Name
	if name == "" {
		name = "ac-" + payloadHash([]string{s.TenantID, s.WorkspaceID, s.SessionID})[:24]
	}
	if !validID(name) {
		return Actor{}, errors.New("invalid Actor name")
	}
	current, err := b.cfg.Client.GetActor(ctx, name)
	if status.Code(err) == codes.NotFound {
		current, err = b.cfg.Client.Create(ctx, name, b.cfg.Template)
	} else if err == nil {
		return Actor{}, errors.New("Actor name already exists; explicit reconciliation is required")
	}
	if err != nil {
		return Actor{}, err
	}
	if b.cfg.CoordinatorURL != "" {
		origin, e := url.Parse(b.cfg.CoordinatorURL)
		if e != nil || origin.Hostname() == "" || (origin.Scheme != "http" && origin.Scheme != "https") {
			return Actor{}, errors.New("invalid Actor coordinator origin")
		}
		port := uint64(80)
		if origin.Scheme == "https" {
			port = 443
		}
		if origin.Port() != "" {
			port, e = strconv.ParseUint(origin.Port(), 10, 16)
			if e != nil || port == 0 {
				return Actor{}, errors.New("invalid Actor coordinator port")
			}
		}
		if e = b.cfg.Client.AllowCoordinatorEgress(ctx, name, origin.Hostname(), uint32(port)); e != nil {
			return Actor{}, e
		}
	}
	current, err = b.cfg.Client.Resume(ctx, name, current.UID, current.Version)
	if err != nil {
		return Actor{}, err
	}
	actor := fromSubstrate(current)
	actor.BootID = newID()
	if err = b.attach(ctx, s, actor, s.Epoch); err != nil {
		return Actor{}, err
	}
	if bytes, e := b.cfg.Client.AssignedMemory(ctx, actor.Name, actor.UID); e == nil {
		actor.AssignedMemoryBytes = bytes
	} else {
		observability.Unavailable(ctx, "stranded_worker_gb_s")
	}
	return actor, nil
}
func (b *SubstrateBackend) Tool(ctx context.Context, s *Session, input ToolRequest) (ToolResult, error) {
	var receipt ToolReceipt
	if err := b.call(ctx, s.Actor.Name, "/runtime/v1/tools", input, &receipt); err != nil {
		return ToolResult{}, err
	}
	if receipt.ID != input.ID || receipt.Epoch != s.Epoch || receipt.Status != "COMPLETED" {
		return receipt.Result, errors.New("runtime tool receipt is incomplete or has another identity")
	}
	return receipt.Result, nil
}
func (b *SubstrateBackend) Checkpoint(ctx context.Context, s *Session, operation string) (Actor, string, error) {
	var barrier struct {
		OperationID string `json:"operation_id"`
		Epoch       uint64 `json:"epoch"`
		Generation  uint64 `json:"barrier_generation"`
		Watermark   uint64 `json:"receipt_watermark"`
		State       string `json:"state"`
		Gate        string `json:"gate"`
		Active      int    `json:"active_tools"`
	}
	if err := b.call(ctx, s.Actor.Name, "/runtime/v1/quiesce", map[string]any{"operation_id": operation, "expected_epoch": s.Epoch, "barrier_generation": s.BarrierGeneration}, &barrier); err != nil {
		return Actor{}, "", err
	}
	if barrier.OperationID != operation || barrier.Epoch != s.Epoch || barrier.Generation != s.BarrierGeneration || barrier.Watermark != s.ReceiptWatermark || barrier.State != "QUIESCED" || barrier.Gate != "CLOSED" || barrier.Active != 0 {
		return Actor{}, "", errors.New("runtime barrier differs from coordinator receipt watermark")
	}
	current, err := b.cfg.Client.Suspend(ctx, s.Actor.Name, s.Actor.UID, s.Actor.Version)
	if err != nil {
		return Actor{}, "", err
	}
	current, err = b.cfg.Client.AwaitDurable(ctx, current.Name, current.UID, current.SnapshotUUID, current.SnapshotGeneration)
	if err != nil {
		return Actor{}, "", err
	}
	if current.SnapshotFidelity != "MEMORY" && (s.Mode == "in-actor" || current.SnapshotFidelity != "VOLUMES") {
		return Actor{}, "", errors.New("snapshot fidelity does not satisfy session mode")
	}
	tag := "ac-snapshot-" + payloadHash([]string{s.TenantID, s.SessionID, operation})[:24]
	retained, err := b.cfg.Client.RetainSnapshot(ctx, current.Name, current.UID, current.SnapshotUUID, current.SnapshotGeneration, tag)
	if err != nil {
		return Actor{}, "", err
	}
	actor := fromSubstrate(current)
	actor.RetainedTag = retained.TagName
	actor.RetainedTagUID = retained.TagUID
	actor.ActualFidelity = current.SnapshotFidelity
	actor.BootID = s.Actor.BootID
	actor.AssignedWorker = s.Actor.AssignedWorker
	actor.AssignedMemoryBytes = s.Actor.AssignedMemoryBytes
	return actor, "", nil
}
func (b *SubstrateBackend) Suspend(ctx context.Context, s *Session) (Actor, error) {
	current, err := b.cfg.Client.GetActor(ctx, s.Actor.Name)
	if err != nil {
		return Actor{}, err
	}
	if current.UID != s.Actor.UID || current.State != "SUSPENDED" || current.SnapshotUUID != s.Actor.SnapshotUUID || int64(current.SnapshotGeneration) != s.Actor.SnapshotGeneration {
		return Actor{}, errors.New("Actor differs from the retained suspended checkpoint")
	}
	return s.Actor, nil
}
func (b *SubstrateBackend) Resume(ctx context.Context, s *Session) (Actor, error) {
	manifest := s.Manifest
	if manifest == nil || manifest.RetainedTag == "" || manifest.RetainedTagUID == "" {
		return Actor{}, errors.New("resume requires a retained snapshot manifest")
	}
	current, err := b.cfg.Client.GetActor(ctx, s.Actor.Name)
	if err != nil {
		return Actor{}, err
	}
	if current.State != "SUSPENDED" || current.UID != manifest.ActorUID || current.SnapshotUUID != manifest.SnapshotUUID || int64(current.SnapshotGeneration) != manifest.SnapshotGeneration {
		return Actor{}, errors.New("current Actor snapshot differs from manifest")
	}
	retained, err := b.cfg.Client.RetainSnapshot(ctx, current.Name, current.UID, manifest.SnapshotUUID, int32(manifest.SnapshotGeneration), manifest.RetainedTag)
	if err != nil {
		return Actor{}, err
	}
	if retained.TagUID != manifest.RetainedTagUID {
		return Actor{}, errors.New("retained tag was replaced")
	}
	restored, err := b.cfg.Client.Resume(ctx, current.Name, current.UID, current.Version)
	if err != nil {
		return Actor{}, err
	}
	if restored.ColdStarted || restored.RestoredSnapshotUUID != manifest.SnapshotUUID || int64(restored.RestoredSnapshotGeneration) != manifest.SnapshotGeneration {
		return Actor{}, errors.New("runtime restored another snapshot or cold-started")
	}
	if restored.ActualFidelity != "MEMORY" && (s.Mode == "in-actor" || restored.ActualFidelity != "VOLUMES") {
		return Actor{}, errors.New("actual restoration fidelity does not satisfy session mode")
	}
	actor := fromSubstrate(restored)
	actor.RetainedTag = retained.TagName
	actor.RetainedTagUID = retained.TagUID
	actor.BootID = newID()
	if err = b.attach(ctx, s, actor, s.Epoch+1); err != nil {
		return Actor{}, err
	}
	if bytes, e := b.cfg.Client.AssignedMemory(ctx, actor.Name, actor.UID); e == nil {
		actor.AssignedMemoryBytes = bytes
	} else {
		observability.Unavailable(ctx, "stranded_worker_gb_s")
	}
	return actor, nil
}
