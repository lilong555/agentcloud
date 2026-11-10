package substrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/lilong555/agentcloud/internal/observability"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	ServerName string `json:"server_name,omitempty"`
	TokenFile  string `json:"token_file,omitempty"`
	Address    string `json:"address"`
	Insecure   bool   `json:"insecure"`
	CAFile     string `json:"ca_file"`
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	Atespace   string `json:"atespace"`
}

type Summary struct {
	AssignedWorker             string `json:"assigned_worker,omitempty"`
	Name                       string `json:"name"`
	UID                        string `json:"uid"`
	Version                    int64  `json:"version"`
	State                      string `json:"state"`
	SnapshotUUID               string `json:"snapshot_uuid,omitempty"`
	SnapshotGeneration         int32  `json:"snapshot_generation,omitempty"`
	SnapshotFidelity           string `json:"snapshot_fidelity,omitempty"`
	Durable                    bool   `json:"durable"`
	Resumed                    bool   `json:"resumed"`
	ColdStarted                bool   `json:"cold_started"`
	ActualFidelity             string `json:"actual_fidelity,omitempty"`
	RestoredSnapshotUUID       string `json:"restored_snapshot_uuid,omitempty"`
	RestoredSnapshotGeneration int32  `json:"restored_snapshot_generation,omitempty"`
}

type Client struct {
	conn     *grpc.ClientConn
	control  ateapi.ControlClient
	atespace string
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Address == "" || cfg.Atespace == "" {
		return nil, fmt.Errorf("Substrate address and atespace are required")
	}
	if cfg.Insecure && cfg.TokenFile != "" {
		return nil, fmt.Errorf("Substrate bearer token requires TLS")
	}
	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		tc := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: cfg.ServerName}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("Substrate CA file contains no certificates")
			}
			tc.RootCAs = roots
		}
		if cfg.CertFile != "" || cfg.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
			if err != nil {
				return nil, err
			}
			tc.Certificates = []tls.Certificate{cert}
		}
		creds = credentials.NewTLS(tc)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor)}
	if cfg.TokenFile != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(fileToken(cfg.TokenFile)))
	}
	conn, err := grpc.NewClient(cfg.Address, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, control: ateapi.NewControlClient(conn), atespace: cfg.Atespace}, nil
}

func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) ref(name string) *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: c.atespace, Name: name}
}
func (c *Client) Control() ateapi.ControlClient { return c.control }

func (c *Client) GetActor(ctx context.Context, name string) (Summary, error) {
	actor, err := c.control.GetActor(ctx, &ateapi.GetActorRequest{Actor: c.ref(name)})
	return summarize(actor), err
}
func (c *Client) Create(ctx context.Context, name, template string) (Summary, error) {
	actor, err := c.control.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: c.atespace, Name: name}, ActorTemplate: c.ref(template)}})
	return summarize(actor), err
}
func (c *Client) Suspend(ctx context.Context, name, uid string, version int64) (Summary, error) {
	request := &ateapi.SuspendActorRequest{Actor: c.ref(name), ExpectedUid: uid, ExpectedVersion: version, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS}}
	resp, err := actorLeaseCall(ctx, uid, version, func() (*ateapi.SuspendActorResponse, error) {
		return c.control.SuspendActor(ctx, request)
	})
	return summarize(resp.GetActor()), err
}
func (c *Client) Resume(ctx context.Context, name, uid string, version int64) (Summary, error) {
	request := &ateapi.ResumeActorRequest{Actor: c.ref(name), ExpectedUid: uid, ExpectedVersion: version}
	resp, err := actorLeaseCall(ctx, uid, version, func() (*ateapi.ResumeActorResponse, error) {
		return c.control.ResumeActor(ctx, request)
	})
	summary := summarize(resp.GetActor())
	summary.Resumed = resp.GetResumed()
	applyRestore(&summary, resp.GetRestore())
	return summary, err
}
func (c *Client) AwaitDurable(ctx context.Context, name, uid, snapshotUUID string, generation int32) (Summary, error) {
	resp, err := c.control.WaitActorSnapshotDurable(ctx, &ateapi.WaitActorSnapshotDurableRequest{Actor: c.ref(name), ExpectedUid: uid, SnapshotUuid: snapshotUUID, SnapshotGeneration: generation})
	summary := summarize(resp.GetActor())
	applySnapshot(&summary, resp.GetSnapshot())
	return summary, err
}

func summarize(actor *ateapi.Actor) Summary {
	s := Summary{AssignedWorker: actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(), Name: actor.GetMetadata().GetName(), UID: actor.GetMetadata().GetUid(), Version: actor.GetMetadata().GetVersion(), State: strings.TrimPrefix(actor.GetStatus().GetState().String(), "ACTOR_STATE_")}
	var latest *ateapi.Snapshot
	for _, snapshot := range actor.GetStatus().GetSnapshots() {
		if latest == nil || snapshot.GetGeneration() > latest.GetGeneration() {
			latest = snapshot
		}
	}
	applySnapshot(&s, latest)
	applyRestore(&s, actor.GetStatus().GetLastRestore())
	return s
}
func applySnapshot(s *Summary, snapshot *ateapi.Snapshot) {
	if snapshot == nil {
		return
	}
	s.SnapshotUUID = snapshot.GetUuid()
	s.SnapshotGeneration = snapshot.GetGeneration()
	s.SnapshotFidelity = strings.TrimPrefix(snapshot.GetFidelity().String(), "SNAPSHOT_FIDELITY_")
	s.Durable = snapshot.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
}
func applyRestore(s *Summary, restore *ateapi.ActorRestoreResult) {
	if restore == nil {
		return
	}
	s.ColdStarted = restore.GetColdStarted()
	s.ActualFidelity = strings.TrimPrefix(restore.GetFidelity().String(), "SNAPSHOT_FIDELITY_")
	s.RestoredSnapshotUUID = restore.GetSnapshotUuid()
	s.RestoredSnapshotGeneration = restore.GetSnapshotGeneration()
}

type fileToken string

func (f fileToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	data, err := os.ReadFile(string(f))
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return nil, fmt.Errorf("Substrate token file is empty")
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}
func (fileToken) RequireTransportSecurity() bool { return true }
