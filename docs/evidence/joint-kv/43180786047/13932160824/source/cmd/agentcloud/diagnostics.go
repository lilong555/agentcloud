package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lilong555/agentcloud/internal/control"
)

func diagnostics(command string, args []string, stdout, stderr io.Writer) error {
	// Permit the documented workspace-first form and standard flag-first form.
	workspace := ""
	if command == "trace" && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		workspace = args[0]
		args = args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	origin := flags.String("url", envDefault("AGENTCLOUD_URL", "http://127.0.0.1:8090"), "协调器HTTP地址")
	token := flags.String("token", os.Getenv("AGENTCLOUD_TOKEN"), "租户Bearer凭证，推荐使用AGENTCLOUD_TOKEN")
	asJSON := flags.Bool("json", false, "输出完整JSON诊断结果")
	probe := flags.Bool("probe", true, "现场检查PostgreSQL、Substrate与Dynamo")
	timeout := flags.Duration("timeout", 10*time.Second, "诊断请求超时")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if command == "trace" && workspace == "" && flags.NArg() == 1 {
		workspace = flags.Arg(0)
	} else if flags.NArg() != 0 {
		return errors.New("诊断命令参数数量错误")
	}
	if command == "trace" && workspace == "" {
		return errors.New("用法：agentcloud trace <workspace-id> [--url URL] [--json]")
	}
	parsed, err := url.Parse(*origin)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("--url需要HTTP或HTTPS协调器地址")
	}
	if *token == "" {
		return errors.New("请设置AGENTCLOUD_TOKEN或--token")
	}
	path := "/v1/diagnostics/status?probe=" + fmt.Sprint(*probe)
	if command == "trace" {
		path = "/v1/workspaces/" + url.PathEscape(workspace) + "/trace"
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*origin, "/")+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+*token)
	request.Header.Set("Accept", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("协调器诊断请求失败")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 8<<20 {
		return errors.New("诊断响应超过8MiB")
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("诊断API返回HTTP %d", response.StatusCode)
	}
	if *asJSON {
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
	if command == "status" {
		var value control.DiagnosticStatus
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "状态：%s  执行后端：%s  日志后端：%s\n会话：%d  等待模型：%d  待对账模型：%d\n", value.Status, value.ExecutionBackend, value.JournalBackend, len(value.Sessions), value.PendingModelRequests, value.UnknownModelRequests)
		for _, component := range []string{"coordinator", "journal", "substrate", "dynamo"} {
			if status, ok := value.Checks[component]; ok {
				fmt.Fprintf(stdout, "%s：%s\n", component, status)
			}
		}
		for _, s := range value.Sessions {
			fmt.Fprintf(stdout, "Workspace %s  Session %s  %s  Gate=%s  Epoch=%d  Tools=%d\n", s.WorkspaceID, s.SessionID, s.State, s.Gate, s.Epoch, s.ActiveTools)
		}
		return nil
	}
	var value control.DiagnosticTrace
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Workspace %s  Session %s  %s  Gate=%s  Epoch=%d\n", value.Session.WorkspaceID, value.Session.SessionID, value.Session.State, value.Session.Gate, value.Session.Epoch)
	fmt.Fprintln(stdout, "状态迁移：")
	for _, event := range value.Events {
		fmt.Fprintf(stdout, "%s  #%d  %s  %s  Gate=%s  Epoch=%d\n", event.At.Format(time.RFC3339Nano), event.Sequence, event.Kind, event.State, event.Gate, event.Epoch)
	}
	fmt.Fprintln(stdout, "Epoch历史：")
	for _, epoch := range value.EpochHistory {
		fmt.Fprintf(stdout, "%d  %s → %s\n", epoch.Epoch, epoch.FirstObservedAt.Format(time.RFC3339Nano), epoch.LastObservedAt.Format(time.RFC3339Nano))
	}
	fmt.Fprintln(stdout, "Manifest摘要：")
	for _, manifest := range value.Manifests {
		fmt.Fprintf(stdout, "%s  %s  Epoch=%d  Snapshot=%s/%d  Fidelity=%s  Watermark=%d  Checkpoint=%s\n", manifest.ID, manifest.State, manifest.Epoch, manifest.SnapshotUUID, manifest.SnapshotGeneration, manifest.Fidelity, manifest.ReceiptWatermark, manifest.CheckpointID)
	}
	fmt.Fprintln(stdout, "关键路径观测（各阶段独立累计，model_http包含原生admission）：")
	for _, metric := range []string{"gateway_queue_ms", "dynamo_admission_wait_ms", "substrate_suspend_ms", "durability_wait_ms", "substrate_resume_ms", "journal_commit_ms", "model_http_ms"} {
		if measured, ok := value.LatencyMS[metric]; ok {
			fmt.Fprintf(stdout, "%s：%.3f ms\n", metric, measured)
		}
	}
	if len(value.UnavailableMetrics) > 0 {
		fmt.Fprintf(stdout, "待观测：%s\n", strings.Join(value.UnavailableMetrics, ", "))
	}
	for _, item := range value.Observations {
		if item.TraceID != "" {
			fmt.Fprintf(stdout, "Trace=%s  Span=%s  Operation=%s  %s=%.3f  Source=%s\n", item.TraceID, item.SpanID, item.OperationID, item.Metric, item.Value, item.Source)
		}
	}
	return nil
}
func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
