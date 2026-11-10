package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"github.com/lilong555/agentcloud/internal/control"
	"github.com/lilong555/agentcloud/internal/observability"
)

func serve(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	autonomous := flags.Bool("enable-autonomous-scheduler", false, "启用服务端基于实际队列、Worker容量与历史延迟的自主休眠恢复")
	schedulerPolicy := flags.String("scheduler-policy", "observed", "生命周期策略：observed、cost-aware 或 agentcloud-v2")
	schedulerShadow := flags.Bool("scheduler-shadow", false, "仅执行 cost-aware 观测与计算；需要启用实验控制和自主调度")
	schedulerInterval := flags.Duration("scheduler-interval", 250*time.Millisecond, "自主调度观察间隔")
	schedulerIdlePrior := flags.Duration("scheduler-idle-prior", 0, "冷启动idle预计时长先验；实际idle观测后由p75历史替代")
	schedulerStable := flags.Duration("scheduler-stable-for", 500*time.Millisecond, "休眠前稳定等待确认时长")
	otlp := flags.Bool("otel-otlp", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "", "启用标准OTLP HTTP tracing exporter")
	serviceName := flags.String("otel-service-name", envDefault("OTEL_SERVICE_NAME", "agentcloud-coordinator"), "OpenTelemetry服务名称")
	listen := flags.String("listen", "127.0.0.1:8090", "HTTP listen address")
	data := flags.String("data-dir", "artifacts/local", "durable local state and workspaces")
	database := flags.String("database-url", os.Getenv("AGENTCLOUD_DATABASE_URL"), "PostgreSQL URL; empty uses fsync JSON")
	tokens := flags.String("tokens", os.Getenv("AGENTCLOUD_TOKENS"), "comma-separated token=tenant bindings")
	backend := flags.String("backend", "local", "local or substrate")
	modelURL := flags.String("model-url", os.Getenv("AGENTCLOUD_MODEL_URL"), "OpenAI-compatible model base URL or mock://deterministic")
	allowShell := flags.Bool("allow-local-shell", false, "enable controlled local development shell commands")
	modelConcurrency := flags.Int("model-concurrency", 4, "maximum parallel physical model attempts")
	modelTimeout := flags.Duration("model-timeout", 120*time.Second, "deadline for one physical model attempt")
	experimentControls := flags.Bool("enable-experiment-controls", false, "enable explicit session ablation groups A..E")
	dynamoURL := flags.String("dynamo-control-url", os.Getenv("AGENTCLOUD_DYNAMO_CONTROL_URL"), "native execution control origin")
	tenantTokensFile := flags.String("dynamo-tenant-tokens-file", "", "私有JSON文件 {tenant-id: native-control-token}")
	source := flags.String("source-id", "coordinator-local", "trusted execution source identity")
	substrateAddress := flags.String("substrate-address", "127.0.0.1:9443", "Substrate gRPC API address")
	atespace := flags.String("atespace", "agentcloud", "Substrate atespace")
	actorCoordinator := flags.String("actor-coordinator-url", "", "coordinator origin allowed by Actor egress policy")
	template := flags.String("actor-template", "agentcloud-tools", "ActorTemplate name")
	ca := flags.String("substrate-ca", "", "Substrate CA bundle")
	apiName := flags.String("substrate-server-name", "api.ate-system.svc", "gRPC TLS service name")
	apiToken := flags.String("substrate-token-file", "", "gRPC bearer token file")
	gateway := flags.String("substrate-gateway", "https://127.0.0.1:9444", "Actor HTTPS gateway")
	gatewayName := flags.String("substrate-gateway-name", "atenet-router.ate-system.svc", "gateway TLS service name")
	cert := flags.String("substrate-client-cert", "", "gateway client certificate")
	key := flags.String("substrate-client-key", "", "gateway client private key")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("serve accepts no positional arguments")
	}
	if *schedulerShadow && (!*experimentControls || !*autonomous) {
		return errors.New("scheduler shadow requires --enable-experiment-controls and --enable-autonomous-scheduler")
	}
	bindings := map[string]string{}
	for _, entry := range strings.Split(*tokens, ",") {
		token, tenant, ok := strings.Cut(entry, "=")
		if !ok || len(token) < 8 || tenant == "" {
			return errors.New("provide --tokens token=tenant (token at least 8 characters)")
		}
		bindings[token] = tenant
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		for token := range bindings {
			if len(token) < 32 {
				return errors.New("non-loopback listeners require bearer tokens of at least 32 characters")
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	telemetry, err := observability.New(ctx, observability.Config{ServiceName: *serviceName, OTLP: *otlp})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = telemetry.Close(shutdown)
	}()
	var execution control.Backend
	switch *backend {
	case "local":
		execution = &control.LocalBackend{Root: *data, AllowShell: *allowShell}
	case "substrate":
		client, e := substrateapi.New(ctx, substrateapi.Config{Address: *substrateAddress, Atespace: *atespace, CAFile: *ca, ServerName: *apiName, TokenFile: *apiToken})
		if e != nil {
			return e
		}
		defer client.Close()
		execution, e = control.NewSubstrateBackend(control.SubstrateConfig{Client: client, Atespace: *atespace, Template: *template, GatewayURL: *gateway, GatewayServerName: *gatewayName, CAFile: *ca, CertFile: *cert, KeyFile: *key, CoordinatorURL: *actorCoordinator, RuntimeToken: os.Getenv("AGENTCLOUD_RUNTIME_TOKEN")})
		if e != nil {
			return e
		}
	default:
		return errors.New("backend must be local or substrate")
	}
	var sink control.SignalSink
	if *dynamoURL != "" {
		credentials := map[string]string{}
		if *tenantTokensFile != "" {
			info, e := os.Stat(*tenantTokensFile)
			if e != nil {
				return e
			}
			if info.Mode().Perm()&0077 != 0 {
				return errors.New("Dynamo租户凭证文件权限需要0600")
			}
			data, e := os.ReadFile(*tenantTokensFile)
			if e != nil {
				return e
			}
			if e = json.Unmarshal(data, &credentials); e != nil {
				return errors.New("Dynamo租户凭证文件需要tenant=token JSON对象")
			}
			if len(credentials) == 0 {
				return errors.New("Dynamo租户凭证文件为空")
			}
		}
		bridge := &control.DynamoBridge{SourceID: *source}
		if len(credentials) == 0 {
			client, e := dynamo.NewClient(*dynamoURL, os.Getenv("AGENTCLOUD_DYNAMO_TOKEN"), nil)
			if e != nil {
				return e
			}
			check, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, e = client.Capabilities(check)
			cancel()
			if e != nil {
				return e
			}
			bridge.Client = client
		} else {
			bridge.Clients = map[string]*dynamo.Client{}
			for tenant, token := range credentials {
				client, e := dynamo.NewClient(*dynamoURL, token, nil)
				if e != nil {
					return errors.New("Dynamo租户凭证格式无效")
				}
				check, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, e = client.Capabilities(check)
				cancel()
				if e != nil {
					return fmt.Errorf("Dynamo tenant %s capability check failed", tenant)
				}
				bridge.Clients[tenant] = client
			}
		}
		sink = bridge
	}
	c, err := control.New(ctx, control.Config{EnableAutonomousScheduler: *autonomous, SchedulerInterval: *schedulerInterval, SchedulerStableFor: *schedulerStable, SchedulerIdlePrior: *schedulerIdlePrior, SchedulerPolicy: *schedulerPolicy, SchedulerShadow: *schedulerShadow, Observability: telemetry, Signals: sink, DataDir: *data, DatabaseURL: *database, Backend: execution, ModelURL: *modelURL, ModelAPIKey: os.Getenv("AGENTCLOUD_MODEL_API_KEY"), ModelConcurrency: *modelConcurrency, ModelTimeout: *modelTimeout, Tokens: bindings, EnableExperimentControls: *experimentControls})
	if err != nil {
		return err
	}
	defer c.Close()
	server := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Fprintf(stdout, "AgentCloud listening on %s; execution backend=%s\n", *listen, *backend)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
