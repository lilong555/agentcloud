// Package observability exports bounded metrics and W3C trace context without request bodies.
package observability

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

type Measurement struct {
	At          time.Time `json:"at"`
	Metric      string    `json:"metric"`
	Value       float64   `json:"value"`
	Source      string    `json:"source"`
	TraceID     string    `json:"trace_id,omitempty"`
	SpanID      string    `json:"span_id,omitempty"`
	OperationID string    `json:"operation_id,omitempty"`
}

type Runtime struct {
	Registry        *prometheus.Registry
	Provider        *sdktrace.TracerProvider
	metrics         map[string]prometheus.Observer
	stranded        prometheus.Counter
	released        prometheus.Counter
	quiesceTimeouts prometheus.Counter
	unavailable     *prometheus.CounterVec
}

type Config struct {
	ServiceName string
	Exporter    sdktrace.SpanExporter
	// OTLP uses the standard OTEL_EXPORTER_OTLP_* environment variables.
	OTLP bool
}

func New(ctx context.Context, cfg Config) (*Runtime, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "agentcloud-coordinator"
	}
	exporter := cfg.Exporter
	if exporter == nil && cfg.OTLP {
		var err error
		exporter, err = otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("initialize OTLP tracing: %w", err)
		}
	}
	options := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName)))}
	if exporter != nil {
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	r := &Runtime{Registry: prometheus.NewRegistry(), Provider: sdktrace.NewTracerProvider(options...), metrics: map[string]prometheus.Observer{}}
	for name, help := range map[string]string{
		"gateway_queue_ms":         "Actual durable PENDING creation to dispatcher selection in milliseconds.",
		"dynamo_admission_wait_ms": "Native classifier admission wait correlated with a physical request, in milliseconds.",
		"substrate_suspend_ms":     "Actual SuspendActor gRPC call duration in milliseconds.",
		"durability_wait_ms":       "Actual WaitActorSnapshotDurable gRPC call duration in milliseconds.",
		"substrate_resume_ms":      "Actual ResumeActor gRPC call duration in milliseconds.",
		"model_http_ms":            "Actual inference HTTP transport and response consumption duration, in milliseconds.",
		"idle_window_ms":           "Actual explicit idle START to END interval in milliseconds; expiry is an observation validity limit.",
		"idle_until_tool_ms":       "Actual model terminal or prior tool completion to next tool admission interval in milliseconds.",
		"checkpoint_total_ms":      "Actual checkpoint safe point, snapshot, durability and retention completion duration, in milliseconds.",
		"resume_ready_ms":          "Actual restore and Actor attach readiness duration, in milliseconds.",
		"journal_commit_ms":        "Actual journal Save completion duration in milliseconds.",
	} {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000}})
		r.Registry.MustRegister(h)
		r.metrics[name] = h
	}
	r.stranded = prometheus.NewCounter(prometheus.CounterOpts{Name: "stranded_worker_gb_s", Help: "Observed assigned Worker memory in decimal GB times idle model/think seconds, excluding tool intervals."})
	r.released = prometheus.NewCounter(prometheus.CounterOpts{Name: "dynamo_released_acting_tokens", Help: "Native applied state updates' observed released acting token budget."})
	r.quiesceTimeouts = prometheus.NewCounter(prometheus.CounterOpts{Name: "quiesce_timeout_total", Help: "Tool drain timeouts before snapshot mutation."})
	r.unavailable = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "agentcloud_observation_unavailable_total", Help: "Missing or invalid native observations by bounded metric name."}, []string{"metric"})
	r.Registry.MustRegister(r.stranded, r.released, r.quiesceTimeouts, r.unavailable)
	return r, nil
}
func (r *Runtime) Close(ctx context.Context) error { return r.Provider.Shutdown(ctx) }
func (r *Runtime) Handler() http.Handler {
	return promhttp.HandlerFor(r.Registry, promhttp.HandlerOpts{})
}

type runtimeKey struct{}
type scopeKey struct{}
type Scope struct {
	OperationID string
	Record      func(Measurement)
}

func WithRuntime(ctx context.Context, r *Runtime) context.Context {
	return context.WithValue(ctx, runtimeKey{}, r)
}
func WithScope(ctx context.Context, s Scope) context.Context {
	return context.WithValue(ctx, scopeKey{}, s)
}
func ScopeFromContext(ctx context.Context) (Scope, bool) {
	scope, ok := ctx.Value(scopeKey{}).(Scope)
	return scope, ok
}
func FromContext(ctx context.Context) *Runtime { r, _ := ctx.Value(runtimeKey{}).(*Runtime); return r }
func Start(ctx context.Context, name string, kind trace.SpanKind) (context.Context, trace.Span) {
	t := otel.Tracer("github.com/lilong555/agentcloud")
	if r := FromContext(ctx); r != nil {
		t = r.Provider.Tracer("github.com/lilong555/agentcloud")
	}
	return t.Start(ctx, name, trace.WithSpanKind(kind))
}

// Finish reports only error class: upstream errors can contain credentials or body text.
func Finish(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "operation failed")
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	}
	span.End()
}
func Observe(ctx context.Context, name string, value float64, source string) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		Unavailable(ctx, name)
		return
	}
	r := FromContext(ctx)
	if r != nil {
		switch name {
		case "stranded_worker_gb_s":
			r.stranded.Add(value)
		case "dynamo_released_acting_tokens":
			r.released.Add(value)
		case "quiesce_timeout_total":
			r.quiesceTimeouts.Add(value)
		default:
			if h := r.metrics[name]; h != nil {
				h.Observe(value)
			}
		}
	}
	sc := trace.SpanContextFromContext(ctx)
	item := Measurement{At: time.Now().UTC(), Metric: name, Value: value, Source: source}
	if sc.IsValid() {
		item.TraceID = sc.TraceID().String()
		item.SpanID = sc.SpanID().String()
	}
	if scope, ok := ctx.Value(scopeKey{}).(Scope); ok {
		item.OperationID = scope.OperationID
		if scope.Record != nil {
			scope.Record(item)
		}
	}
	trace.SpanFromContext(ctx).AddEvent("observation", trace.WithAttributes(attribute.String("metric", name), attribute.Float64("value", value), attribute.String("source", source)))
}
func Unavailable(ctx context.Context, name string) {
	if r := FromContext(ctx); r != nil {
		if _, ok := r.metrics[name]; ok || name == "stranded_worker_gb_s" || name == "dynamo_released_acting_tokens" {
			r.unavailable.WithLabelValues(name).Inc()
		}
	}
}

var Propagator = propagation.TraceContext{}

func Extract(ctx context.Context, h http.Header) context.Context {
	return Propagator.Extract(ctx, propagation.HeaderCarrier(h))
}
func Inject(ctx context.Context, h http.Header) { Propagator.Inject(ctx, propagation.HeaderCarrier(h)) }

type metadataCarrier metadata.MD

func (m metadataCarrier) Get(k string) string {
	values := metadata.MD(m).Get(k)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}
func (m metadataCarrier) Set(k, v string) { metadata.MD(m).Set(k, v) }
func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
func UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, span := Start(ctx, method, trace.SpanKindClient)
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	Propagator.Inject(ctx, metadataCarrier(md))
	ctx = metadata.NewOutgoingContext(ctx, md)
	start := time.Now()
	err := invoke(ctx, method, req, reply, cc, opts...)
	span.SetAttributes(attribute.String("rpc.system", "grpc"), attribute.String("rpc.grpc.status_code", grpcstatus.Code(err).String()))
	name := ""
	switch {
	case strings.HasSuffix(method, "/SuspendActor"):
		name = "substrate_suspend_ms"
	case strings.HasSuffix(method, "/WaitActorSnapshotDurable"):
		name = "durability_wait_ms"
	case strings.HasSuffix(method, "/ResumeActor"):
		name = "substrate_resume_ms"
	}
	if name != "" {
		Observe(ctx, name, float64(time.Since(start))/float64(time.Millisecond), "substrate_grpc_client")
	}
	Finish(span, err)
	return err
}
