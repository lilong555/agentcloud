package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"github.com/lilong555/agentcloud/internal/journal"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A bounded gRPC backend exercises the production Substrate client and Ensure
// path. Slot counts exist only in this fixture, outside the Coordinator.
type initialControlServer struct {
	ateapi.UnimplementedControlServer
	mu                               sync.Mutex
	actors                           map[string]*ateapi.Actor
	creates                          map[string]int
	capacity, active, peak, attempts int
	rejectCode                       codes.Code
	mutateOnReject                   bool
	createReject                     bool
	beforeResume                     func()
}

func (s *initialControlServer) GetActor(_ context.Context, req *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.actors[req.Actor.Name]
	if a == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.Clone(a).(*ateapi.Actor), nil
}

func (s *initialControlServer) CreateActor(_ context.Context, req *ateapi.CreateActorRequest) (*ateapi.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createReject {
		return nil, status.Error(codes.ResourceExhausted, "create capacity")
	}
	name := req.Actor.Metadata.Name
	s.creates[name]++
	if s.actors[name] != nil {
		return nil, status.Error(codes.AlreadyExists, "duplicate actor")
	}
	a := &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: name, Atespace: "team-a", Uid: newID(), Version: 1},
		Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}}
	s.actors[name] = a
	return proto.Clone(a).(*ateapi.Actor), nil
}

func (s *initialControlServer) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.beforeResume != nil {
		s.beforeResume()
	}
	a := s.actors[req.Actor.Name]
	if a == nil || req.ExpectedUid != a.Metadata.Uid || req.ExpectedVersion != a.Metadata.Version {
		return nil, status.Error(codes.FailedPrecondition, "identity fence")
	}
	if s.active >= s.capacity || s.rejectCode != codes.OK {
		code := s.rejectCode
		if code == codes.OK {
			code = codes.ResourceExhausted
		}
		if s.mutateOnReject {
			a.Metadata.Version++
		}
		return nil, status.Error(code, "no Worker capacity")
	}
	s.active++
	s.peak = max(s.peak, s.active)
	a.Status.State, a.Metadata.Version = ateapi.ActorState_ACTOR_STATE_RUNNING, a.Metadata.Version+1
	a.Status.LastRestore = &ateapi.ActorRestoreResult{Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ColdStarted: true}
	return &ateapi.ResumeActorResponse{Actor: proto.Clone(a).(*ateapi.Actor), Resumed: true, Restore: a.Status.LastRestore}, nil
}

func admissionFixture(t *testing.T) (*SubstrateBackend, *initialControlServer) {
	t.Helper()
	s := &initialControlServer{actors: map[string]*ateapi.Actor{}, creates: map[string]int{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	ateapi.RegisterControlServer(g, s)
	go g.Serve(listener)
	t.Cleanup(g.Stop)
	client, err := substrateapi.New(context.Background(), substrateapi.Config{Address: listener.Addr().String(), Insecure: true, Atespace: "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			respond(w, 200, map[string]any{"ready": true})
			return
		}
		if r.URL.Path == "/runtime/v1/attach" {
			var input runtimeAttachReceipt
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			input.Gate, input.IdentityAttested = "OPEN", true
			respond(w, 200, input)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(runtime.Close)
	return &SubstrateBackend{cfg: SubstrateConfig{Client: client, GatewayURL: runtime.URL, Atespace: "team-a", Template: "fixture"}, http: runtime.Client()}, s
}

type initialFixtureBackend struct {
	*SubstrateBackend
	local  *LocalBackend
	server *initialControlServer
}

func (b *initialFixtureBackend) Checkpoint(ctx context.Context, s *Session, op string) (Actor, string, error) {
	if err := os.MkdirAll(b.local.workspace(s), 0700); err != nil {
		return Actor{}, "", err
	}
	a, d, err := b.local.Checkpoint(ctx, s, op)
	if err == nil {
		b.server.mu.Lock()
		b.server.active--
		b.server.actors[s.Actor.Name].Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
		b.server.mu.Unlock()
	}
	return a, d, err
}
func (b *initialFixtureBackend) Suspend(ctx context.Context, s *Session) (Actor, error) {
	return b.local.Suspend(ctx, s)
}

func TestNativeInitialAdmissionPostgresConcurrencyRestartAndRelease(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL, cfg.ResearchRunID = isolatedControllerDatabase(t), "native-admission-capacity"
	cfg.SchedulerInterval, cfg.ModelTimeout = time.Hour, 10*time.Second
	backend, upstream := admissionFixture(t)
	cfg.Backend = &initialFixtureBackend{SubstrateBackend: backend, local: cfg.Backend.(*LocalBackend), server: upstream}
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.Close() }()
	const n = 12
	var group sync.WaitGroup
	errorsCh := make(chan error, n)
	for i := 0; i < n; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			id := fmt.Sprintf("session-%02d", i)
			s, e := c.Create(context.Background(), "tenant-a", CreateSession{SessionID: id, WorkspaceID: id, ActorName: id})
			if e != nil {
				errorsCh <- e
				return
			}
			if s.State != "WAITING_CAPACITY" || s.Gate != "CLOSED" || s.Actor.UID == "" || s.Admissions["initial"].AttemptCount != 1 {
				errorsCh <- fmt.Errorf("invalid waiting state: %+v", s)
			}
		}(i)
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	before, _ := c.Get("tenant-a", "session-00")
	// A duplicate HTTP-level Create reads the durable intent without creating another Actor.
	duplicate := createTestSession(t, c, "tenant-a", "session-00")
	if duplicate.Actor.UID != before.Actor.UID || duplicate.Admissions["initial"].AttemptCount != 1 {
		t.Fatal("duplicate create changed admission")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	upstream.mu.Lock()
	upstream.capacity = 4
	upstream.mu.Unlock()
	cfg.SchedulerInterval = 20 * time.Millisecond
	c, err = New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := c.Get("tenant-a", "session-00")
	if after.Actor.UID != before.Actor.UID || !after.Admissions["initial"].RequestedAt.Equal(before.Admissions["initial"].RequestedAt) {
		t.Fatal("restart lost original identity or arrival")
	}
	deadline := time.Now().Add(12 * time.Second)
	released := map[string]bool{}
	for len(released) < n && time.Now().Before(deadline) {
		for _, s := range c.List("tenant-a") {
			if s.State == "RECOVERY_REQUIRED" {
				t.Fatalf("unresolved admission: %+v", s)
			}
			if s.State != "RUNNING" || released[s.SessionID] {
				continue
			}
			d := s.Admissions["initial"]
			if d.Status != "READY" || d.AttemptCount < 2 || d.CapacityWaitMS <= 0 || d.ReadyAt == nil || s.Epoch != 1 {
				t.Fatalf("bad terminal demand: %+v", d)
			}
			if _, _, e := c.Lifecycle(context.Background(), "tenant-a", s.SessionID, "suspend", LifecycleRequest{OperationID: "release", ExpectedEpoch: 1}); e != nil {
				t.Fatal(e)
			}
			released[s.SessionID] = true
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(released) != n {
		t.Fatalf("only %d/%d admitted", len(released), n)
	}
	upstream.mu.Lock()
	for name, count := range upstream.creates {
		if count != 1 {
			t.Errorf("%s created %d times", name, count)
		}
	}
	if upstream.peak > 4 || upstream.active != 0 {
		t.Errorf("capacity conservation: active=%d peak=%d", upstream.active, upstream.peak)
	}
	upstream.mu.Unlock()
	db := postgresInspector(t, cfg.DatabaseURL)
	var count, lo, hi, instances, ready, releases int
	err = db.QueryRow(context.Background(), `SELECT count(*),min(sequence),max(sequence),
count(distinct payload->'evidence_clock'->>'source_instance_id'),
count(*) FILTER (WHERE payload->>'kind'='admission.actor_ready'),
count(*) FILTER (WHERE payload->>'kind'='admission.resource_released')
FROM agentcloud.research_records WHERE stream='events'`).Scan(&count, &lo, &hi, &instances, &ready, &releases)
	if err != nil || count != hi-lo+1 || instances != 2 || ready != n || releases != n {
		t.Fatalf("capture continuity count=%d range=%d..%d instances=%d ready=%d release=%d err=%v", count, lo, hi, instances, ready, releases, err)
	}
	var missing int
	err = db.QueryRow(context.Background(), `SELECT count(*) FROM agentcloud.journal_entities e
WHERE e.kind='sessions' AND NOT EXISTS (SELECT 1 FROM agentcloud.research_records r
WHERE r.payload->>'session_id'=e.data->>'session_id' AND r.payload->>'kind'='admission.demand_persisted')`).Scan(&missing)
	if err != nil || missing != 0 {
		t.Fatalf("demand event transaction missing: %d %v", missing, err)
	}
	if output := os.Getenv("AGENTCLOUD_ADMISSION_TEST_EXPORT"); output != "" {
		if err = os.MkdirAll(filepath.Dir(output), 0700); err != nil {
			t.Fatal(err)
		}
		file, e := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		if e = journal.ExportResearch(context.Background(), cfg.DatabaseURL, cfg.ResearchRunID, file); e != nil {
			t.Fatal(e)
		}
		if e = file.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestInitialAdmissionProofAndRetryFences(t *testing.T) {
	for _, test := range []struct {
		name                 string
		code                 codes.Code
		mutate, createReject bool
		verified             bool
	}{
		{name: "unchanged", verified: true},
		{name: "create-rejected-before-mutation", createReject: true, verified: true},
		{name: "changed-version", mutate: true},
		{name: "lost-response", code: codes.Unavailable},
		{name: "timeout", code: codes.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, server := admissionFixture(t)
			server.rejectCode, server.mutateOnReject, server.createReject = test.code, test.mutate, test.createReject
			s := &Session{TenantID: "tenant-a", WorkspaceID: "w", SessionID: "s", Actor: Actor{Name: "s"}}
			_, err := backend.Ensure(context.Background(), s)
			var waiting *initialCapacityWaitError
			if errors.As(err, &waiting) != test.verified {
				t.Fatalf("verified=%t err=%v", test.verified, err)
			}
			if !test.verified {
				return
			}
			if !waiting.matches(s) {
				t.Fatal("proof failed")
			}
			if test.createReject {
				return
			}
			s.Actor = waiting.actor
			s.Admissions = map[string]*AdmissionDemand{"initial": {InitialActor: waiting.proof}}
			_, err = backend.Ensure(context.Background(), s)
			if !errors.As(err, &waiting) {
				t.Fatalf("owned initial Actor not retried: %v", err)
			}
			server.mu.Lock()
			server.actors["s"].Metadata.Version++
			attempts := server.attempts
			server.mu.Unlock()
			_, err = backend.Ensure(context.Background(), s)
			if !errors.Is(err, errInitialActorChanged) {
				t.Fatalf("changed Actor admitted: %v", err)
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			if server.attempts != attempts || server.creates["s"] != 1 {
				t.Fatal("retry crossed ownership fence")
			}
		})
	}
	t.Run("unowned-existing-name", func(t *testing.T) {
		backend, server := admissionFixture(t)
		_, err := server.CreateActor(context.Background(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: "s"}}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = backend.Ensure(context.Background(), &Session{Actor: Actor{Name: "s"}})
		if err == nil || server.attempts != 0 {
			t.Fatal("unowned actor adopted")
		}
	})
}

func TestStaticPolicyReturnDemandResumesWithoutIdleScheduling(t *testing.T) {
	cfg, backend := capacityConfig(t)
	cfg.SchedulerInterval = 10 * time.Millisecond
	c := newTestController(t, cfg)
	suspendCapacitySession(t, c, "suspend")
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", "capacity", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "capacity")
		for _, d := range s.Admissions {
			if d.Kind == "resume_admission" && d.Status == "WAITING_CAPACITY" {
				return true
			}
		}
		return false
	})
	backend.allowed.Store(true)
	deadline := time.Now().Add(capacityProbeInterval + time.Second)
	for {
		s, _ := c.Get("tenant-a", "capacity")
		if s.State == "RUNNING" && s.Epoch == 2 && s.ReturnDemands["return"].Status == "COMPLETED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("external capacity change was not picked up by bounded fallback")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, _ := c.Get("tenant-a", "capacity")
	if len(s.Scheduling) != 0 {
		t.Fatal("static policy invoked idle scheduling")
	}
}

func TestInitialAdmissionFailedCommitKeepsOwnedActorAndUnknownIntent(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL, cfg.ResearchRunID = isolatedControllerDatabase(t), "initial-failed-commit"
	cfg.SchedulerInterval = time.Hour
	backend, server := admissionFixture(t)
	cfg.Backend = backend
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.Close() }()
	db := postgresInspector(t, cfg.DatabaseURL)
	server.beforeResume = func() {
		if _, err := db.Exec(context.Background(), "UPDATE agentcloud.journal SET revision=revision+1 WHERE id=1"); err != nil {
			t.Error(err)
		}
	}
	if _, err = c.Create(context.Background(), "tenant-a", CreateSession{SessionID: "lost", WorkspaceID: "lost", ActorName: "lost"}); err == nil {
		t.Fatal("uncommitted capacity outcome exposed")
	}
	if _, err = c.Get("tenant-a", "lost"); err == nil {
		t.Fatal("failed journal still serving")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.beforeResume = nil
	attempts := server.attempts
	server.mu.Unlock()
	c, err = New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.Get("tenant-a", "lost")
	if err != nil || s.State != "RECOVERY_REQUIRED" || s.Actor.UID == "" || s.Admissions["initial"].Status != "OUTCOME_UNKNOWN" {
		t.Fatalf("restart replayed or lost uncertain intent: %+v %v", s, err)
	}
	c.autonomousStep(schedulingSnapshot(s))
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.attempts != attempts {
		t.Fatal("uncertain attempt replayed")
	}
}

func TestInitialAdmissionHTTPAcceptedAndArrivalClock(t *testing.T) {
	cfg := testConfig(t)
	cfg.Signals = &experimentSignal{}
	backend, _ := admissionFixture(t)
	cfg.Backend, cfg.SchedulerInterval = backend, time.Hour
	c := newTestController(t, cfg)
	body := `{"session_id":"http","workspace_id":"http","actor_name":"http"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer token-a")
	r.Header.Set("X-AgentCloud-Client-Sent-At-Ns", "1795158000000000001")
	w := httptest.NewRecorder()
	c.Handler().ServeHTTP(w, r)
	var s Session
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusAccepted || w.Header().Get("Retry-After") != "1" || s.State != "WAITING_CAPACITY" {
		t.Fatalf("response: %d %s", w.Code, w.Body.String())
	}
	d := s.Admissions["initial"]
	if d.ClientSentAtNS != "1795158000000000001" || d.ReceivedAt.IsZero() || !d.RequestedAt.Equal(d.ReceivedAt) {
		t.Fatalf("arrival evidence: %+v", d)
	}
	c.signal(&s, "")
	c.lock()
	defer c.mu.Unlock()
	last := c.state.Events[len(c.state.Events)-1]
	if last.Kind != "signal.applied" || last.Detail != "PARKED" {
		t.Fatalf("initial waiting execution feedback: %+v", last)
	}
}

type admissionReadySignal struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *admissionReadySignal) Send(ctx context.Context, session *Session, state string) error {
	if session.Epoch == 2 && state == "RUNNING_FAST" {
		s.once.Do(func() {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		})
	}
	return nil
}

func TestAdmissionReadyFeedbackPreservesNextEpochReturn(t *testing.T) {
	cfg, backend := capacityConfig(t)
	cfg.SchedulerInterval = time.Hour
	sink := &admissionReadySignal{entered: make(chan struct{}), release: make(chan struct{})}
	cfg.Signals = sink
	c := newTestController(t, cfg)
	defer close(sink.release)
	backend.allowed.Store(true)
	suspendCapacitySession(t, c, "suspend")
	// Hold the scheduler claim while the controlled feedback interleaving runs.
	c.lock()
	c.schedulerBusy[key("tenant-a", "capacity")] = true
	c.mu.Unlock()
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", "capacity", LifecycleRequest{OperationID: "return-1", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := c.Get("tenant-a", "capacity")
	done := make(chan struct{})
	go func() { c.advanceAdmission(context.Background(), snapshot); close(done) }()
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ready feedback was not reached")
	}
	suspended := make(chan error, 1)
	go func() {
		_, _, err := c.Lifecycle(context.Background(), "tenant-a", "capacity", "suspend", LifecycleRequest{OperationID: "sleep-2", ExpectedEpoch: 2})
		suspended <- err
	}()
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "capacity")
		return s.State == "SUSPENDED" && s.Epoch == 2
	})
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", "capacity", LifecycleRequest{OperationID: "return-2", ExpectedEpoch: 2}); err != nil {
		t.Fatal(err)
	}
	sink.release <- struct{}{}
	<-done
	if err := <-suspended; err != nil {
		t.Fatal(err)
	}
	s, _ := c.Get("tenant-a", "capacity")
	if s.ReturnDemands["return-1"].Status != "COMPLETED" || s.ReturnDemands["return-2"].Status != "PENDING" || !s.Autonomous.ReturnDemand {
		t.Fatalf("ready feedback changed the next epoch demand: %+v", s.ReturnDemands)
	}
	c.lock()
	delete(c.schedulerBusy, key("tenant-a", "capacity"))
	c.wakeAdmission()
	c.mu.Unlock()
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "capacity")
		return s.Epoch == 3 && s.ReturnDemands["return-2"].Status == "COMPLETED"
	})
}
