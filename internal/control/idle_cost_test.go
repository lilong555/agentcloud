package control

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

func costFixture(now time.Time) (*Controller, *Session, policy.LifecycleConfig) {
	s := &Session{TenantID: "tenant", SessionID: "subject", Profile: "research",
		State: "RUNNING", Gate: "OPEN", Epoch: 1, IdleSince: now,
		RunningSince: now.Add(-time.Minute)}
	c := &Controller{cfg: Config{SchedulerPolicy: "cost-aware", ModelConcurrency: 4},
		state: State{Sessions: map[string]*Session{key(s.TenantID, s.SessionID): s}, Requests: map[string]*ModelRequest{}}}
	return c, s, policy.LifecycleConfig{SnapshotCostMS: 100, ResumeCostMS: 100, CooldownMS: 5000}
}

func predictionEvidence(started time.Time, duration int64) *ServerEstimateEvidence {
	e := &ServerEstimateEvidence{WaitPhase: "explicit_idle", WaitStartedAt: started}
	if duration >= 0 {
		for i := 0; i < 8; i++ {
			e.waitSamples = append(e.waitSamples, policy.SurvivalSample{DurationMS: duration, Completed: true})
		}
	}
	p := policy.ConditionalRemaining(e.waitSamples, 0, .75, waitMinimumSupport)
	e.WaitPrediction = &p
	return e
}

func TestCostReleasePlanUsesConditionalCostAndBoundedFallback(t *testing.T) {
	now := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name            string
		duration, delay int64
	}{{"unknown", -1, 200}, {"short_return", 20, 400}, {"long_wait", 10000, 100}} {
		t.Run(tc.name, func(t *testing.T) {
			c, s, cfg := costFixture(now)
			e := predictionEvidence(now, tc.duration)
			input := ModelBoundaryRequest{ExpectedEpoch: 1}
			d, err := c.costDecisionLocked(s, input, e, cfg, now)
			if err != nil || d.Action != "KEEP" {
				t.Fatalf("initial decision %+v %v", d, err)
			}
			plan := s.Autonomous.ReleasePlan
			if got := plan.PlannedReleaseAt.Sub(now).Milliseconds(); got != tc.delay {
				t.Fatalf("release=%d expected=%d, plan=%+v", got, tc.delay, plan)
			}
			if plan.LatestReleaseAt.Sub(now) != 400*time.Millisecond {
				t.Fatalf("budget horizon %+v", plan)
			}
			e.WaitAgeMS = tc.delay
			e.WaitPrediction = nil
			d, err = c.costDecisionLocked(s, input, e, cfg, now.Add(time.Duration(tc.delay)*time.Millisecond))
			if err != nil || d.Action != "SUSPEND" || d.Reason != "observed_cost_deadline" || d.Estimates.ExpectedWaitMS != 0 {
				t.Fatalf("deadline decision must retain actual estimate: %+v %v", d, err)
			}
		})
	}
}

func TestCostReleasePlanNeverRenewsAcrossForecastOrPhaseChanges(t *testing.T) {
	now := time.Now().UTC()
	c, s, cfg := costFixture(now)
	e := predictionEvidence(now, 20)
	input := ModelBoundaryRequest{ExpectedEpoch: 1}
	if _, err := c.costDecisionLocked(s, input, e, cfg, now); err != nil {
		t.Fatal(err)
	}
	latest := s.Autonomous.ReleasePlan.LatestReleaseAt
	e = predictionEvidence(now.Add(20*time.Millisecond), 10000)
	if _, err := c.costDecisionLocked(s, input, e, cfg, now.Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	earlier := s.Autonomous.ReleasePlan.PlannedReleaseAt
	if earlier != now.Add(100*time.Millisecond) {
		t.Fatalf("long wait should advance release: %+v", s.Autonomous.ReleasePlan)
	}
	for i := 1; i < 5; i++ {
		e = predictionEvidence(now.Add(time.Duration(i)*time.Millisecond), 1)
		e.WaitPhase = "model"
		cfg.SnapshotCostMS += 500
		if _, err := c.costDecisionLocked(s, input, e, cfg, now.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		plan := s.Autonomous.ReleasePlan
		if plan.PlannedReleaseAt != earlier || plan.LatestReleaseAt != latest || plan.StartedAt != now || plan.SwitchCostMS != 200 {
			t.Fatalf("episode renewed by forecast/phase/cost update: %+v", plan)
		}
	}
	s.Epoch++
	s.LastResumedAt = now.Add(time.Second)
	if _, err := c.costDecisionLocked(s, input, predictionEvidence(now, -1), cfg, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if s.Autonomous.ReleasePlan.Epoch != 2 || s.Autonomous.ReleasePlan.StartedAt != s.LastResumedAt {
		t.Fatalf("new activation retained old budget: %+v", s.Autonomous.ReleasePlan)
	}
}

func TestCostReleaseRespectsSafetyAndWaitingReturnCapacity(t *testing.T) {
	now := time.Now().UTC()
	c, s, cfg := costFixture(now)
	for i := 0; i < 2; i++ {
		peer := &Session{TenantID: "peer", SessionID: fmt.Sprint(i), State: "SUSPENDED", Autonomous: &AutonomousState{ReturnDemand: true}}
		c.state.Sessions[key(peer.TenantID, peer.SessionID)] = peer
	}
	e := predictionEvidence(now, -1)
	e.WorkerCapacity = &CapacityObservation{CapacityActors: 1}
	d, err := c.costDecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now)
	if err != nil || d.Action != "KEEP" || s.Autonomous.ReleasePlan.ReadyReturns != 2 || s.Autonomous.ReleasePlan.BreakEvenWaitMS != 67 {
		t.Fatalf("capacity opportunity cost %+v %+v %v", d, s.Autonomous, err)
	}
	s.ActiveTools = 1
	d, err = c.costDecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(time.Second))
	if err != nil || d.Action != "KEEP" || d.Reason != "unsafe_point" {
		t.Fatalf("active tool released: %+v %v", d, err)
	}
	s.ActiveTools = 0
	s.Tools = map[string]*ToolReceipt{"uncertain": {Status: "OUTCOME_UNKNOWN"}}
	d, _ = c.costDecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(time.Second))
	if d.Action != "KEEP" || d.Reason != "unsafe_point" {
		t.Fatalf("unknown side effect released: %+v", d)
	}
	s.Tools = nil
	s.Autonomous.ReturnDemand = true
	d, _ = c.costDecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(time.Second))
	if d.Action != "KEEP" || d.Reason != "return_demand" {
		t.Fatalf("return demand lost: %+v", d)
	}
}

func TestWaitHistoryContainsLiveCensoringAndNoFutureLabels(t *testing.T) {
	now := time.Now().UTC()
	c, s, _ := costFixture(now)
	peer := &Session{TenantID: s.TenantID, SessionID: "peer", Profile: s.Profile}
	c.state.Sessions[key(peer.TenantID, peer.SessionID)] = peer
	current := &ModelRequest{ID: "current", TenantID: s.TenantID, SessionID: s.SessionID, CreatedAt: now.Add(-2 * time.Second), Status: "DISPATCHING"}
	c.state.Requests["current"] = current
	for i := 0; i < 20; i++ {
		duration := time.Second
		if i >= 16 {
			duration = 20 * time.Second
		}
		start := now.Add(-30 * time.Second).Add(time.Duration(i) * time.Millisecond)
		r := &ModelRequest{ID: fmt.Sprint(i), TenantID: peer.TenantID, SessionID: peer.SessionID, CreatedAt: start, UpdatedAt: start.Add(duration), Status: "COMPLETED"}
		c.state.Requests[r.ID] = r
	}
	phase, start, samples := c.waitHistoryLocked(s, now)
	p := policy.ConditionalRemaining(samples, now.Sub(start).Milliseconds(), .75, waitMinimumSupport)
	if phase != "model" || !p.Known || p.RemainingMS != 18000 || p.Samples != 20 {
		t.Fatalf("mixed wait history: phase=%s prediction=%+v", phase, p)
	}
	c.state.Requests["future_completion"] = &ModelRequest{ID: "future", TenantID: peer.TenantID, SessionID: peer.SessionID, CreatedAt: now.Add(-3 * time.Second), UpdatedAt: now.Add(100 * time.Second), Status: "COMPLETED"}
	c.state.Requests["unknown"] = &ModelRequest{ID: "unknown", TenantID: peer.TenantID, SessionID: peer.SessionID, CreatedAt: now.Add(-4 * time.Second), UpdatedAt: now.Add(-time.Second), Status: "OUTCOME_UNKNOWN"}
	c.state.Requests["future_start"] = &ModelRequest{ID: "unseen", TenantID: peer.TenantID, SessionID: peer.SessionID, CreatedAt: now.Add(time.Second), Status: "PENDING"}
	c.state.Requests["other_tenant"] = &ModelRequest{ID: "other", TenantID: "other", SessionID: peer.SessionID, CreatedAt: now.Add(-time.Second), UpdatedAt: now, Status: "COMPLETED"}
	_, _, samples = c.waitHistoryLocked(s, now)
	censored := 0
	for _, sample := range samples {
		if !sample.Completed {
			censored++
			if sample.DurationMS != 3000 {
				t.Fatalf("future label/unknown exposure leaked: %+v", sample)
			}
		}
	}
	if len(samples) != 22 || censored != 2 {
		t.Fatalf("cohort selection: samples=%d censored=%d", len(samples), censored)
	}
}

func TestWaitCohortBoundedByStartAndExpiredWindowStopsPrediction(t *testing.T) {
	now := time.Now().UTC()
	h := waitCohort{}
	for i := 100; i >= 0; i-- {
		start := now.Add(-time.Duration(i) * time.Second)
		h.add(fmt.Sprint(i), start, now, now, i%2 == 0)
	}
	if len(h.rows) != waitHistoryLimit || h.rows[0].id != "0" || h.rows[63].id != "63" {
		t.Fatalf("started-time capacity: %+v", h.rows)
	}
	duplicates := waitCohort{}
	for i := 0; i < 8; i++ {
		duplicates.add("same", now.Add(-time.Second), now, now, true)
	}
	if len(duplicates.rows) != 1 {
		t.Fatalf("one wait counted repeatedly: %d", len(duplicates.rows))
	}
	c, s, _ := costFixture(now)
	s.IdleSince = now.Add(-time.Second)
	s.IdleWindows = map[string]*IdleWindow{"expired": {Epoch: 1, StartedAt: s.IdleSince, ExpiresAt: now.Add(-time.Millisecond)}}
	phase, start, samples := c.waitHistoryLocked(s, now)
	if phase != "expired_idle" || !start.IsZero() || len(samples) != 0 {
		t.Fatalf("expired observation reused: %s %v %+v", phase, start, samples)
	}
}

func TestCostPlanPersistsAndToolAdmissionStartsNewEpisode(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "cost-aware"
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, c, "tenant-a", "persistent")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "persistent")]
	now := time.Now().UTC()
	_, err = c.costDecisionLocked(s, ModelBoundaryRequest{}, predictionEvidence(now, -1), policy.DefaultLifecycleConfig(), now)
	plan := copyValue(s.Autonomous.ReleasePlan)
	if err == nil {
		err = c.persist()
	}
	c.mu.Unlock()
	if err != nil {
		_ = c.Close()
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newTestController(t, cfg)
	state, err := reopened.Get("tenant-a", "persistent")
	if err != nil || !reflect.DeepEqual(state.Autonomous.ReleasePlan, plan) {
		t.Fatalf("release deadline lost on restart: %+v %v", state, err)
	}
	_, err = reopened.Tool(context.Background(), "tenant-a", "persistent", ToolRequest{ID: "activity", Epoch: 1, Kind: "file_write", Path: "proof.txt", Content: "durable"})
	if err != nil {
		t.Fatal(err)
	}
	state, _ = reopened.Get("tenant-a", "persistent")
	if state.Autonomous.ReleasePlan != nil {
		t.Fatalf("tool admission retained old plan: %+v", state.Autonomous)
	}
}

type costQueueFunc func(context.Context, *Session) (QueueObservation, error)

func (f costQueueFunc) Waiting(ctx context.Context, s *Session) (QueueObservation, error) {
	return f(ctx, s)
}

func TestCostBoundaryExecutesAndReplaysOriginalDecision(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueObserver = costQueueFunc(func(context.Context, *Session) (QueueObservation, error) {
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, nil
	})
	cfg.SchedulerPolicy = "cost-aware"
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5}
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "boundary")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "boundary")]
	s.IdleSince, s.RunningSince = time.Now().Add(-time.Second), time.Now().Add(-time.Second)
	c.mu.Unlock()
	input := ModelBoundaryRequest{OperationID: "cost-release", ExpectedEpoch: 1}
	first, err := c.ModelBoundary(context.Background(), "tenant-a", "boundary", input)
	if err != nil || first.Decision.Action != "SUSPEND" || first.Status != "COMPLETED" || first.ServerEvidence.ReleasePlan == nil {
		t.Fatalf("cost boundary: %+v %v", first, err)
	}
	second, err := c.ModelBoundary(context.Background(), "tenant-a", "boundary", input)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("decision replay changed: %+v %v", second, err)
	}
	state, _ := c.Get("tenant-a", "boundary")
	if state.State != "SUSPENDED" || state.Gate != "CLOSED" {
		t.Fatalf("lifecycle result %+v", state)
	}
}

func TestSchedulingSnapshotDetachesMetadataAndOmitsHistory(t *testing.T) {
	s := &Session{TenantID: "t", SessionID: "s", Actor: Actor{UID: "actor"}, Manifest: &Manifest{ID: "manifest"},
		Autonomous: &AutonomousState{Epoch: 2}, Tools: map[string]*ToolReceipt{"tool": {Status: "COMPLETED"}},
		Lifecycle: map[string]*LifecycleReceipt{"op": {}}, Scheduling: map[string]*BoundaryDecision{"decision": {}}}
	snapshot := schedulingSnapshot(s)
	if snapshot.Tools != nil || snapshot.Lifecycle != nil || snapshot.Scheduling != nil || snapshot.Actor.UID != "actor" {
		t.Fatalf("observer snapshot %+v", snapshot)
	}
	snapshot.Manifest.ID = "changed"
	snapshot.Autonomous.Epoch = 8
	if s.Manifest.ID != "manifest" || s.Autonomous.Epoch != 2 {
		t.Fatal("observer mutated coordinator state")
	}
}

func TestCostBoundaryRechecksReturnDuringQueueObservation(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "cost-aware"
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5}
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "race")
	c.lock()
	c.cfg.EnableAutonomousScheduler = true
	s := c.state.Sessions[key("tenant-a", "race")]
	s.IdleSince, s.RunningSince = time.Now().Add(-time.Second), time.Now().Add(-time.Second)
	s.Autonomous = &AutonomousState{Epoch: 1, CandidateSince: time.Now().Add(-time.Minute)}
	c.cfg.QueueObserver = costQueueFunc(func(ctx context.Context, _ *Session) (QueueObservation, error) {
		receipt, err := c.ReturnDemand(ctx, "tenant-a", "race", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1})
		if err == nil && receipt.Status != "COMPLETED" {
			t.Errorf("running return demand: %+v", receipt)
		}
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, err
	})
	c.mu.Unlock()
	receipt, err := c.ModelBoundary(context.Background(), "tenant-a", "race", ModelBoundaryRequest{OperationID: "suspend", ExpectedEpoch: 1})
	if err != nil || receipt.Decision.Action != "KEEP" || receipt.Decision.Reason != "return_demand" {
		t.Fatalf("late return was not rechecked: %+v %v", receipt, err)
	}
	state, _ := c.Get("tenant-a", "race")
	if state.State != "RUNNING" || !state.Autonomous.CandidateSince.IsZero() || state.Autonomous.ReleasePlan != nil {
		t.Fatalf("return did not invalidate suspension: %+v", state)
	}
	if _, err = c.ObserveIdle(context.Background(), "tenant-a", "race", IdleWindowRequest{OperationID: "next-idle", ExpectedEpoch: 1, Phase: "START", TTLMS: 1000}); err != nil {
		t.Fatal(err)
	}
	state, _ = c.Get("tenant-a", "race")
	if state.Autonomous.ReturnDemand {
		t.Fatal("new authenticated idle observation retained return intent")
	}
}

func TestCostAutonomousClosesColdIdleAndRestoresOnReturn(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "cost-aware"
	cfg.EnableAutonomousScheduler = true
	cfg.SchedulerInterval = 10 * time.Millisecond
	cfg.SchedulerStableFor = 20 * time.Millisecond
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5, JournalCostMS: 1, CooldownMS: 500}
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "closed-loop")
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "closed-loop")
		return s.State == "SUSPENDED"
	})
	state, _ := c.Get("tenant-a", "closed-loop")
	if state.Autonomous.ReleasePlan == nil || len(state.Scheduling) == 0 {
		t.Fatalf("missing observed cost receipt: %+v", state)
	}
	for _, r := range state.Scheduling {
		if r.EstimateSource != "server_observed" || r.ServerEvidence.ReleasePlan == nil {
			t.Fatalf("missing cost provenance: %+v", r)
		}
	}
	if _, err := c.ReturnDemand(context.Background(), "tenant-a", "closed-loop", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, _ := c.Get("tenant-a", "closed-loop")
		return s.State == "RUNNING" && s.Epoch == 2
	})
	result, err := c.Tool(context.Background(), "tenant-a", "closed-loop", ToolRequest{ID: "after-return", Epoch: 2, Kind: "file_write", Path: "result.txt", Content: "continued"})
	if err != nil || result.Status != "COMPLETED" {
		t.Fatalf("restored tool admission: %+v %v", result, err)
	}
}

func BenchmarkSchedulingSnapshot(b *testing.B) {
	s := &Session{TenantID: "tenant", SessionID: "session", Tools: map[string]*ToolReceipt{}, Autonomous: &AutonomousState{Epoch: 1}}
	content := strings.Repeat("x", 1024)
	for i := 0; i < 4096; i++ {
		s.Tools[fmt.Sprint(i)] = &ToolReceipt{ID: fmt.Sprint(i), Status: "COMPLETED", Result: ToolResult{Content: content}}
	}
	b.Run("bounded_observer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = schedulingSnapshot(s)
		}
	})
	b.Run("complete_history", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = copyValue(s)
		}
	})
}
