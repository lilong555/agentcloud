package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

func TestV2UnknownDeadlineIsIndependentOfCostsAndSurvivesPhaseChange(t *testing.T) {
	started := time.Now().UTC()
	for _, cost := range []int64{1, 10000} {
		t.Run(fmt.Sprint(cost), func(t *testing.T) {
			c, s, cfg := costFixture(started)
			c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
			cfg.SnapshotCostMS, cfg.ResumeCostMS = cost, cost
			e := predictionEvidence(started, -1)
			e.WaitPhase = "model"
			d, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started)
			if err != nil || d.Action != "KEEP" {
				t.Fatalf("initial decision: %+v %v", d, err)
			}
			id := s.Autonomous.Episode.ID
			for _, elapsed := range []time.Duration{500 * time.Millisecond, 1999 * time.Millisecond, 2 * time.Second} {
				e.WaitPhase, e.WaitStartedAt = "implicit_idle", started.Add(500*time.Millisecond)
				d, err = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started.Add(elapsed))
				want := "KEEP"
				if elapsed == 2*time.Second {
					want = "SUSPEND"
				}
				ep := s.Autonomous.Episode
				if err != nil || d.Action != want || ep.ID != id || !ep.UnknownReleaseAt.Equal(started.Add(2*time.Second)) || !ep.PlannedReleaseAt.Equal(ep.UnknownReleaseAt) {
					t.Fatalf("elapsed=%s decision=%+v episode=%+v error=%v", elapsed, d, ep, err)
				}
			}
		})
	}
}

func TestV2KnownCostProtectsKeepAndSelectsEarliestSafeRelease(t *testing.T) {
	started := time.Now().UTC()
	for _, tc := range []struct {
		name                       string
		switchCost, total, elapsed int64
		want                       string
	}{{"expensive_restore_keep", 10000, 3500, 2500, "KEEP"}, {"long_wait_release", 200, 10000, 500, "SUSPEND"}} {
		t.Run(tc.name, func(t *testing.T) {
			c, s, cfg := costFixture(started)
			c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
			cfg.SnapshotCostMS, cfg.ResumeCostMS = 0, tc.switchCost
			e := predictionEvidence(started, tc.total)
			e.WaitAgeMS = tc.elapsed
			prediction := policy.ConditionalRemaining(e.waitSamples, tc.elapsed, .75, waitMinimumSupport)
			e.WaitPrediction = &prediction
			d, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started.Add(time.Duration(tc.elapsed)*time.Millisecond))
			if err != nil || d.Action != tc.want {
				t.Fatalf("decision=%+v episode=%+v err=%v", d, s.Autonomous.Episode, err)
			}
			if tc.want == "KEEP" && (!s.Autonomous.Episode.PlannedReleaseAt.IsZero() || s.Autonomous.Episode.Selection != "conditional_keep") {
				t.Fatalf("known KEEP lost to the unknown deadline: %+v", s.Autonomous.Episode)
			}
		})
	}
}

func TestV2LowSupportAndUnknownTailUseFixedFallback(t *testing.T) {
	started := time.Now().UTC()
	for _, censored := range []bool{false, true} {
		c, s, cfg := costFixture(started)
		c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
		e := predictionEvidence(started, 10000)
		if censored {
			for i := range e.waitSamples {
				e.waitSamples[i].Completed = false
			}
		} else {
			e.waitSamples = e.waitSamples[:3]
		}
		prediction := policy.ConditionalRemaining(e.waitSamples, 0, .75, waitMinimumSupport)
		e.WaitPrediction = &prediction
		_, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started)
		if err != nil || s.Autonomous.Episode.PredictionKnown || !s.Autonomous.Episode.PlannedReleaseAt.Equal(started.Add(2*time.Second)) {
			t.Fatalf("unsupported history: %+v %v", s.Autonomous.Episode, err)
		}
	}
}

func TestV2UnsafeAndEpochTerminateEpisode(t *testing.T) {
	started := time.Now().UTC()
	c, s, cfg := costFixture(started)
	c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
	e := predictionEvidence(started, -1)
	_, _ = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started)
	id := s.Autonomous.Episode.ID
	s.Tools = map[string]*ToolReceipt{"unknown": {Status: "OUTCOME_UNKNOWN", Epoch: 1}}
	d, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started.Add(time.Second))
	if err != nil || d.Action != "KEEP" || d.Reason != "unsafe_point" || s.Autonomous.Episode != nil {
		t.Fatalf("unknown tool: %+v %+v %v", d, s.Autonomous, err)
	}
	delete(s.Tools, "unknown")
	_, _ = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started.Add(time.Second))
	if s.Autonomous.Episode.ID == id || !s.Autonomous.Episode.StartedAt.Equal(started.Add(time.Second)) {
		t.Fatalf("unsafe interval reused old age: %+v", s.Autonomous.Episode)
	}
	id = s.Autonomous.Episode.ID
	s.Epoch++
	s.LastResumedAt = started.Add(2 * time.Second)
	_, _ = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, started.Add(2*time.Second))
	if s.Autonomous.Episode.ID == id || s.Autonomous.Episode.Epoch != 2 || !s.Autonomous.Episode.UnknownReleaseAt.Equal(started.Add(4*time.Second)) {
		t.Fatalf("epoch retained old episode: %+v", s.Autonomous.Episode)
	}
}

func TestV2ModelEpisodePersistsAndToolAdmissionEndsIt(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "agentcloud-v2"
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, c, "tenant-a", "v2-persistent")
	r, err := c.Submit("tenant-a", "v2-persistent", "model-1", json.RawMessage(`{"messages":[]}`))
	if err != nil {
		_ = c.Close()
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		c.lock()
		defer c.mu.Unlock()
		return c.state.Requests[key("tenant-a", "model-1")].Status == "COMPLETED"
	})
	s, _ := c.Get("tenant-a", "v2-persistent")
	episode := s.Autonomous.Episode
	if episode == nil || !episode.StartedAt.Equal(r.CreatedAt) || episode.Origin != "model:model-1" {
		_ = c.Close()
		t.Fatalf("model registration clock: %+v", episode)
	}
	c.lock()
	live := c.state.Sessions[key("tenant-a", "v2-persistent")]
	now := time.Now().UTC()
	input, evidence := c.serverEstimateLocked(live, ModelBoundaryRequest{}, nil, nil, policy.DefaultLifecycleConfig(), now)
	_, err = c.schedulerDecisionLocked(live, input, evidence, policy.DefaultLifecycleConfig(), now)
	if err == nil {
		err = c.persist()
	}
	episode = copyValue(live.Autonomous.Episode)
	c.mu.Unlock()
	if err != nil || evidence.WaitPhase != "implicit_idle" || !episode.StartedAt.Equal(r.CreatedAt) || !episode.UnknownReleaseAt.Equal(r.CreatedAt.Add(2*time.Second)) {
		_ = c.Close()
		t.Fatalf("model-to-idle renewed deadline: phase=%s episode=%+v error=%v", evidence.WaitPhase, episode, err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	s, _ = c.Get("tenant-a", "v2-persistent")
	if !reflect.DeepEqual(episode, s.Autonomous.Episode) {
		t.Fatalf("restart episode: %+v", s.Autonomous.Episode)
	}
	_, err = c.Tool(context.Background(), "tenant-a", "v2-persistent", ToolRequest{ID: "work", Epoch: 1, Kind: "file_write", Path: "proof.txt", Content: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	s, _ = c.Get("tenant-a", "v2-persistent")
	if s.Autonomous.Episode != nil || !s.Autonomous.CandidateSince.IsZero() {
		t.Fatalf("tool retained episode: %+v", s.Autonomous)
	}
}

func TestV2ToolCompletionAndResumeAnchorClockBeforeFirstSample(t *testing.T) {
	started := time.Now().UTC()
	for _, delayed := range []time.Duration{100 * time.Millisecond, 1900 * time.Millisecond} {
		c, s, cfg := costFixture(started)
		c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
		finished := started.Add(time.Second)
		s.Tools = map[string]*ToolReceipt{"work": {Epoch: 1, Status: "COMPLETED", FinishedAt: &finished}}
		e := predictionEvidence(started, -1)
		_, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, finished.Add(delayed))
		if err != nil || !s.Autonomous.Episode.StartedAt.Equal(finished) || !s.Autonomous.Episode.PlannedReleaseAt.Equal(finished.Add(2*time.Second)) {
			t.Fatalf("tool first sample=%s episode=%+v error=%v", delayed, s.Autonomous.Episode, err)
		}
		s.Epoch++
		s.LastResumedAt = started.Add(4 * time.Second)
		_, err = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, s.LastResumedAt.Add(delayed))
		if err != nil || !s.Autonomous.Episode.StartedAt.Equal(s.LastResumedAt) || !s.Autonomous.Episode.PlannedReleaseAt.Equal(s.LastResumedAt.Add(2*time.Second)) {
			t.Fatalf("resume first sample=%s episode=%+v error=%v", delayed, s.Autonomous.Episode, err)
		}
	}
}

func TestV2ExpiredExplicitWaitStartsNewEpisodeWithoutIntermediateSample(t *testing.T) {
	started := time.Now().UTC()
	c, s, _ := costFixture(started)
	c.cfg.SchedulerPolicy = "agentcloud-v2"
	first := c.beginWaitEpisodeLocked(s, "explicit_idle:first", started, started)
	first.ValidUntil = started.Add(time.Second)
	secondStart := started.Add(1500 * time.Millisecond)
	second := c.beginWaitEpisodeLocked(s, "explicit_idle:second", secondStart, secondStart.Add(time.Second))
	if first.ID == second.ID || !second.StartedAt.Equal(secondStart) || !second.UnknownReleaseAt.Equal(secondStart.Add(2*time.Second)) {
		t.Fatalf("expired wait retained episode: %+v", second)
	}
}

func TestV2AutonomousAndBoundaryUseSameDeadlineAndPolicy(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy, cfg.SchedulerStableFor = "agentcloud-v2", 10*time.Millisecond
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5}
	cfg.QueueObserver = costQueueFunc(func(context.Context, *Session) (QueueObservation, error) {
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, nil
	})
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "v2-boundary")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "v2-boundary")]
	s.IdleSince, s.RunningSince = time.Now().Add(-3*time.Second), time.Now().Add(-time.Minute)
	snapshot := schedulingSnapshot(s)
	c.mu.Unlock()
	c.autonomousStep(snapshot)
	s, _ = c.Get("tenant-a", "v2-boundary")
	if s.State != "SUSPENDED" || len(s.Scheduling) != 1 {
		t.Fatalf("autonomous result: %+v", s)
	}
	for _, receipt := range s.Scheduling {
		if receipt.Decision.Reason != "v2_unknown_two_second_deadline" || receipt.ServerEvidence.Episode == nil || receipt.ServerEvidence.ReleasePlan != nil || receipt.ServerEvidence.Episode.ID != s.Autonomous.Episode.ID {
			t.Fatalf("boundary changed policy: %+v", receipt)
		}
	}
}

func TestV2BoundaryRechecksReturnAndIgnoresCallerWait(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "agentcloud-v2"
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "v2-return")
	c.lock()
	c.cfg.EnableAutonomousScheduler = true
	s := c.state.Sessions[key("tenant-a", "v2-return")]
	s.IdleSince = time.Now().Add(-3 * time.Second)
	c.cfg.QueueObserver = costQueueFunc(func(ctx context.Context, _ *Session) (QueueObservation, error) {
		_, err := c.ReturnDemand(ctx, "tenant-a", "v2-return", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1})
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, err
	})
	c.mu.Unlock()
	r, err := c.ModelBoundary(context.Background(), "tenant-a", "v2-return", ModelBoundaryRequest{OperationID: "release", ExpectedEpoch: 1, ExpectedWaitMS: 999999})
	if err != nil || r.Decision.Action != "KEEP" || r.Decision.Reason != "return_demand" || r.EstimateSource != "server_observed" {
		t.Fatalf("return during observation: %+v %v", r, err)
	}
}

func TestV2AutonomousRecordsReturnDuringBoundaryRecheck(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy, cfg.SchedulerStableFor = "agentcloud-v2", time.Millisecond
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "v2-recheck")
	c.lock()
	c.cfg.EnableAutonomousScheduler = true
	s := c.state.Sessions[key("tenant-a", "v2-recheck")]
	s.IdleSince, s.RunningSince = time.Now().Add(-3*time.Second), time.Now().Add(-time.Minute)
	count := 0
	c.cfg.QueueObserver = costQueueFunc(func(ctx context.Context, _ *Session) (QueueObservation, error) {
		count++
		var err error
		if count == 2 {
			_, err = c.ReturnDemand(ctx, "tenant-a", "v2-recheck", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1})
		}
		return QueueObservation{Source: "native_test", Scope: "classifier", ObservedAt: time.Now().UTC()}, err
	})
	snapshot := schedulingSnapshot(s)
	c.mu.Unlock()
	c.autonomousStep(snapshot)
	s, _ = c.Get("tenant-a", "v2-recheck")
	if count != 2 || s.State != "RUNNING" || s.Autonomous.LastAction != "KEEP" || s.Autonomous.Reason != "return_demand" || len(s.Lifecycle) != 0 {
		t.Fatalf("boundary return result: %+v", s)
	}
	last := s.Autonomous.Diagnostics[len(s.Autonomous.Diagnostics)-1]
	if last.ComputedAction != "SUSPEND" || last.Action != "KEEP" || last.Reason != "return_demand" {
		t.Fatalf("boundary return diagnostic: %+v", last)
	}
}

func TestShadowComputesOldCostWithoutAutonomousActions(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy, cfg.SchedulerShadow = "agentcloud-v2", true
	cfg.EnableAutonomousScheduler, cfg.EnableExperimentControls = true, true
	cfg.SchedulerInterval, cfg.SchedulerStableFor = time.Hour, time.Millisecond
	cfg.Scheduling = policy.LifecycleConfig{SnapshotCostMS: 5, ResumeCostMS: 5}
	c := newTestController(t, cfg)
	createTestSession(t, c, "tenant-a", "shadow")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "shadow")]
	s.IdleSince = time.Now().Add(-3 * time.Second)
	s.Autonomous = &AutonomousState{Epoch: 1, CandidateSince: time.Now().Add(-time.Second)}
	snapshot := schedulingSnapshot(s)
	c.mu.Unlock()
	c.autonomousStep(snapshot)
	s, _ = c.Get("tenant-a", "shadow")
	if s.State != "RUNNING" || len(s.Scheduling) != 0 || len(s.Lifecycle) != 0 || s.Autonomous.Episode != nil || s.Autonomous.ReleasePlan == nil || s.Autonomous.Evidence.ComputedAction != "SUSPEND" || s.Autonomous.LastAction != "KEEP" {
		t.Fatalf("shadow suspension: %+v", s)
	}
	r, err := c.ModelBoundary(context.Background(), "tenant-a", "shadow", ModelBoundaryRequest{OperationID: "shadow-boundary", ExpectedEpoch: 1})
	if err != nil || r.Decision.Action != "KEEP" || r.ServerEvidence.ComputedAction != "SUSPEND" {
		t.Fatalf("shadow boundary: %+v %v", r, err)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "shadow", "suspend", LifecycleRequest{OperationID: "ttl-suspend", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ReturnDemand(context.Background(), "tenant-a", "shadow", LifecycleRequest{OperationID: "return", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	s, _ = c.Get("tenant-a", "shadow")
	c.autonomousStep(schedulingSnapshot(s))
	s, _ = c.Get("tenant-a", "shadow")
	if s.State != "SUSPENDED" || s.Epoch != 1 || s.Autonomous.Evidence.ComputedAction != "RESUME" || len(s.Lifecycle) != 1 {
		t.Fatalf("shadow resumed: %+v", s)
	}
	_, _, err = c.Lifecycle(context.Background(), "tenant-a", "shadow", "resume", LifecycleRequest{OperationID: "ttl-resume", ExpectedEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerDiagnosticsAreBounded(t *testing.T) {
	c, s, _ := costFixture(time.Now().UTC())
	c.cfg.SchedulerShadow = true
	s.Autonomous = &AutonomousState{Epoch: 1}
	for i := 0; i < 3*schedulerDiagnosticLimit; i++ {
		c.recordSchedulerDiagnosticLocked(s, nil, "SUSPEND", fmt.Sprint(i), "KEEP", "shadow_action_suppressed", 1, 2, time.Now().UTC())
	}
	rows := s.Autonomous.Diagnostics
	if len(rows) != schedulerDiagnosticLimit || rows[0].ComputedReason != "64" || rows[len(rows)-1].ComputedReason != "95" {
		t.Fatalf("diagnostic retention: %+v", rows)
	}
}

func TestSchedulerShadowRequiresExplicitControlsAndReportsCapabilities(t *testing.T) {
	for _, tc := range []struct{ enabled, experiment bool }{{false, false}, {true, false}, {false, true}} {
		cfg := testConfig(t)
		cfg.SchedulerShadow, cfg.EnableAutonomousScheduler, cfg.EnableExperimentControls = true, tc.enabled, tc.experiment
		if c, err := New(context.Background(), cfg); err == nil {
			_ = c.Close()
			t.Fatalf("accepted shadow config: %+v", tc)
		}
	}
	cfg := testConfig(t)
	cfg.SchedulerShadow, cfg.EnableAutonomousScheduler, cfg.EnableExperimentControls = true, true, true
	cfg.SchedulerInterval = time.Hour
	c := newTestController(t, cfg)
	req := httptest.NewRequest("GET", "/v1/capabilities", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	response := httptest.NewRecorder()
	c.Handler().ServeHTTP(response, req)
	var capability map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &capability); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || capability["scheduler_policy"] != "observed" || capability["scheduler_effective_policy"] != "cost-aware" || capability["scheduler_shadow"] != true {
		t.Fatalf("capabilities: %s", response.Body.String())
	}
}
