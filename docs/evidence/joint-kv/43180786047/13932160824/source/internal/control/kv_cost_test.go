package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/observability"
	"github.com/lilong555/agentcloud/internal/policy"
	"github.com/lilong555/agentcloud/internal/signals"
)

func jointEvidence(now time.Time, duration int64) (*ServerEstimateEvidence, *policy.KVCalibration) {
	e := predictionEvidence(now, duration)
	worker, rank := uint64(7), uint32(0)
	e.SelectedWorkerID, e.SelectedDPRank = &worker, &rank
	events, samples, exposure := uint64(4), uint64(8), 8000.0
	at := now.Add(-time.Millisecond)
	e.CacheObservation = &signals.CacheObservation{SchemaVersion: 1, ObservedAt: at, SourceKind: "live_kvindexer_observation", BackendKind: "mocker", SourceVersion: "training-observer-v1", WorkerID: 7, WorkerEpoch: "producer-a", ExecutionEpoch: 1, PrefixDigest: "prefix-a", HistoryTokens: 100, DeviceOverlapTokens: 100, Valid: true, LossEvents: &events, ExposureMS: &exposure, SampleCount: &samples, StatsObservedAt: &at, LossBuckets: []signals.PrefixLossBin{{LostTokens: 50, Count: 4}}, ModelID: "test-model", CalibrationKey: "mocker-batch1-v1"}
	cal := &policy.KVCalibration{SchemaVersion: 1, ModelVersion: "frozen-v1", SourceKind: "mocker_simulation", BackendKind: "mocker", ModelID: "test-model", CalibrationKey: "mocker-batch1-v1", SourceArtifact: "training-prefill-curve.json", SourceSHA256: strings.Repeat("a", 64), SourceRunID: "training-only", MinPromptTokens: 100, MaxPromptTokens: 100, LossPoints: []policy.KVLossPoint{{}, {LostTokens: 100, ExtraPrefillMS: 30000}}}
	return e, cal
}

func addJointResume(c *Controller, s *Session, now time.Time, ms float64) {
	c.state.Observations = append(c.state.Observations, RecordedMeasurement{TenantID: s.TenantID, SessionID: s.SessionID, Measurement: observability.Measurement{At: now.Add(-time.Second), Metric: "resume_ready_ms", Value: ms, Source: "backend_resume_attach_completion"}})
}

func TestJointKVChangesRealV2ScoreAndRetainsFiniteDeadline(t *testing.T) {
	now := time.Now().UTC()
	baseline, bs, cfg := costFixture(now)
	baseline.cfg.SchedulerPolicy, baseline.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
	be, _ := jointEvidence(now, 1000)
	bd, err := baseline.v2DecisionLocked(bs, ModelBoundaryRequest{}, be, cfg, now.Add(500*time.Millisecond))
	if err != nil || bd.Action != "SUSPEND" {
		t.Fatalf("baseline: %+v %v", bd, err)
	}
	c, s, cfg := costFixture(now)
	c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor = "agentcloud-v2-joint", 500*time.Millisecond
	e, cal := jointEvidence(now, 1000)
	c.cfg.KVCalibration = cal
	addJointResume(c, s, now, 400)
	d, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(500*time.Millisecond))
	kv := s.Autonomous.Episode.KVCost
	if err != nil || d.Action != "KEEP" || !kv.Valid || !kv.AppliedToDecision || kv.DeltaMS <= 0 || s.Autonomous.Episode.SwitchCostMS <= kv.BaselineSwitchCostMS {
		t.Fatalf("joint decision: %+v estimate=%+v %v", d, kv, err)
	}
	if kv.TotalSwitchCostMS != float64(kv.BaselineSwitchCostMS)+kv.DeltaMS || kv.ResumeCriticalPathMS != 400 || kv.ResumeSource != "historical_backend_resume_attach_mean" {
		t.Fatalf("cost/provenance: %+v", kv)
	}
	latest := s.Autonomous.Episode.KVLatestReleaseAt
	if latest.IsZero() || !latest.Equal(now.Add(30*time.Second)) {
		t.Fatalf("bounded KEEP: %+v", s.Autonomous.Episode)
	}
	for _, delta := range []time.Duration{time.Second, 10 * time.Second, 30 * time.Second} {
		at := now.Add(delta)
		e.CacheObservation.ObservedAt = at.Add(-time.Millisecond)
		e.CacheObservation.StatsObservedAt = &e.CacheObservation.ObservedAt
		_, err = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, at)
		if err != nil || s.Autonomous.Episode.KVLatestReleaseAt != latest || s.Autonomous.Episode.PlannedReleaseAt.After(latest) {
			t.Fatalf("deadline refreshed: %+v %v", s.Autonomous.Episode, err)
		}
	}
}

func TestJointKVZeroAndInvalidEvidenceLeaveBaselineDecisionsUnchanged(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"zero-duration", "no-feedback", "expired", "no-resume"} {
		t.Run(scenario, func(t *testing.T) {
			for _, duration := range []int64{-1, 1000, 10000} {
				bc, bs, cfg := costFixture(now)
				bc.cfg.SchedulerPolicy, bc.cfg.SchedulerStableFor = "agentcloud-v2", 500*time.Millisecond
				be, cal := jointEvidence(now, duration)
				c, s, _ := costFixture(now)
				c.cfg.SchedulerPolicy, c.cfg.SchedulerStableFor, c.cfg.KVCalibration = "agentcloud-v2-joint", 500*time.Millisecond, cal
				e, _ := jointEvidence(now, duration)
				resume := 400.0
				if scenario == "zero-duration" {
					resume = 0
				}
				if scenario != "no-resume" {
					addJointResume(c, s, now, resume)
				}
				if scenario == "no-feedback" {
					e.CacheObservation.LossEvents = nil
				}
				if scenario == "expired" {
					e.CacheObservation.ObservedAt = now.Add(-3 * time.Second)
				}
				for _, delta := range []time.Duration{0, 500 * time.Millisecond, 2 * time.Second} {
					bd, beErr := bc.v2DecisionLocked(bs, ModelBoundaryRequest{}, be, cfg, now.Add(delta))
					jd, jeErr := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(delta))
					b, j := bs.Autonomous.Episode, s.Autonomous.Episode
					if beErr != nil || jeErr != nil || !reflect.DeepEqual(bd, jd) || b.SwitchCostMS != j.SwitchCostMS || b.PlannedReleaseAt != j.PlannedReleaseAt || b.Selection != j.Selection {
						t.Fatalf("duration=%d time=%s baseline=%+v joint=%+v", duration, delta, b, j)
					}
					if j.KVCost == nil || j.KVCost.DeltaMS != 0 {
						t.Fatalf("missing finite fallback/zero audit: %+v", j.KVCost)
					}
				}
			}
		})
	}
}

func TestJointKVUnknownWaitObservesCostAndKeepsTwoSecondFallback(t *testing.T) {
	now := time.Now().UTC()
	c, s, cfg := costFixture(now)
	c.cfg.SchedulerPolicy = "agentcloud-v2-joint"
	e, cal := jointEvidence(now, -1)
	c.cfg.KVCalibration = cal
	addJointResume(c, s, now, 400)
	_, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now)
	kv, episode := s.Autonomous.Episode.KVCost, s.Autonomous.Episode
	if err != nil || !kv.Valid || kv.AppliedToDecision || kv.DeltaMS <= 0 || kv.ReturnTimeSource != "unknown_two_second_deadline_proxy" || episode.SwitchCostMS != 200 || !episode.PlannedReleaseAt.Equal(now.Add(2*time.Second)) {
		t.Fatalf("unknown wait: %+v %v", episode, err)
	}
}

func TestJointKVSafePointAndReturnDemandHavePriority(t *testing.T) {
	now := time.Now().UTC()
	for _, returnDemand := range []bool{false, true} {
		c, s, cfg := costFixture(now)
		c.cfg.SchedulerPolicy = "agentcloud-v2-joint"
		e, cal := jointEvidence(now, 10000)
		c.cfg.KVCalibration = cal
		addJointResume(c, s, now, 400)
		if returnDemand {
			s.Autonomous = &AutonomousState{Epoch: 1, ReturnDemand: true}
		} else {
			s.ActiveTools = 1
		}
		d, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now.Add(time.Second))
		want := "unsafe_point"
		if returnDemand {
			want = "return_demand"
		}
		if err != nil || d.Action != "KEEP" || d.Reason != want || s.Autonomous != nil && s.Autonomous.Episode != nil {
			t.Fatalf("safety gate: %+v %v", d, err)
		}
	}
}

func TestJointKVChecksActualSelectedWorkerAndRank(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name, reason string
		edit         func(*ServerEstimateEvidence)
	}{
		{"unselected", "selected_worker_unavailable", func(e *ServerEstimateEvidence) { e.SelectedWorkerID = nil }},
		{"migrated", "worker_mismatch", func(e *ServerEstimateEvidence) { *e.SelectedWorkerID = 8 }},
		{"rank", "dp_rank_mismatch", func(e *ServerEstimateEvidence) { *e.SelectedDPRank = 1 }},
		{"removed", "worker_removed", func(e *ServerEstimateEvidence) {
			e.CacheObservation.Valid = false
			e.CacheObservation.InvalidReason = "worker_removed"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s, cfg := costFixture(now)
			c.cfg.SchedulerPolicy = "agentcloud-v2-joint"
			e, cal := jointEvidence(now, 1000)
			c.cfg.KVCalibration = cal
			addJointResume(c, s, now, 400)
			tc.edit(e)
			_, err := c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, cfg, now)
			kv := s.Autonomous.Episode.KVCost
			if err != nil || kv.Valid || kv.DeltaMS != 0 || kv.FallbackReason != tc.reason || s.Autonomous.Episode.SwitchCostMS != 200 {
				t.Fatalf("selection fence: %+v %v", kv, err)
			}
		})
	}
}

func TestJointKVResumeExposureUsesPastScopedBackendCompletions(t *testing.T) {
	now := time.Now().UTC()
	c, s, _ := costFixture(now)
	c.cfg.KVResumeMaxAge = time.Minute
	addJointResume(c, s, now, 10)
	addJointResume(c, s, now, 30)
	for _, edit := range []func(*RecordedMeasurement){
		func(m *RecordedMeasurement) { m.At = now.Add(time.Second) },
		func(m *RecordedMeasurement) { m.At = now.Add(-2 * time.Minute) },
		func(m *RecordedMeasurement) { m.Source = "caller_estimate" },
		func(m *RecordedMeasurement) { m.TenantID = "other-tenant" },
		func(m *RecordedMeasurement) { m.SessionID = "missing-peer" },
		func(m *RecordedMeasurement) { m.Metric = "checkpoint_total_ms" },
		func(m *RecordedMeasurement) { m.Value = math.NaN() },
		func(m *RecordedMeasurement) { m.Value = -1 },
	} {
		m := c.state.Observations[0]
		m.Value = 999
		edit(&m)
		c.state.Observations = append(c.state.Observations, m)
	}
	for _, peer := range []*Session{
		{TenantID: s.TenantID, SessionID: "other-profile", Profile: "other", Backend: s.Backend},
		{TenantID: s.TenantID, SessionID: "other-backend", Profile: s.Profile, Backend: "other"},
	} {
		c.state.Sessions[key(peer.TenantID, peer.SessionID)] = peer
		addJointResume(c, peer, now, 999)
	}
	ms, at, source := c.kvResumeExposureLocked(s, now)
	if ms != 20 || at == nil || !at.Equal(now.Add(-time.Second)) || source != "historical_backend_resume_attach_mean" {
		t.Fatalf("resume exposure scope/time: %f %v %s", ms, at, source)
	}
}

func TestJointKVIntegratesConditionalReturnsAtCurrentWaitAge(t *testing.T) {
	now := time.Now().UTC()
	c, s, _ := costFixture(now)
	e, cal := jointEvidence(now.Add(-500*time.Millisecond), 1000)
	e.WaitAgeMS = 500
	e.CacheObservation.ObservedAt = now.Add(-time.Millisecond)
	e.CacheObservation.StatsObservedAt = &e.CacheObservation.ObservedAt
	e.waitSamples = []policy.SurvivalSample{{DurationMS: 1000, Completed: true}, {DurationMS: 1000, Completed: true}, {DurationMS: 3000, Completed: true}, {DurationMS: 3000, Completed: true}}
	p := policy.ConditionalRemaining(e.waitSamples, e.WaitAgeMS, .75, waitMinimumSupport)
	e.WaitPrediction = &p
	c.cfg.KVCalibration = cal
	addJointResume(c, s, now, 400)
	episode := &WaitEpisode{SwitchCostMS: 200}
	c.jointKVCostLocked(s, episode, e, now)
	r := episode.KVCost
	want := (.5*math.Exp(-.0005*501) + .5*math.Exp(-.0005*2501)) * -math.Expm1(-.0005*400) * 15000
	if !r.Valid || !r.AppliedToDecision || len(r.ReturnDistribution) != 2 || r.Keep.ReturnAfterMS != 1500 || math.Abs(r.DeltaMS-want) > 1e-8 || r.TotalSwitchCostMS != 200+r.DeltaMS || episode.SwitchCostMS != int64(math.Ceil(200+r.DeltaMS)) {
		t.Fatalf("conditional return exposure: %+v want=%f", r, want)
	}
}

func TestJointKVEpisodeAndRawObservationPersistAcrossJSONJournalRestart(t *testing.T) {
	cfg := testConfig(t)
	cfg.SchedulerPolicy = "agentcloud-v2-joint"
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	createTestSession(t, c, "tenant-a", "joint-json")
	c.lock()
	s := c.state.Sessions[key("tenant-a", "joint-json")]
	now := time.Now().UTC()
	e, cal := jointEvidence(now, 1000)
	c.cfg.KVCalibration = cal
	addJointResume(c, s, now, 400)
	_, err = c.v2DecisionLocked(s, ModelBoundaryRequest{}, e, policy.DefaultLifecycleConfig(), now)
	s.Autonomous.Evidence = e
	c.markSession(s)
	if err == nil {
		err = c.persist()
	}
	before := copyValue(s.Autonomous)
	c.mu.Unlock()
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c = newTestController(t, cfg)
	restored, _ := c.Get("tenant-a", "joint-json")
	if !reflect.DeepEqual(before, restored.Autonomous) {
		t.Fatalf("persisted joint evidence mismatch: %+v", restored.Autonomous)
	}
	if restored.Autonomous.Episode.KVCost == nil || restored.Autonomous.Evidence.CacheObservation == nil {
		t.Fatal("joint audit was discarded")
	}
}

func TestLoadKVCalibrationVerifiesFrozenEvidenceArtifact(t *testing.T) {
	dir := t.TempDir()
	evidence := []byte(`{"training_seed":42,"prefill_ms":200}`)
	if err := os.WriteFile(filepath.Join(dir, "training.json"), evidence, 0600); err != nil {
		t.Fatal(err)
	}
	_, cal := jointEvidence(time.Now().UTC(), 1000)
	sum := sha256.Sum256(evidence)
	cal.SourceSHA256 = hex.EncodeToString(sum[:])
	cal.SourceArtifact = "training.json"
	data, _ := json.Marshal(cal)
	path := filepath.Join(dir, "calibration.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKVCalibration(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "training.json"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKVCalibration(path); err == nil {
		t.Fatal("modified calibration evidence passed verification")
	}
}
