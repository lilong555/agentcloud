package policy

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lilong555/agentcloud/internal/signals"
)

func kvFixture(now time.Time) (*signals.CacheObservation, *KVCalibration, KVAction) {
	events, samples, exposure := uint64(4), uint64(8), 8000.0
	at := now.Add(-time.Millisecond)
	o := &signals.CacheObservation{SchemaVersion: 1, ObservedAt: at, SourceKind: "live_kvindexer_observation", BackendKind: "mocker", SourceVersion: "test-observer-v1", WorkerID: 7, WorkerEpoch: "incarnation-1", ExecutionEpoch: 1, PrefixDigest: "prefix-a", HistoryTokens: 100, DeviceOverlapTokens: 100, Valid: true, LossEvents: &events, ExposureMS: &exposure, SampleCount: &samples, StatsObservedAt: &at, LossBuckets: []signals.PrefixLossBin{{LostTokens: 50, Count: 4}}, ModelID: "test-model", CalibrationKey: "mocker-test-batch1"}
	c := &KVCalibration{SchemaVersion: 1, ModelVersion: "test-frozen-v1", SourceKind: "mocker_simulation", BackendKind: "mocker", ModelID: o.ModelID, CalibrationKey: o.CalibrationKey, SourceArtifact: "test-curve.json", SourceSHA256: strings.Repeat("a", 64), SourceRunID: "training-run", MinPromptTokens: 100, MaxPromptTokens: 100, LossPoints: []KVLossPoint{{}, {LostTokens: 100, ExtraPrefillMS: 200}}}
	a := KVAction{WorkerID: 7, WorkerEpoch: "incarnation-1", GPUPolicy: "unchanged_unpinned", ReturnAfterMS: 1000}
	return o, c, a
}

func TestKVSharedInputsForceZeroEffectAndCommonRiskCancels(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	r := EstimateKV(o, c, keep, keep, 1, now, 2*time.Second, 4)
	if !r.Valid || !r.ZeroEffect || r.DeltaMS != 0 || *r.KeepLossRisk <= 0 || *r.KeepExpectedMS <= 0 || *r.KeepExpectedMS != *r.SuspendExpectedMS {
		t.Fatalf("shared cache cost: %+v", r)
	}
	o.LossEvents, o.ExposureMS, o.SampleCount, o.StatsObservedAt = nil, nil, nil, nil
	r = EstimateKV(o, nil, keep, keep, 1, now, 2*time.Second, 4)
	if !r.Valid || !r.ZeroEffect || r.DeltaMS != 0 {
		t.Fatalf("structural zero effect requires no guessed hazard: %+v", r)
	}
}

func TestKVPositiveEffectUsesOnlyAdditionalExposureAndPartialLoss(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	suspend := keep
	suspend.ReturnAfterMS += 400
	r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4)
	want := math.Exp(-.0005*1001) * -math.Expm1(-.0005*400) * 100
	if !r.Valid || r.ZeroEffect || math.Abs(r.DeltaMS-want) > 1e-10 || *r.SuspendLossRisk <= *r.KeepLossRisk || r.DeltaMS >= *r.SuspendExpectedMS {
		t.Fatalf("increment=%+v want=%f", r, want)
	}
}

func TestKVRejectsUnavailableSignalsWithFiniteAuditableFallback(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name, reason string
		edit         func(*signals.CacheObservation, *KVCalibration, *KVAction)
	}{
		{"expired", "observation_expired", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			o.ObservedAt = now.Add(-3 * time.Second)
		}},
		{"no-feedback", "no_feedback", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.LossEvents = nil }},
		{"epoch", "execution_epoch_mismatch", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.ExecutionEpoch++ }},
		{"worker", "worker_mismatch", func(_ *signals.CacheObservation, _ *KVCalibration, a *KVAction) { a.WorkerID++ }},
		{"worker-epoch", "worker_epoch_mismatch", func(_ *signals.CacheObservation, _ *KVCalibration, a *KVAction) { a.WorkerEpoch = "restarted" }},
		{"gap", "event_gap", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.GapPending = true }},
		{"nan", "feedback_out_of_range", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { *o.ExposureMS = math.NaN() }},
		{"zero-exposure", "feedback_out_of_range", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { *o.ExposureMS = 0 }},
		{"events-exceed-cohort", "feedback_out_of_range", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { *o.LossEvents = *o.SampleCount + 1 }},
		{"age-nan", "observation_expired", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.AgeMS = math.NaN() }},
		{"action-nan", "features_out_of_range", func(_ *signals.CacheObservation, _ *KVCalibration, a *KVAction) { a.ReturnAfterMS = math.NaN() }},
		{"range", "features_out_of_range", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.DeviceOverlapTokens = 101 }},
		{"calibration", "calibration_mismatch", func(_ *signals.CacheObservation, c *KVCalibration, _ *KVAction) { c.ModelID = "other-model" }},
		{"calibration-range", "calibration_out_of_range", func(_ *signals.CacheObservation, c *KVCalibration, _ *KVAction) {
			c.MinPromptTokens = 101
			c.MaxPromptTokens = 101
		}},
		{"calibration-nan", "calibration_point_invalid", func(_ *signals.CacheObservation, c *KVCalibration, _ *KVAction) {
			c.LossPoints[1].ExtraPrefillMS = math.NaN()
		}},
		{"bucket-missing", "loss_buckets_missing", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.LossBuckets = nil }},
		{"bucket-exceeds-events", "loss_buckets_invalid", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			o.LossBuckets[0].Count = *o.LossEvents + 1
		}},
		{"bucket-exceeds-resident", "loss_buckets_invalid", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) { o.DeviceOverlapTokens = 40 }},
		{"future-statistics", "statistics_expired", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			at := now.Add(time.Millisecond)
			o.StatsObservedAt = &at
		}},
		{"statistics-after-features", "statistics_expired", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			at := now.Add(-time.Millisecond / 2)
			o.StatsObservedAt = &at
		}},
		{"statistics-expired", "statistics_expired", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			at := now.Add(-3 * time.Second)
			o.StatsObservedAt = &at
		}},
		{"low-support", "low_support", func(o *signals.CacheObservation, _ *KVCalibration, _ *KVAction) {
			*o.SampleCount = 0
			*o.LossEvents = 0
			*o.ExposureMS = 0
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, c, keep := kvFixture(now)
			suspend := keep
			suspend.ReturnAfterMS += 400
			tc.edit(o, c, &suspend)
			r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4)
			if r.Valid || r.FallbackReason != tc.reason || r.DeltaMS != 0 {
				t.Fatalf("fallback: %+v", r)
			}
			if _, err := json.Marshal(r); err != nil {
				t.Fatalf("fallback audit JSON: %v", err)
			}
		})
	}
}

func TestKVPartialResidentPricesNonlinearLossAboveCommonMissingPrefix(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	o.DeviceOverlapTokens = 40
	o.LossBuckets = []signals.PrefixLossBin{{LostTokens: 20, Count: 4}}
	c.LossPoints = []KVLossPoint{{}, {LostTokens: 20, ExtraPrefillMS: 4}, {LostTokens: 50, ExtraPrefillMS: 40}, {LostTokens: 80, ExtraPrefillMS: 130}, {LostTokens: 100, ExtraPrefillMS: 200}}
	suspend := keep
	suspend.ReturnAfterMS += 400
	r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4)
	// Sixty missing tokens already cost 70ms for both actions. Losing twenty
	// resident tokens adds g(80)-g(60)=60ms to the first-loss severity.
	want := math.Exp(-.0005*1001) * -math.Expm1(-.0005*400) * 60
	if !r.Valid || math.Abs(r.DeltaMS-want) > 1e-10 || math.Abs(*r.KeepExpectedMS-(70+*r.KeepLossRisk*60)) > 1e-10 || math.Abs(*r.SuspendExpectedMS-(70+*r.SuspendLossRisk*60)) > 1e-10 {
		t.Fatalf("partial resident nonlinear cost: %+v want=%f", r, want)
	}
}

func TestKVObservedZeroLossHasZeroDeltaAndValidStatistics(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	*o.LossEvents = 0
	o.LossBuckets = nil
	suspend := keep
	suspend.ReturnAfterMS += 400
	r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4)
	if !r.Valid || r.DeltaMS != 0 || *r.KeepLossRisk != 0 || *r.SuspendLossRisk != 0 {
		t.Fatalf("zero measured events: %+v", r)
	}
}

func TestKVAlreadyLostPrefixHasIdenticalCommonRebuildingCost(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	o.DeviceOverlapTokens = 0
	suspend := keep
	suspend.ReturnAfterMS += 400
	r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4)
	if !r.Valid || !r.ZeroEffect || r.DeltaMS != 0 || *r.KeepExpectedMS != 200 || *r.SuspendExpectedMS != 200 || *r.KeepLossRisk != 0 || *r.SuspendLossRisk != 0 {
		t.Fatalf("already lost common prefix: %+v", r)
	}
	// The shared cohort still contains prior resident-prefix loss bins; those
	// cannot impose future losses on the current empty prefix.
	o.LossEvents = nil
	o.ExposureMS = nil
	o.SampleCount = nil
	r = EstimateKV(o, nil, keep, suspend, 1, now, 2*time.Second, 4)
	if !r.Valid || !r.ZeroEffect || r.DeltaMS != 0 {
		t.Fatalf("zero resident tokens: %+v", r)
	}
}

func TestKVConditionalReturnsIntegratePastEventMassAndRejectUnknownTail(t *testing.T) {
	now := time.Now().UTC()
	o, c, keep := kvFixture(now)
	samples := []SurvivalSample{{DurationMS: 1000, Completed: true}, {DurationMS: 3000, Completed: true}}
	points, known := ConditionalKVReturns(samples, 0, 2)
	if !known || len(points) != 2 || points[0].Probability != .5 || points[1].Probability != .5 {
		t.Fatalf("conditional mass: %+v %v", points, known)
	}
	keep.ReturnAfterMS = 2000
	suspend := keep
	suspend.ReturnAfterMS += 400
	r := EstimateKV(o, c, keep, suspend, 1, now, 2*time.Second, 4, points...)
	want := (.5*math.Exp(-.0005*1001) + .5*math.Exp(-.0005*3001)) * -math.Expm1(-.0005*400) * 100
	if !r.Valid || math.Abs(r.DeltaMS-want) > 1e-10 {
		t.Fatalf("integrated increment %+v want=%f", r, want)
	}
	samples[1].Completed = false
	if _, known = ConditionalKVReturns(samples, 0, 2); known {
		t.Fatal("right-censored tail was assigned zero cost")
	}
}
