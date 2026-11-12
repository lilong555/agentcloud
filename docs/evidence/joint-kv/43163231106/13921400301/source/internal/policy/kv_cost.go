package policy

import (
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/lilong555/agentcloud/internal/signals"
)

const KVModelVersion = "shared-prefix-first-loss-v1"

// KVCalibration freezes a monotone loss-time curve for one execution regime.
// Values are additional prefill service milliseconds, excluding queue/resume.
// The key identifies engine/version, hardware, dtype, batch and timing regime.
type KVCalibration struct {
	SchemaVersion   int           `json:"schema_version"`
	ModelVersion    string        `json:"model_version"`
	SourceKind      string        `json:"source_kind"`
	BackendKind     string        `json:"backend_kind"`
	ModelID         string        `json:"model_id"`
	CalibrationKey  string        `json:"calibration_key"`
	SourceArtifact  string        `json:"source_artifact"`
	SourceSHA256    string        `json:"source_sha256"`
	SourceRunID     string        `json:"source_run_id"`
	MinPromptTokens int64         `json:"min_prompt_tokens"`
	MaxPromptTokens int64         `json:"max_prompt_tokens"`
	LossPoints      []KVLossPoint `json:"loss_points"`
}

type KVLossPoint struct {
	LostTokens     int64   `json:"lost_tokens"`
	ExtraPrefillMS float64 `json:"extra_prefill_ms"`
}

func (c *KVCalibration) Validate() error {
	if c == nil {
		return fmt.Errorf("calibration_missing")
	}
	if c.SchemaVersion != 1 || c.ModelVersion == "" || c.ModelID == "" || c.CalibrationKey == "" || c.SourceArtifact == "" || c.SourceRunID == "" {
		return fmt.Errorf("calibration_metadata_invalid")
	}
	if c.BackendKind != "gpu" && c.BackendKind != "mocker" || c.SourceKind != "physical_calibration_prediction" && c.SourceKind != "mocker_simulation" || c.BackendKind == "mocker" && c.SourceKind != "mocker_simulation" || c.BackendKind == "gpu" && c.SourceKind != "physical_calibration_prediction" {
		return fmt.Errorf("calibration_source_invalid")
	}
	hash, err := hex.DecodeString(c.SourceSHA256)
	if err != nil || len(hash) != 32 {
		return fmt.Errorf("calibration_digest_invalid")
	}
	if c.MinPromptTokens < 1 || c.MaxPromptTokens < c.MinPromptTokens || c.MaxPromptTokens > 16_777_216 || len(c.LossPoints) < 2 || len(c.LossPoints) > 4096 {
		return fmt.Errorf("calibration_range_invalid")
	}
	if c.LossPoints[0] != (KVLossPoint{}) {
		return fmt.Errorf("calibration_zero_missing")
	}
	for i, p := range c.LossPoints {
		if p.LostTokens < 0 || p.LostTokens > c.MaxPromptTokens || !finite(p.ExtraPrefillMS) || p.ExtraPrefillMS < 0 || p.ExtraPrefillMS > 86_400_000 {
			return fmt.Errorf("calibration_point_invalid")
		}
		if i > 0 && (p.LostTokens <= c.LossPoints[i-1].LostTokens || p.ExtraPrefillMS < c.LossPoints[i-1].ExtraPrefillMS) {
			return fmt.Errorf("calibration_nonmonotone")
		}
	}
	return nil
}

func (c *KVCalibration) lossMS(tokens int64) (float64, bool) {
	if tokens < 0 || tokens > c.LossPoints[len(c.LossPoints)-1].LostTokens {
		return 0, false
	}
	for i, upper := range c.LossPoints {
		if tokens == upper.LostTokens {
			return upper.ExtraPrefillMS, true
		}
		if tokens < upper.LostTokens && i > 0 {
			lower := c.LossPoints[i-1]
			ratio := float64(tokens-lower.LostTokens) / float64(upper.LostTokens-lower.LostTokens)
			return lower.ExtraPrefillMS + ratio*(upper.ExtraPrefillMS-lower.ExtraPrefillMS), true
		}
	}
	return 0, false
}

// KVAction supplies only differences actually affected by the sandbox action.
// ReturnAfterMS includes additional critical-path exposure. Common arrival,
// routing and GPU policy inputs use one shared survival distribution.
type KVAction struct {
	WorkerID      uint64  `json:"worker_id"`
	DPRank        uint32  `json:"dp_rank"`
	WorkerEpoch   string  `json:"worker_epoch"`
	GPUPolicy     string  `json:"gpu_policy"`
	ReturnAfterMS float64 `json:"return_after_ms"`
}

type KVReturnPoint struct {
	RemainingMS float64 `json:"remaining_ms"`
	Probability float64 `json:"probability"`
}

// ConditionalKVReturns extracts the event mass of the conditional Kaplan-Meier
// distribution. Remaining right-censored mass keeps the estimate unavailable.
func ConditionalKVReturns(samples []SurvivalSample, elapsedMS int64, minAtRisk int) ([]KVReturnPoint, bool) {
	ordered, result := conditionalHistory(samples, elapsedMS, minAtRisk)
	if result.Reason != "OK" {
		return nil, false
	}
	risk, survival := result.AtRisk, 1.0
	var points []KVReturnPoint
	for i := 0; i < len(ordered); {
		t, events, next := survivalGroup(ordered, i)
		if events > 0 {
			mass := survival * float64(events) / float64(risk)
			points = append(points, KVReturnPoint{RemainingMS: float64(t - elapsedMS), Probability: mass})
			survival *= float64(risk-events) / float64(risk)
		}
		risk -= next - i
		i = next
	}
	return points, survival == 0
}

type KVCostEstimate struct {
	ModelVersion         string          `json:"model_version"`
	PredictedAt          time.Time       `json:"predicted_at"`
	Valid                bool            `json:"valid"`
	FallbackReason       string          `json:"fallback_reason,omitempty"`
	ZeroEffect           bool            `json:"zero_effect"`
	SourceKind           string          `json:"source_kind,omitempty"`
	BackendKind          string          `json:"backend_kind,omitempty"`
	SourceVersion        string          `json:"source_version,omitempty"`
	FeaturesObservedAt   time.Time       `json:"features_observed_at,omitempty"`
	ObservationAgeMS     float64         `json:"observation_age_ms"`
	StatisticsObservedAt *time.Time      `json:"statistics_observed_at,omitempty"`
	SampleCount          uint64          `json:"sample_count"`
	LossEvents           uint64          `json:"loss_events"`
	ExposureMS           float64         `json:"exposure_ms"`
	HazardPerMS          *float64        `json:"hazard_per_ms,omitempty"`
	Keep                 KVAction        `json:"keep"`
	Suspend              KVAction        `json:"suspend"`
	KeepLossRisk         *float64        `json:"keep_loss_risk,omitempty"`
	SuspendLossRisk      *float64        `json:"suspend_loss_risk,omitempty"`
	KeepExpectedMS       *float64        `json:"keep_expected_reprefill_ms,omitempty"`
	SuspendExpectedMS    *float64        `json:"suspend_expected_reprefill_ms,omitempty"`
	DeltaMS              float64         `json:"delta_ms"`
	BaselineSwitchCostMS int64           `json:"baseline_switch_cost_ms"`
	TotalSwitchCostMS    float64         `json:"total_switch_cost_ms"`
	ResumeCriticalPathMS float64         `json:"resume_critical_path_ms"`
	ResumeSource         string          `json:"resume_source,omitempty"`
	ResumeObservedAt     *time.Time      `json:"resume_observed_at,omitempty"`
	CalibrationSource    string          `json:"calibration_source,omitempty"`
	CalibrationVersion   string          `json:"calibration_version,omitempty"`
	CalibrationArtifact  string          `json:"calibration_artifact,omitempty"`
	CalibrationSHA256    string          `json:"calibration_sha256,omitempty"`
	CalibrationRunID     string          `json:"calibration_run_id,omitempty"`
	AppliedToDecision    bool            `json:"applied_to_decision"`
	ReturnTimeSource     string          `json:"return_time_source,omitempty"`
	ReturnDistribution   []KVReturnPoint `json:"return_distribution,omitempty"`
}

// EstimateKV compares a shared first-loss survival model at the two action
// exposure times. Empirical loss bins retain partial-prefix losses. A missing
// signal returns a finite zero delta with an auditable reason.
func EstimateKV(o *signals.CacheObservation, calibration *KVCalibration, keep, suspend KVAction, executionEpoch uint64, now time.Time, maxAge time.Duration, minSamples uint64, returns ...KVReturnPoint) KVCostEstimate {
	r := KVCostEstimate{ModelVersion: KVModelVersion, PredictedAt: now, Keep: keep, Suspend: suspend}
	fail := func(reason string) KVCostEstimate {
		r.FallbackReason = reason
		if !finite(r.Keep.ReturnAfterMS) {
			r.Keep.ReturnAfterMS = 0
		}
		if !finite(r.Suspend.ReturnAfterMS) {
			r.Suspend.ReturnAfterMS = 0
		}
		return r
	}
	if o == nil {
		return fail("no_cache_observation")
	}
	r.SourceKind, r.BackendKind, r.SourceVersion = o.SourceKind, o.BackendKind, o.SourceVersion
	r.FeaturesObservedAt = o.ObservedAt
	r.StatisticsObservedAt = o.StatsObservedAt
	if !o.ObservedAt.IsZero() && !o.ObservedAt.After(now) {
		r.ObservationAgeMS = float64(now.Sub(o.ObservedAt)) / float64(time.Millisecond)
	}
	if o.SchemaVersion != 1 {
		return fail("cache_schema_unsupported")
	}
	if !o.Valid {
		if o.InvalidReason != "" {
			return fail(o.InvalidReason)
		}
		return fail("invalid_cache_observation")
	}
	if o.ExecutionEpoch != executionEpoch {
		return fail("execution_epoch_mismatch")
	}
	if o.SourceKind != "live_kvindexer_observation" || o.SourceVersion == "" || o.BackendKind != "gpu" && o.BackendKind != "mocker" {
		return fail("cache_source_invalid")
	}
	if maxAge <= 0 || o.ObservedAt.IsZero() || o.ObservedAt.After(now) {
		return fail("observation_time_invalid")
	}
	if now.Sub(o.ObservedAt) > maxAge || !finite(o.AgeMS) || o.AgeMS < 0 || o.AgeMS > float64(maxAge)/float64(time.Millisecond) {
		return fail("observation_expired")
	}
	if o.GapPending {
		return fail("event_gap")
	}
	if o.WorkerEpoch == "" || o.PrefixDigest == "" || keep.WorkerID != o.WorkerID || suspend.WorkerID != o.WorkerID {
		return fail("worker_mismatch")
	}
	if keep.WorkerEpoch != o.WorkerEpoch || suspend.WorkerEpoch != o.WorkerEpoch {
		return fail("worker_epoch_mismatch")
	}
	if keep.DPRank != o.DPRank || suspend.DPRank != o.DPRank {
		return fail("dp_rank_mismatch")
	}
	if keep.GPUPolicy == "" || keep.GPUPolicy != suspend.GPUPolicy {
		return fail("gpu_action_effect_unidentified")
	}
	if o.HistoryTokens < 1 || o.HistoryTokens > 16_777_216 || o.DeviceOverlapTokens < 0 || o.DeviceOverlapTokens > o.HistoryTokens || !finite(keep.ReturnAfterMS) || !finite(suspend.ReturnAfterMS) || keep.ReturnAfterMS < 0 || suspend.ReturnAfterMS < 0 || keep.ReturnAfterMS > 86_400_000 || suspend.ReturnAfterMS > 86_400_000 {
		return fail("features_out_of_range")
	}
	r.ZeroEffect = keep == suspend
	// An already absent device prefix contributes the same rebuilding cost to
	// both actions. Cohort losses concern currently resident tokens only.
	if o.DeviceOverlapTokens == 0 {
		r.Valid, r.ZeroEffect = true, true
		zero := 0.0
		r.KeepLossRisk, r.SuspendLossRisk = &zero, &zero
		if calibration.Validate() == nil && calibration.BackendKind == o.BackendKind && calibration.ModelID == o.ModelID && calibration.CalibrationKey == o.CalibrationKey && o.HistoryTokens >= calibration.MinPromptTokens && o.HistoryTokens <= calibration.MaxPromptTokens {
			if common, known := calibration.lossMS(o.HistoryTokens); known {
				r.KeepExpectedMS, r.SuspendExpectedMS = &common, &common
				r.CalibrationSource, r.CalibrationVersion = calibration.SourceKind, calibration.ModelVersion
				r.CalibrationArtifact, r.CalibrationSHA256, r.CalibrationRunID = calibration.SourceArtifact, calibration.SourceSHA256, calibration.SourceRunID
			}
		}
		return r
	}
	// Equal interventions identify the delta even before a cohort has labels.
	if r.ZeroEffect && (o.LossEvents == nil || o.ExposureMS == nil || o.SampleCount == nil || calibration == nil) {
		r.Valid = true
		return r
	}
	if err := calibration.Validate(); err != nil {
		return fail(err.Error())
	}
	r.CalibrationSource, r.CalibrationVersion = calibration.SourceKind, calibration.ModelVersion
	r.CalibrationArtifact, r.CalibrationSHA256, r.CalibrationRunID = calibration.SourceArtifact, calibration.SourceSHA256, calibration.SourceRunID
	if o.BackendKind != calibration.BackendKind || o.ModelID != calibration.ModelID || o.CalibrationKey != calibration.CalibrationKey {
		return fail("calibration_mismatch")
	}
	if o.HistoryTokens < calibration.MinPromptTokens || o.HistoryTokens > calibration.MaxPromptTokens {
		return fail("calibration_out_of_range")
	}
	if o.LossEvents == nil || o.ExposureMS == nil || o.SampleCount == nil || o.StatsObservedAt == nil {
		return fail("no_feedback")
	}
	if o.StatsObservedAt.IsZero() || !o.StatsObservedAt.Before(now) || o.StatsObservedAt.After(o.ObservedAt) || now.Sub(*o.StatsObservedAt) > maxAge {
		return fail("statistics_expired")
	}
	r.SampleCount, r.LossEvents = *o.SampleCount, *o.LossEvents
	if !finite(*o.ExposureMS) || *o.ExposureMS < 0 || *o.ExposureMS > 1e15 || r.LossEvents > r.SampleCount {
		return fail("feedback_out_of_range")
	}
	r.ExposureMS = *o.ExposureMS
	if minSamples == 0 || r.SampleCount < minSamples {
		return fail("low_support")
	}
	if r.ExposureMS == 0 {
		return fail("feedback_out_of_range")
	}
	currentMissing := o.HistoryTokens - o.DeviceOverlapTokens
	baseLoss, known := calibration.lossMS(currentMissing)
	if !known {
		return fail("calibration_out_of_range")
	}
	meanExtraLoss, total := 0.0, uint64(0)
	for _, bin := range o.LossBuckets {
		if bin.LostTokens <= 0 || bin.LostTokens > o.DeviceOverlapTokens || bin.Count == 0 || bin.Count > r.LossEvents-total {
			return fail("loss_buckets_invalid")
		}
		loss, known := calibration.lossMS(currentMissing + bin.LostTokens)
		if !known {
			return fail("calibration_out_of_range")
		}
		meanExtraLoss += float64(bin.Count) * (loss - baseLoss)
		total += bin.Count
	}
	if total != r.LossEvents {
		return fail("loss_buckets_missing")
	}
	if total > 0 {
		meanExtraLoss /= float64(total)
	}
	hazard := float64(r.LossEvents) / r.ExposureMS
	if !finite(hazard) {
		return fail("feedback_out_of_range")
	}
	r.HazardPerMS = &hazard
	keepRisk := -math.Expm1(-hazard * (r.ObservationAgeMS + keep.ReturnAfterMS))
	suspendRisk := -math.Expm1(-hazard * (r.ObservationAgeMS + suspend.ReturnAfterMS))
	if len(returns) > 0 {
		mean, mass := 0.0, 0.0
		for _, point := range returns {
			if !finite(point.RemainingMS) || !finite(point.Probability) || point.RemainingMS < 0 || point.RemainingMS > 86_400_000 || point.Probability <= 0 || point.Probability > 1 {
				return fail("return_distribution_invalid")
			}
			mean += point.Probability * point.RemainingMS
			mass += point.Probability
		}
		if math.Abs(mass-1) > 1e-10 {
			return fail("return_distribution_incomplete")
		}
		keepRisk, suspendRisk = 0, 0
		for _, point := range returns {
			kt, st := point.RemainingMS+keep.ReturnAfterMS-mean, point.RemainingMS+suspend.ReturnAfterMS-mean
			if kt < 0 || st < 0 {
				return fail("return_distribution_invalid")
			}
			keepRisk += point.Probability * -math.Expm1(-hazard*(r.ObservationAgeMS+kt))
			suspendRisk += point.Probability * -math.Expm1(-hazard*(r.ObservationAgeMS+st))
		}
		r.ReturnDistribution = append([]KVReturnPoint(nil), returns...)
	}
	keepCost, suspendCost := baseLoss+keepRisk*meanExtraLoss, baseLoss+suspendRisk*meanExtraLoss
	r.KeepLossRisk, r.SuspendLossRisk = &keepRisk, &suspendRisk
	r.KeepExpectedMS, r.SuspendExpectedMS = &keepCost, &suspendCost
	r.DeltaMS = suspendCost - keepCost
	if r.ZeroEffect {
		r.DeltaMS = 0
	}
	r.Valid = true
	return r
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
