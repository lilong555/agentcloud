package control

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
)

// LoadKVCalibration loads frozen calibration and verifies its referenced local
// evidence artifact. No fitting or evaluation labels are consumed at startup.
func LoadKVCalibration(path string) (*policy.KVCalibration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("KV calibration exceeds 1 MiB")
	}
	var calibration policy.KVCalibration
	if err = json.Unmarshal(data, &calibration); err != nil {
		return nil, err
	}
	if err = calibration.Validate(); err != nil {
		return nil, err
	}
	artifact := calibration.SourceArtifact
	if !filepath.IsAbs(artifact) {
		if _, err = os.Stat(artifact); err != nil {
			artifact = filepath.Join(filepath.Dir(path), artifact)
		}
	}
	evidence, err := os.ReadFile(artifact)
	if err != nil {
		return nil, fmt.Errorf("KV calibration evidence: %w", err)
	}
	digest := sha256.Sum256(evidence)
	if hex.EncodeToString(digest[:]) != calibration.SourceSHA256 {
		return nil, fmt.Errorf("KV calibration evidence digest mismatch")
	}
	return &calibration, nil
}

// Historical backend completion measurements predict the critical path of a
// demand-triggered resume. This controller starts resumes on return demand;
// tool execution consequently supplies zero pre-return resume overlap.
func (c *Controller) kvResumeExposureLocked(s *Session, now time.Time) (float64, *time.Time, string) {
	maxAge := c.cfg.KVResumeMaxAge
	if maxAge == 0 {
		maxAge = 30 * time.Minute
	}
	values, newest := make([]float64, 0, 32), time.Time{}
	for i := len(c.state.Observations) - 1; i >= 0 && len(values) < 32; i-- {
		m := c.state.Observations[i]
		peer := c.state.Sessions[key(m.TenantID, m.SessionID)]
		if m.Metric != "resume_ready_ms" || m.TenantID != s.TenantID || peer == nil || peer.Profile != s.Profile || peer.Backend != s.Backend || m.Source != "backend_resume_attach_completion" {
			continue
		}
		if m.At.IsZero() || !m.At.Before(now) || now.Sub(m.At) > maxAge {
			continue
		}
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0 || m.Value > 86_400_000 {
			continue
		}
		values = append(values, m.Value)
		if m.At.After(newest) {
			newest = m.At
		}
	}
	if len(values) == 0 {
		return 0, nil, "resume_measurement_unavailable"
	}
	mean := 0.0
	for _, value := range values {
		mean += value / float64(len(values))
	}
	return mean, &newest, "historical_backend_resume_attach_mean"
}

func (c *Controller) jointKVCostLocked(s *Session, episode *WaitEpisode, evidence *ServerEstimateEvidence, now time.Time) {
	baseline := episode.SwitchCostMS
	maxAge, minSamples := c.cfg.KVObservationMaxAge, c.cfg.KVMinimumSamples
	if maxAge == 0 {
		maxAge = 2 * time.Second
	}
	if minSamples == 0 {
		minSamples = 4
	}
	resume, resumeAt, resumeSource := c.kvResumeExposureLocked(s, now)
	returns, returnKnown := policy.ConditionalKVReturns(evidence.waitSamples, evidence.WaitAgeMS, waitMinimumSupport)
	returnAfter := float64(v2UnknownWait) / float64(time.Millisecond)
	if returnKnown {
		returnAfter = 0
		for _, point := range returns {
			returnAfter += point.RemainingMS * point.Probability
		}
	} else {
		returns = nil
	}
	keep := policy.KVAction{GPUPolicy: "unchanged_unpinned", ReturnAfterMS: returnAfter}
	if o := evidence.CacheObservation; o != nil {
		keep.WorkerID, keep.WorkerEpoch = o.WorkerID, o.WorkerEpoch
	}
	if evidence.SelectedWorkerID != nil {
		keep.WorkerID = *evidence.SelectedWorkerID
	}
	if evidence.SelectedDPRank != nil {
		keep.DPRank = *evidence.SelectedDPRank
	}
	suspend := keep
	suspend.ReturnAfterMS += resume
	estimate := policy.EstimateKV(evidence.CacheObservation, c.cfg.KVCalibration, keep, suspend, s.Epoch, now, maxAge, minSamples, returns...)
	estimate.BaselineSwitchCostMS, estimate.TotalSwitchCostMS = baseline, float64(baseline)
	estimate.ResumeCriticalPathMS, estimate.ResumeSource, estimate.ResumeObservedAt = resume, resumeSource, resumeAt
	estimate.ReturnTimeSource = "conditional_kaplan_meier_history"
	if !returnKnown {
		estimate.ReturnTimeSource = "unknown_two_second_deadline_proxy"
	}
	if evidence.CacheObservation != nil && (evidence.SelectedWorkerID == nil || evidence.SelectedDPRank == nil) {
		estimate.Valid, estimate.DeltaMS, estimate.ZeroEffect, estimate.FallbackReason = false, 0, false, "selected_worker_unavailable"
	}
	// Missing duration evidence denotes an unidentified action effect. Keep the
	// structural zero effect available when an actual zero duration was measured.
	if resumeAt == nil && evidence.CacheObservation != nil && evidence.CacheObservation.DeviceOverlapTokens > 0 && estimate.FallbackReason == "" {
		estimate.Valid, estimate.ZeroEffect, estimate.FallbackReason = false, false, "resume_measurement_unavailable"
	}
	if estimate.Valid {
		estimate.TotalSwitchCostMS = max(1.0, float64(baseline)+estimate.DeltaMS)
		estimate.AppliedToDecision = returnKnown && evidence.WaitPrediction != nil && evidence.WaitPrediction.Known
		if estimate.AppliedToDecision {
			episode.SwitchCostMS = max(int64(1), int64(math.Ceil(estimate.TotalSwitchCostMS)))
		}
	}
	episode.KVCost = &estimate
}
