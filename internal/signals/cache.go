package signals

import (
	"math"
	"time"
)

// CacheObservation is a trusted, prefix-specific KVIndexer observation. The
// survival cohort counts each prefix's first loss once, and exposure stops at
// that loss or censoring. WorkerEpoch is the producer incarnation, independently
// of ExecutionEpoch, which fences the sandbox binding.
type CacheObservation struct {
	SchemaVersion       int             `json:"schema_version"`
	ObservedAt          time.Time       `json:"observed_at"`
	SourceKind          string          `json:"source_kind"`
	BackendKind         string          `json:"backend_kind"`
	SourceVersion       string          `json:"source_version"`
	WorkerID            uint64          `json:"worker_id"`
	WorkerEpoch         string          `json:"worker_epoch"`
	ExecutionEpoch      uint64          `json:"execution_epoch"`
	DPRank              uint32          `json:"dp_rank"`
	PrefixDigest        string          `json:"prefix_digest"`
	HistoryTokens       int64           `json:"history_tokens"`
	DeviceOverlapTokens int64           `json:"device_overlap_tokens"`
	GapPending          bool            `json:"gap_pending"`
	EventWatermark      uint64          `json:"event_watermark"`
	Valid               bool            `json:"valid"`
	InvalidReason       string          `json:"invalid_reason,omitempty"`
	AgeMS               float64         `json:"age_ms"`
	LossEvents          *uint64         `json:"loss_events,omitempty"`
	ExposureMS          *float64        `json:"exposure_ms,omitempty"`
	SampleCount         *uint64         `json:"sample_count,omitempty"`
	StatsObservedAt     *time.Time      `json:"stats_observed_at,omitempty"`
	LossBuckets         []PrefixLossBin `json:"loss_buckets,omitempty"`
	ModelID             string          `json:"model_id,omitempty"`
	CalibrationKey      string          `json:"calibration_key,omitempty"`
}

type PrefixLossBin struct {
	LostTokens int64  `json:"lost_tokens"`
	Count      uint64 `json:"count"`
}

// CopyCacheObservation keeps trusted observer diagnostics JSON-serializable
// even when an in-process observer supplies a non-finite numeric value.
func CopyCacheObservation(in *CacheObservation) *CacheObservation {
	if in == nil {
		return nil
	}
	out := *in
	out.LossBuckets = append([]PrefixLossBin(nil), in.LossBuckets...)
	if !finiteCacheValue(out.AgeMS) {
		out.AgeMS, out.Valid, out.InvalidReason = 0, false, "nonfinite_observation_age"
	}
	if in.ExposureMS != nil {
		exposure := *in.ExposureMS
		if !finiteCacheValue(exposure) {
			exposure, out.Valid, out.InvalidReason = 0, false, "nonfinite_exposure"
		}
		out.ExposureMS = &exposure
	}
	if in.LossEvents != nil {
		v := *in.LossEvents
		out.LossEvents = &v
	}
	if in.SampleCount != nil {
		v := *in.SampleCount
		out.SampleCount = &v
	}
	if in.StatsObservedAt != nil {
		v := *in.StatsObservedAt
		out.StatsObservedAt = &v
	}
	return &out
}

func finiteCacheValue(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
