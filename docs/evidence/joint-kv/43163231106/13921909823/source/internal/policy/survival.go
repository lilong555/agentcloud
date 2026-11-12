package policy

import (
	"math"
	"sort"
)

// SurvivalSample records a total observed duration in milliseconds. Completed
// marks a terminal event; otherwise DurationMS is a right-censoring time.
type SurvivalSample struct {
	DurationMS int64
	Completed  bool
}

// ResidualPrediction describes a Kaplan-Meier conditional waiting-time quantile.
// SupportMS is the largest total observed duration. AtRisk counts observations
// extending strictly beyond the current age. Known identifies a quantile reached
// within observed support, and Reason records OK or the unavailable estimate's
// cause. Samples, Completed and Censored describe the entire input history.
type ResidualPrediction struct {
	RemainingMS int64  `json:"remaining_ms"`
	Known       bool   `json:"known"`
	Reason      string `json:"reason"`
	Samples     int    `json:"samples"`
	Completed   int    `json:"completed"`
	Censored    int    `json:"censored"`
	AtRisk      int    `json:"at_risk"`
	SupportMS   int64  `json:"support_ms"`
}

// ConditionalRemaining estimates inf{r >= 0: S(e+r)/S(e) <= 1-q}, where
// e=elapsedMS and S(t)=P(T>t). Durations and elapsedMS are nonnegative; q is finite
// and in [0,1]; minAtRisk is positive. The caller bounds the history size.
// Tied events use the complete risk set before tied censorings leave it.
// An unidentified tail returns Known=false and Reason=UNKNOWN_TAIL. The input
// slice retains its original order and contents.
func ConditionalRemaining(samples []SurvivalSample, elapsedMS int64, q float64, minAtRisk int) ResidualPrediction {
	ordered, result := conditionalHistory(samples, elapsedMS, minAtRisk)
	if math.IsNaN(q) || math.IsInf(q, 0) || q < 0 || q > 1 {
		result.Reason = "INVALID_QUANTILE"
		return result
	}
	if result.Reason != "OK" {
		return result
	}
	if q == 0 {
		result.Known = true
		return result
	}
	remainingRisk := result.AtRisk
	survival := 1.0
	target := 1 - q
	for i := 0; i < len(ordered); {
		timeMS, events, next := survivalGroup(ordered, i)
		if events > 0 {
			survival *= float64(remainingRisk-events) / float64(remainingRisk)
			// Relative rounding tolerance preserves empirical quantiles at jumps.
			// A zero target requires an actual zero-survival event.
			if survival <= target || target > 0 && math.Abs(survival-target) <= 1e-14*math.Max(survival, target) {
				result.RemainingMS, result.Known = timeMS-elapsedMS, true
				return result
			}
		}
		remainingRisk -= next - i
		i = next
	}
	result.Reason = "UNKNOWN_TAIL"
	return result
}

// ConditionalKeepCost estimates integral_0^tau S(e+u)/S(e) du plus
// switchCostMS*S(e+tau)/S(e), using milliseconds for all time and cost inputs.
// A zero keepForMS immediately returns switchCostMS. Positive windows require
// minAtRisk support and a horizon inside observed support, or a terminal event
// that exhausts the conditional survival probability. Invalid inputs and
// unidentified tails return known=false. The caller bounds the history size;
// samples retains its original order and contents.
func ConditionalKeepCost(samples []SurvivalSample, elapsedMS, keepForMS, switchCostMS int64, minAtRisk int) (costMS float64, known bool) {
	if elapsedMS < 0 || keepForMS < 0 || switchCostMS < 0 {
		return 0, false
	}
	if keepForMS == 0 {
		return float64(switchCostMS), true
	}
	if elapsedMS > math.MaxInt64-keepForMS {
		return 0, false
	}
	ordered, result := conditionalHistory(samples, elapsedMS, minAtRisk)
	if result.Reason != "OK" {
		return 0, false
	}
	horizonMS := elapsedMS + keepForMS
	remainingRisk := result.AtRisk
	survival := 1.0
	previousMS := elapsedMS
	for i := 0; i < len(ordered); {
		timeMS, events, next := survivalGroup(ordered, i)
		if timeMS > horizonMS {
			break
		}
		costMS += float64(timeMS-previousMS) * survival
		previousMS = timeMS
		if events > 0 {
			survival *= float64(remainingRisk-events) / float64(remainingRisk)
		}
		remainingRisk -= next - i
		i = next
	}
	if horizonMS > result.SupportMS && survival > 0 {
		return 0, false
	}
	costMS += float64(horizonMS-previousMS)*survival + float64(switchCostMS)*survival
	return costMS, true
}

func conditionalHistory(samples []SurvivalSample, elapsedMS int64, minAtRisk int) ([]SurvivalSample, ResidualPrediction) {
	result := ResidualPrediction{Samples: len(samples), Reason: "OK"}
	invalidSample := false
	for _, sample := range samples {
		if sample.Completed {
			result.Completed++
		} else {
			result.Censored++
		}
		if sample.DurationMS < 0 {
			invalidSample = true
		}
		if sample.DurationMS > result.SupportMS {
			result.SupportMS = sample.DurationMS
		}
		if sample.DurationMS > elapsedMS {
			result.AtRisk++
		}
	}
	switch {
	case elapsedMS < 0:
		result.Reason = "INVALID_ELAPSED"
	case minAtRisk < 1:
		result.Reason = "INVALID_MIN_AT_RISK"
	case invalidSample:
		result.Reason = "INVALID_SAMPLE"
	case len(samples) == 0:
		result.Reason = "NO_HISTORY"
	case result.AtRisk < minAtRisk:
		result.Reason = "LOW_SUPPORT"
	}
	if result.Reason != "OK" {
		return nil, result
	}
	ordered := make([]SurvivalSample, 0, result.AtRisk)
	for _, sample := range samples {
		if sample.DurationMS > elapsedMS {
			ordered = append(ordered, sample)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].DurationMS < ordered[j].DurationMS })
	return ordered, result
}

func survivalGroup(samples []SurvivalSample, start int) (durationMS int64, completed, next int) {
	durationMS = samples[start].DurationMS
	next = start
	for next < len(samples) && samples[next].DurationMS == durationMS {
		if samples[next].Completed {
			completed++
		}
		next++
	}
	return durationMS, completed, next
}
