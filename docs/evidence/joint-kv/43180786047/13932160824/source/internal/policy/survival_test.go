package policy

import (
	"math"
	"reflect"
	"sort"
	"testing"
)

func survivalMixture() []SurvivalSample {
	return []SurvivalSample{
		{1000, true}, {1000, true}, {1000, true}, {1000, true},
		{1000, true}, {1000, true}, {1000, true}, {1000, true},
		{20000, true}, {20000, true},
	}
}

func TestSurvivalConditionalLongTail(t *testing.T) {
	for _, test := range []struct {
		elapsedMS, remainingMS int64
		atRisk                 int
	}{
		{0, 1000, 10},
		{1000, 19000, 2},
		{2000, 18000, 2},
		{19999, 1, 2},
	} {
		prediction := ConditionalRemaining(survivalMixture(), test.elapsedMS, 0.75, 2)
		if !prediction.Known || prediction.Reason != "OK" || prediction.RemainingMS != test.remainingMS || prediction.AtRisk != test.atRisk {
			t.Fatalf("elapsed=%d prediction=%+v", test.elapsedMS, prediction)
		}
		if prediction.Samples != 10 || prediction.Completed != 10 || prediction.Censored != 0 || prediction.SupportMS != 20000 {
			t.Fatalf("history metadata=%+v", prediction)
		}
	}
}

func TestSurvivalCompleteMatchesEmpiricalQuantiles(t *testing.T) {
	for n := 1; n <= 64; n++ {
		samples := make([]SurvivalSample, n)
		for i := range samples {
			samples[i] = SurvivalSample{DurationMS: int64((i*17)%23) * 1000, Completed: true}
		}
		for _, age := range []int64{0, 1000, 5000, 15000} {
			var residuals []int64
			for _, sample := range samples {
				if sample.DurationMS > age {
					residuals = append(residuals, sample.DurationMS-age)
				}
			}
			sort.Slice(residuals, func(i, j int) bool { return residuals[i] < residuals[j] })
			for rank := 1; rank <= len(residuals); rank++ {
				q := float64(rank) / float64(len(residuals))
				prediction := ConditionalRemaining(samples, age, q, 1)
				if !prediction.Known || prediction.RemainingMS != residuals[rank-1] {
					t.Fatalf("n=%d age=%d rank=%d q=%.17g prediction=%+v expected=%d", n, age, rank, q, prediction, residuals[rank-1])
				}
			}
		}
	}
}

func TestSurvivalRightCensoringAndTiedRiskSets(t *testing.T) {
	samples := []SurvivalSample{{3000, false}, {1000, false}, {2000, true}, {1000, true}}
	for _, test := range []struct {
		age, remaining int64
		q              float64
		known          bool
		atRisk         int
	}{
		{0, 1000, 0.25, true, 4},
		{0, 2000, 0.5, true, 4},
		{0, 2000, 0.625, true, 4},
		{0, 0, 0.65, false, 4},
		{0, 0, 1, false, 4},
		{1000, 1000, 0.5, true, 2},
		{1000, 0, 0.75, false, 2},
		{2000, 0, 0.1, false, 1},
	} {
		prediction := ConditionalRemaining(samples, test.age, test.q, 1)
		if prediction.Known != test.known || prediction.RemainingMS != test.remaining || prediction.AtRisk != test.atRisk {
			t.Fatalf("age=%d q=%v prediction=%+v", test.age, test.q, prediction)
		}
		if !test.known && prediction.Reason != "UNKNOWN_TAIL" {
			t.Fatalf("tail reason=%+v", prediction)
		}
	}
}

func TestSurvivalInputAndSupportValidation(t *testing.T) {
	for _, test := range []struct {
		name       string
		samples    []SurvivalSample
		elapsedMS  int64
		q          float64
		minAtRisk  int
		wantReason string
	}{
		{"no_history", nil, 0, 0.75, 1, "NO_HISTORY"},
		{"negative_sample", []SurvivalSample{{-1, true}}, 0, 0.75, 1, "INVALID_SAMPLE"},
		{"negative_age", survivalMixture(), -1, 0.75, 1, "INVALID_ELAPSED"},
		{"negative_q", survivalMixture(), 0, -0.1, 1, "INVALID_QUANTILE"},
		{"excessive_q", survivalMixture(), 0, 1.1, 1, "INVALID_QUANTILE"},
		{"nan_q", survivalMixture(), 0, math.NaN(), 1, "INVALID_QUANTILE"},
		{"infinite_q", survivalMixture(), 0, math.Inf(1), 1, "INVALID_QUANTILE"},
		{"negative_infinite_q", survivalMixture(), 0, math.Inf(-1), 1, "INVALID_QUANTILE"},
		{"zero_minimum", survivalMixture(), 0, 0.75, 0, "INVALID_MIN_AT_RISK"},
		{"negative_minimum", survivalMixture(), 0, 0.75, -1, "INVALID_MIN_AT_RISK"},
		{"low_support", survivalMixture(), 2000, 0.75, 3, "LOW_SUPPORT"},
		{"at_final_event", survivalMixture(), 20000, 0.75, 1, "LOW_SUPPORT"},
		{"beyond_support", survivalMixture(), 20001, 0.75, 1, "LOW_SUPPORT"},
		{"all_censored", []SurvivalSample{{1000, false}, {2000, false}}, 0, 0.75, 1, "UNKNOWN_TAIL"},
		{"zero_duration", []SurvivalSample{{0, true}, {0, false}}, 0, 0.75, 1, "LOW_SUPPORT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			prediction := ConditionalRemaining(test.samples, test.elapsedMS, test.q, test.minAtRisk)
			if prediction.Known || prediction.Reason != test.wantReason || prediction.RemainingMS != 0 {
				t.Fatalf("prediction=%+v expected reason=%s", prediction, test.wantReason)
			}
		})
	}
	zero := ConditionalRemaining(survivalMixture(), 2000, 0, 1)
	if !zero.Known || zero.RemainingMS != 0 {
		t.Fatalf("zero quantile=%+v", zero)
	}
	maxDuration := ConditionalRemaining([]SurvivalSample{{math.MaxInt64, true}}, math.MaxInt64-1, 1, 1)
	if !maxDuration.Known || maxDuration.RemainingMS != 1 {
		t.Fatalf("large duration=%+v", maxDuration)
	}
}

func TestConditionalKeepCostFiniteIntegral(t *testing.T) {
	for _, test := range []struct {
		age, keep, switchCost int64
		want                  float64
	}{
		{0, 0, 500, 500},
		{0, 500, 500, 1000},
		{0, 1000, 500, 1100},
		{0, 2000, 500, 1300},
		{0, 20000, 500, 4800},
		{0, 30000, 500, 4800},
		{2000, 5000, 500, 5500},
		{2000, 18000, 500, 18000},
		{2000, 20000, 500, 18000},
	} {
		cost, known := ConditionalKeepCost(survivalMixture(), test.age, test.keep, test.switchCost, 2)
		if !known || math.Abs(cost-test.want) > 1e-9 {
			t.Fatalf("age=%d keep=%d switch=%d cost=%v known=%v expected=%v", test.age, test.keep, test.switchCost, cost, known, test.want)
		}
	}
}

func TestConditionalKeepCostCensoringSupport(t *testing.T) {
	tied := []SurvivalSample{{1000, true}, {1000, false}, {2000, true}, {3000, false}}
	for _, test := range []struct {
		age, keep int64
		want      float64
		known     bool
	}{
		{0, 2500, 2125, true},
		{0, 3000, 2312.5, true},
		{0, 3001, 0, false},
		{1000, 1500, 1500, true},
		{2000, 1000, 1500, true},
		{2000, 1001, 0, false},
	} {
		cost, known := ConditionalKeepCost(tied, test.age, test.keep, 500, 1)
		if known != test.known || math.Abs(cost-test.want) > 1e-9 {
			t.Fatalf("age=%d keep=%d cost=%v known=%v expected=%v known=%v", test.age, test.keep, cost, known, test.want, test.known)
		}
	}
	allCensored := []SurvivalSample{{1000, false}, {2000, false}}
	cost, known := ConditionalKeepCost(allCensored, 0, 1500, 500, 1)
	if !known || cost != 2000 {
		t.Fatalf("observed all-censored window cost=%v known=%v", cost, known)
	}
	if _, known := ConditionalKeepCost(allCensored, 0, 2001, 500, 1); known {
		t.Fatal("unobserved all-censored tail has a known cost")
	}
}

func TestConditionalKeepCostValidationAndImmediateRelease(t *testing.T) {
	for _, test := range []struct {
		samples    []SurvivalSample
		age, keep  int64
		switchCost int64
		minimum    int
	}{
		{nil, 0, 1, 500, 1},
		{survivalMixture(), -1, 1, 500, 1},
		{survivalMixture(), 0, -1, 500, 1},
		{survivalMixture(), 0, 1, -1, 1},
		{survivalMixture(), 0, 1, 500, 0},
		{survivalMixture(), 2000, 1, 500, 3},
		{[]SurvivalSample{{-1, false}, {1000, true}}, 0, 1, 500, 1},
		{[]SurvivalSample{{math.MaxInt64, true}}, math.MaxInt64 - 1, 2, 500, 1},
	} {
		if _, known := ConditionalKeepCost(test.samples, test.age, test.keep, test.switchCost, test.minimum); known {
			t.Fatalf("invalid or unsupported cost is known: %+v", test)
		}
	}
	cost, known := ConditionalKeepCost(nil, 0, 0, 500, 0)
	if !known || cost != 500 {
		t.Fatalf("immediate release cost=%v known=%v", cost, known)
	}
	cost, known = ConditionalKeepCost([]SurvivalSample{{math.MaxInt64, true}}, 0, math.MaxInt64, math.MaxInt64, 1)
	if !known || math.IsNaN(cost) || math.IsInf(cost, 0) || cost != float64(math.MaxInt64) {
		t.Fatalf("large finite cost=%v known=%v", cost, known)
	}
}

func TestSurvivalInputRemainsUnchanged(t *testing.T) {
	samples := []SurvivalSample{{3000, false}, {1000, true}, {2000, true}, {1000, false}}
	before := append([]SurvivalSample(nil), samples...)
	ConditionalRemaining(samples, 0, 0.5, 1)
	ConditionalKeepCost(samples, 0, 2000, 500, 1)
	if !reflect.DeepEqual(samples, before) {
		t.Fatalf("samples=%+v expected=%+v", samples, before)
	}
}

func BenchmarkSurvivalConditional64(b *testing.B) {
	samples := make([]SurvivalSample, 64)
	for i := range samples {
		samples[i] = SurvivalSample{DurationMS: int64((i*17)%64+1) * 1000, Completed: i%5 != 0}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ConditionalRemaining(samples, 2000, 0.75, 8)
	}
}
