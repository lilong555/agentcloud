package signals

import (
	"encoding/json"
	"math"
	"testing"
)

func TestCopyCacheObservationRetainsFiniteFallbackAndIndependentValues(t *testing.T) {
	exposure := math.NaN()
	events := uint64(1)
	in := &CacheObservation{Valid: true, ExposureMS: &exposure, LossEvents: &events, LossBuckets: []PrefixLossBin{{LostTokens: 16, Count: 1}}}
	out := CopyCacheObservation(in)
	if out.Valid || out.InvalidReason != "nonfinite_exposure" || *out.ExposureMS != 0 {
		t.Fatalf("fallback: %+v", out)
	}
	if _, err := json.Marshal(out); err != nil {
		t.Fatal(err)
	}
	*out.LossEvents = 2
	out.LossBuckets[0].LostTokens = 32
	if *in.LossEvents != 1 || in.LossBuckets[0].LostTokens != 16 || !math.IsNaN(*in.ExposureMS) {
		t.Fatal("copy changed the observer state")
	}
}
