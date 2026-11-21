package sim

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestParkedTicksMatchPeriodicPoliciesAndGlobalPhase(t *testing.T) {
	for seed := int64(0); seed < 12; seed++ {
		for _, policy := range []string{"ttl2", "v2", "break-even-gdsf"} {
			rng := rand.New(rand.NewSource(seed))
			idle := 70000.0
			if seed%3 == 0 {
				idle = 700000
			}
			trace := fixture(node("m0", "model_wait", 3300), node("gap", "active", 21, "m0"), node("idle", "user_idle", idle, "gap"), node("m1", "model_wait", 1700, "idle"))
			trace.Sessions[0].Tenant = "tenant"
			for _, id := range []string{"b", "c"} {
				arrival := float64(rng.Intn(2000)) + 0.25
				if id == "c" {
					arrival = 100000
				}
				trace.Sessions = append(trace.Sessions, Session{ID: id, Family: id, Tenant: "tenant", Split: "holdout", Arrival: arrival, Events: []Event{
					node("m0", "model_wait", float64(2000+rng.Intn(3000))), node("a", "active", float64(1+rng.Intn(500)), "m0"), node("m1", "model_wait", float64(rng.Intn(3000)), "a")}})
			}
			cfg := nativeConfig()
			cfg.Slots = 2
			cfg.Environment = testEnvironment()
			cfg.HarnessEnabled = seed%2 == 0
			p := func() OnlinePolicy {
				if policy == "v2" {
					return NewV2(cfg.StableMS, cfg.MinimumResidenceMS)
				}
				return Baseline{Name: policy, Eviction: "gdsf"}
			}
			old, err := Run(trace, costs(), cfg, p(), nil)
			if err != nil {
				t.Fatal(err)
			}
			cfg.ParkInactiveTicks = true
			parked, err := Run(trace, costs(), cfg, p(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if parked.ProcessedEvents >= old.ProcessedEvents {
				t.Fatal("idle ticks were not parked")
			}
			old.ProcessedEvents, parked.ProcessedEvents = 0, 0
			old.Config.ParkInactiveTicks = true
			if !reflect.DeepEqual(old, parked) {
				t.Fatalf("seed=%d policy=%s periodic=%+v parked=%+v", seed, policy, old.Sessions, parked.Sessions)
			}
			cfg.DiscardEvaluations = true
			compact, err := Run(trace, costs(), cfg, p(), nil)
			if err != nil {
				t.Fatal(err)
			}
			compact.Config.DiscardEvaluations = false
			compact.ProcessedEvents = 0
			compact.Evaluations = parked.Evaluations
			if !reflect.DeepEqual(compact, parked) {
				t.Fatal("evaluation storage changed execution")
			}
		}
	}
}
