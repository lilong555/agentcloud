package sim

import (
	"fmt"
	"math"
)

type OracleResult struct {
	Kind                   string   `json:"kind"`
	Assumptions            []string `json:"assumptions"`
	Objective              string   `json:"objective"`
	RelaxedLowerBound      float64  `json:"relaxed_lower_bound"`
	BestRestrictedFeasible *float64 `json:"best_restricted_feasible"`
	Evaluated              int      `json:"evaluated"`
	Feasible               int      `json:"feasible"`
	Decisions              int      `json:"binary_decisions"`
	BestMask               uint64   `json:"best_mask"`
}

// The lower bound removes slot/disk limits and makes C/R and startup free.
// Each session's longest active dependency path bounds residency; earliest
// DAG completion is a latency lower bound for every feasible online schedule.
func RelaxedBound(t Trace, cfg Config, latencyWeight float64) float64 {
	objective := 0.0
	for _, s := range t.Sessions {
		ends := map[string]float64{}
		finish := 0.0
		for _, n := range s.Events {
			start := 0.0
			for _, p := range n.Parents {
				start = math.Max(start, ends[p])
			}
			end := start + *n.Duration
			ends[n.ID] = end
			finish = math.Max(finish, end)
		}
		// A universally valid residency bound is the longest dependency chain of
		// active work. Independent active intervals may overlap or be delayed.
		activePath := map[string]float64{}
		activeLB := 0.0
		for _, n := range s.Events {
			a := 0.0
			for _, p := range n.Parents {
				a = math.Max(a, activePath[p])
			}
			if n.Kind == "active" {
				a += *n.Duration
			}
			activePath[n.ID] = a
			activeLB = math.Max(activeLB, a)
		}
		objective += activeLB*float64(cfg.ReservationBytes)/1e12 + latencyWeight*finish/1000
	}
	return objective
}

type oraclePolicy struct{ bits map[string]bool }

func (p oraclePolicy) Decide(o Observation) Action {
	if !p.bits[o.SessionID+"/"+o.EventID] {
		return Action{Reason: "offline_binary_keep"}
	}
	if o.Now < o.Earliest {
		return Action{RevisitAt: o.Earliest, Reason: "offline_binary_release"}
	}
	return Action{Suspend: true, Reason: "offline_binary_release"}
}

func Objective(r Result, latencyWeight float64) float64 {
	v := r.ReservedGBSeconds
	for _, s := range r.Sessions {
		v += latencyWeight * (s.Finish - s.Arrival) / 1000
	}
	return v
}

// EnumerateOracle is exact within the announced binary action class; its best
// feasible value is an upper bound. RelaxedBound is a separate lower bound.
func EnumerateOracle(t Trace, costs Costs, cfg Config, maxBits int, weight float64) (OracleResult, error) {
	if err := t.Validate(); err != nil {
		return OracleResult{}, err
	}
	if maxBits < 0 || maxBits > 16 || weight < 0 || !valid(weight) {
		return OracleResult{}, fmt.Errorf("invalid bounded oracle configuration")
	}
	var keys []string
	for _, s := range t.Sessions {
		for _, n := range s.Events {
			if n.Kind != "active" && n.Safe != nil && *n.Safe {
				keys = append(keys, s.ID+"/"+n.ID)
			}
		}
	}
	r := OracleResult{Kind: "relaxed_lower_bound_and_restricted_binary_enumeration", Decisions: len(keys), RelaxedLowerBound: RelaxedBound(t, cfg, weight),
		Objective: fmt.Sprintf("reserved_GB_seconds + %.6g * sum_session_E2E_seconds", weight),
		Assumptions: []string{"lower bound removes all queues, startup/C/R costs and disk limits; latency is DAG critical path and residency is active-work longest path",
			"enumeration optimizes KEEP versus earliest-safe SUSPEND for each potential safe episode; continuous-time release and admission order are not optimized",
			"enumerated plans use the same causal engine, FIFO queue, physical cost table and disk constraint as baselines",
			"future knowledge is confined to this offline package entry point; result provides a loose two-sided bracket, not a general optimal scheduler"}}
	if len(keys) > maxBits {
		r.Assumptions = append(r.Assumptions, "enumeration skipped: configured binary decision budget exceeded")
		return r, nil
	}
	for mask := uint64(0); mask < uint64(1)<<len(keys); mask++ {
		p := oraclePolicy{bits: map[string]bool{}}
		for i, k := range keys {
			p.bits[k] = mask&(uint64(1)<<i) != 0
		}
		result, err := Run(t, costs, cfg, p, nil)
		r.Evaluated++
		if err != nil {
			continue
		}
		r.Feasible++
		v := Objective(result, weight)
		if v+1e-8 < r.RelaxedLowerBound {
			return r, fmt.Errorf("oracle lower bound violated")
		}
		if r.BestRestrictedFeasible == nil || v < *r.BestRestrictedFeasible {
			r.BestRestrictedFeasible = &v
			r.BestMask = mask
		}
	}
	return r, nil
}
