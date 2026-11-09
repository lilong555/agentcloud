package control

import (
	"context"
	"github.com/lilong555/agentcloud/internal/observability"
	"time"
)

type strandedInterval struct {
	At          time.Time
	MemoryBytes int64
	Idle        bool
	ObservedAt  time.Time
	Accumulated float64
	LastEmitted time.Time
}

// Integrate actual sampled assignment memory over the union of model and think
// windows, subtracting controller-confirmed tool intervals at event boundaries.
func (c *Controller) updateStrandingLocked(s *Session, now time.Time) {
	k := key(s.TenantID, s.SessionID)
	prior := c.stranded[k]
	if prior == nil {
		prior = &strandedInterval{At: now, ObservedAt: now, MemoryBytes: s.Actor.AssignedMemoryBytes, LastEmitted: now}
	}
	if now.Before(prior.At) {
		return
	}
	end := now
	// Limit a missing allocation sample to one sample period. Its uncovered tail
	// stays unobserved instead of inheriting an indefinitely stale allocation.
	if end.After(prior.ObservedAt.Add(100 * time.Millisecond)) {
		end = prior.ObservedAt.Add(100 * time.Millisecond)
	}
	if prior.Idle && prior.MemoryBytes > 0 && end.After(prior.At) {
		prior.Accumulated += float64(prior.MemoryBytes) / 1e9 * end.Sub(prior.At).Seconds()
	}
	idle := false
	if s.ActiveTools == 0 {
		for _, r := range c.state.Requests {
			if r.TenantID == s.TenantID && r.SessionID == s.SessionID && (r.Status == "PENDING" || r.Status == "DISPATCHING") {
				idle = true
				break
			}
		}
		for _, window := range s.IdleWindows {
			if window.FinishedAt == nil && window.Epoch == s.Epoch && now.Before(window.ExpiresAt) {
				idle = true
				break
			}
		}
	}
	if prior.Accumulated > 0 && (!idle || now.Sub(prior.LastEmitted) >= time.Second || c.closing) {
		ctx := c.operationContext(c.ctx, s.TenantID, s.SessionID, "worker_idle")
		c.observeLocked(ctx, "stranded_worker_gb_s", prior.Accumulated, "substrate_assignment_memory_100ms_left_step_x_idle_union_minus_tools")
		prior.Accumulated = 0
		prior.LastEmitted = now
	}
	prior.At = now
	prior.Idle = idle
	c.stranded[k] = prior
}
func (c *Controller) strandingLoop() {
	defer c.workers.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			backend, ok := c.cfg.Backend.(*SubstrateBackend)
			if !ok {
				continue
			}
			c.lock()
			workers := map[string]bool{}
			for _, s := range c.state.Sessions {
				if s.Actor.AssignedWorker != "" {
					workers[s.Actor.AssignedWorker] = true
				}
			}
			c.mu.Unlock()
			values := map[string]int64{}
			observed := map[string]bool{}
			for worker := range workers {
				ctx, cancel := context.WithTimeout(observability.WithRuntime(c.ctx, c.telemetry), 250*time.Millisecond)
				samples, err := backend.cfg.Client.WorkerMemoryAssignments(ctx, worker)
				cancel()
				if err == nil {
					observed[worker] = true
					for uid, memory := range samples {
						values[uid] = memory
					}
				}
			}
			now := time.Now().UTC()
			c.lock()
			for _, s := range c.state.Sessions {
				c.updateStrandingLocked(s, now)
				prior := c.stranded[key(s.TenantID, s.SessionID)]
				if observed[s.Actor.AssignedWorker] {
					prior.MemoryBytes = values[s.Actor.UID]
					prior.ObservedAt = now
				} else if s.Actor.AssignedWorker != "" {
					observability.Unavailable(observability.WithRuntime(c.ctx, c.telemetry), "stranded_worker_gb_s")
				}
			}
			c.mu.Unlock()
		}
	}
}
func (c *Controller) observeAllocationMissing(s *Session) {
	if s.Backend == "substrate" && s.Actor.AssignedMemoryBytes <= 0 && (s.State == "RUNNING" || s.State == "CREATING" || s.State == "RESUMING") {
		observability.Unavailable(c.operationContext(c.ctx, s.TenantID, s.SessionID, "allocation"), "stranded_worker_gb_s")
	}
}

type IdleWindow struct {
	OperationID string     `json:"operation_id"`
	Epoch       uint64     `json:"epoch"`
	StartedAt   time.Time  `json:"started_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}
type IdleWindowRequest struct {
	OperationID   string `json:"operation_id"`
	ExpectedEpoch uint64 `json:"expected_epoch"`
	Phase         string `json:"phase"`
	TTLMS         int64  `json:"ttl_ms,omitempty"`
}

func (c *Controller) ObserveIdle(ctx context.Context, tenant, id string, input IdleWindowRequest) (*IdleWindow, error) {
	if !validID(input.OperationID) || input.ExpectedEpoch == 0 || (input.Phase != "START" && input.Phase != "END") {
		return nil, problem(400, "invalid_idle_window", "operation_id, expected_epoch and START/END phase are required")
	}
	if input.Phase == "START" && (input.TTLMS < 1 || input.TTLMS > 600000) {
		return nil, problem(400, "invalid_idle_ttl", "idle ttl_ms must be 1..600000")
	}
	ctx = c.operationContext(ctx, tenant, id, input.OperationID)
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	if s.IdleWindows == nil {
		s.IdleWindows = map[string]*IdleWindow{}
	}
	window := s.IdleWindows[input.OperationID]
	now := time.Now().UTC()
	if input.Phase == "START" {
		if window != nil {
			if window.Epoch != input.ExpectedEpoch || window.ExpiresAt.Sub(window.StartedAt) != time.Duration(input.TTLMS)*time.Millisecond {
				return nil, problem(409, "payload_conflict", "idle operation has another epoch or ttl")
			}
			return copyValue(window), nil
		}
		if s.Epoch != input.ExpectedEpoch {
			return nil, problem(409, "stale_epoch", "execution epoch differs")
		}
		if len(s.IdleWindows) >= 4096 {
			return nil, problem(429, "idle_window_quota", "idle window capacity reached")
		}
		window = &IdleWindow{OperationID: input.OperationID, Epoch: s.Epoch, StartedAt: now, ExpiresAt: now.Add(time.Duration(input.TTLMS) * time.Millisecond)}
		s.IdleWindows[input.OperationID] = window
		s.IdleSince = now
	} else {
		if window == nil {
			return nil, problem(404, "idle_window_not_found", "idle window not found")
		}
		if window.Epoch != input.ExpectedEpoch {
			return nil, problem(409, "stale_epoch", "idle completion requires the window's originating epoch")
		}
		if window.FinishedAt != nil {
			return copyValue(window), nil
		}
		window.FinishedAt = &now
		if now.Before(window.ExpiresAt) {
			c.observeLocked(ctx, "idle_window_ms", float64(now.Sub(window.StartedAt))/float64(time.Millisecond), "authenticated_idle_start_to_end_observation")
		}
		if s.IdleSince.Equal(window.StartedAt) {
			s.IdleSince = time.Time{}
		}
	}
	c.event(s, "idle."+input.Phase, input.OperationID)
	if err := c.persistContext(ctx); err != nil {
		return nil, err
	}
	return copyValue(window), nil
}
