package control

import "time"

// QueuePressure counts current-epoch durable demands; ATTEMPTING is separate.
type QueuePressure struct {
	Pending         int     `json:"pending"`
	WaitingCapacity int     `json:"waiting_capacity"`
	Attempting      int     `json:"attempting"`
	OldestWaitMS    float64 `json:"oldest_wait_ms"`
}

type PressureSnapshot struct {
	ObservedAt   time.Time                `json:"observed_at"`
	Initial      QueuePressure            `json:"initial_admission"`
	Resume       QueuePressure            `json:"resume_admission"`
	RetainedTags int                      `json:"retained_tags"`
	PendingTags  int                      `json:"pending_tags"`
	GCStates     map[string]int           `json:"gc_states"`
	PhysicalDisk *PhysicalDiskObservation `json:"physical_disk"`
}

// PhysicalDiskObservation is an external filesystem/device observation. The
// control-state aggregation leaves this slot nil; Tag inventory supplies no bytes.
type PhysicalDiskObservation struct {
	ObservedAt    time.Time `json:"observed_at"`
	Source        string    `json:"source"`
	Scope         string    `json:"scope"`
	UsedBytes     *int64    `json:"used_bytes"`
	FreeBytes     *int64    `json:"free_bytes"`
	MissingReason string    `json:"missing_reason,omitempty"`
}

// PressureSnapshot observes committed state without changing scheduler behavior.
func (c *Controller) PressureSnapshot(now time.Time) PressureSnapshot {
	c.lock()
	defer c.mu.Unlock()
	return pressureSnapshot(c.state.Sessions, now)
}

func pressureSnapshot(sessions map[string]*Session, now time.Time) PressureSnapshot {
	r := PressureSnapshot{ObservedAt: now, GCStates: map[string]int{}}
	tags := map[string]bool{}
	for _, s := range sessions {
		if s == nil {
			continue
		}
		for _, d := range s.Admissions {
			if d == nil || d.Epoch != s.Epoch || d.RequestedAt.IsZero() || d.RequestedAt.After(now) {
				continue
			}
			var q *QueuePressure
			switch d.Kind {
			case "initial_admission":
				q = &r.Initial
			case "resume_admission":
				q = &r.Resume
			default:
				continue
			}
			switch d.Status {
			case "PENDING":
				q.Pending++
			case "WAITING_CAPACITY":
				q.WaitingCapacity++
			case "ATTEMPTING":
				q.Attempting++
				continue
			default:
				continue
			}
			q.OldestWaitMS = max(q.OldestWaitMS, float64(now.Sub(d.RequestedAt))/float64(time.Millisecond))
		}
		if s.SnapshotGC == nil {
			continue
		}
		for _, tag := range s.SnapshotGC.Tags {
			if tag != nil && !tag.CreatedAt.After(now) {
				state := tag.DeleteState
				if state == "" {
					state = "RETAINED"
				}
				r.GCStates[state]++
			}
			if tag == nil || !tag.DeletedAt.IsZero() || tag.DeleteState == "DELETED" || tag.CreatedAt.After(now) {
				continue
			}
			if tag.TagUID == "" {
				r.PendingTags++
				continue
			}
			id := tag.TagUID
			tags[id] = true
		}
	}
	r.RetainedTags = len(tags)
	return r
}
