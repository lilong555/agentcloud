package substrate

import (
	"context"
	"fmt"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
)

// AssignedMemory reads the actual ActorAssignment, including runtime overhead.
// The template limit and process RSS have different meanings from this allocation.
func (c *Client) AssignedMemory(ctx context.Context, name, uid string) (int64, error) {
	actor, err := c.control.GetActor(ctx, &ateapi.GetActorRequest{Actor: c.ref(name)})
	if err != nil {
		return 0, err
	}
	if actor.GetMetadata().GetUid() != uid {
		return 0, fmt.Errorf("Actor UID differs during allocation observation")
	}
	worker := actor.GetStatus().GetWorkerAssignment().GetWorker()
	if worker == nil {
		return 0, fmt.Errorf("Actor has no Worker assignment")
	}
	options := &ateapi.ListOptions{PageSize: 100}
	for {
		page, err := c.control.ListWorkerActorAssignments(ctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: worker, Options: options})
		if err != nil {
			return 0, err
		}
		for _, assignment := range page.GetActorAssignments() {
			if assignment.GetActorUid() == uid && assignment.GetActor().GetAtespace() == c.atespace && assignment.GetActor().GetName() == name {
				bytes := assignment.GetResources().GetMemoryBytes()
				if bytes <= 0 {
					return 0, fmt.Errorf("assigned memory is unavailable")
				}
				return bytes, nil
			}
		}
		if page.GetNextPageToken() == "" {
			break
		}
		options.PageToken = page.GetNextPageToken()
	}
	return 0, fmt.Errorf("Actor assignment was released during observation")
}

func (c *Client) WorkerMemoryAssignments(ctx context.Context, worker string) (map[string]int64, error) {
	result := map[string]int64{}
	options := &ateapi.ListOptions{PageSize: 100}
	for {
		page, err := c.control.ListWorkerActorAssignments(ctx, &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: worker}, Options: options})
		if err != nil {
			return nil, err
		}
		for _, assignment := range page.GetActorAssignments() {
			if assignment.GetActor().GetAtespace() == c.atespace {
				result[assignment.GetActorUid()] = assignment.GetResources().GetMemoryBytes()
			}
		}
		if page.GetNextPageToken() == "" {
			return result, nil
		}
		options.PageToken = page.GetNextPageToken()
	}
}

type WorkerCapacity struct {
	Worker               string    `json:"worker"`
	CapacityActors       int64     `json:"capacity_actors"`
	AllocatedActors      int64     `json:"allocated_actors"`
	CapacityMemoryBytes  int64     `json:"capacity_memory_bytes"`
	AllocatedMemoryBytes int64     `json:"allocated_memory_bytes"`
	ObservedAt           time.Time `json:"observed_at"`
	Source               string    `json:"source"`
}

func (c *Client) WorkerCapacity(ctx context.Context, worker string) (WorkerCapacity, error) {
	value, err := c.control.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: &ateapi.ObjectRef{Name: worker}})
	if err != nil {
		return WorkerCapacity{}, err
	}
	status := value.GetStatus()
	return WorkerCapacity{Worker: worker, CapacityActors: int64(status.GetCapacity().GetActors()), AllocatedActors: int64(status.GetAllocated().GetActors()), CapacityMemoryBytes: status.GetCapacity().GetMemoryBytes(), AllocatedMemoryBytes: status.GetAllocated().GetMemoryBytes(), ObservedAt: time.Now().UTC(), Source: "substrate_worker_status_capacity_and_allocated"}, nil
}
