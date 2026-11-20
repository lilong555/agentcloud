package control

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lilong555/agentcloud/internal/journal"
)

func (c *Controller) lock() {
	c.mu.Lock()
	for c.committing {
		c.commitCond.Wait()
	}
}
func (c *Controller) markEntity(kind, key string) {
	if c.dirty == nil {
		c.dirty = map[string]map[string]bool{}
	}
	if c.dirty[kind] == nil {
		c.dirty[kind] = map[string]bool{}
	}
	c.dirty[kind][key] = true
}
func (c *Controller) markSession(s *Session) { c.markEntity("sessions", key(s.TenantID, s.SessionID)) }
func (c *Controller) markAll() {
	for sessionKey, s := range c.state.Sessions {
		c.markEntity("sessions", sessionKey)
		for id := range s.Tools {
			c.markEntity("tools", sessionKey+"/"+id)
		}
		for id := range s.Lifecycle {
			c.markEntity("lifecycle", sessionKey+"/"+id)
		}
		for id := range s.Scheduling {
			c.markEntity("scheduling", sessionKey+"/"+id)
		}
	}
	for requestKey := range c.state.Requests {
		c.markEntity("requests", requestKey)
	}
	for _, event := range c.state.Events {
		eventKey := fmt.Sprintf("%020d", event.Sequence)
		c.markEntity("events", eventKey)
		if c.pendingEvents == nil {
			c.pendingEvents = map[string]Event{}
		}
		c.pendingEvents[eventKey] = event
	}
	for i := range c.state.Observations {
		if c.state.Observations[i].Sequence == 0 {
			c.state.ObservationSequence++
			c.state.Observations[i].Sequence = c.state.ObservationSequence
		}
		item := c.state.Observations[i]
		observationKey := fmt.Sprintf("%020d", item.Sequence)
		c.markEntity("observations", observationKey)
		if c.pendingObservations == nil {
			c.pendingObservations = map[string]RecordedMeasurement{}
		}
		c.pendingObservations[observationKey] = item
	}
}
func (c *Controller) entityChanges() (journal.EntityChanges, error) {
	metadata, err := json.Marshal(map[string]any{"entity_format": 1, "version": c.state.Version, "event_sequence": c.state.EventSequence, "observation_sequence": c.state.ObservationSequence})
	if err != nil {
		return journal.EntityChanges{}, err
	}
	changes := journal.EntityChanges{Metadata: metadata}
	if len(c.state.Events) > 0 {
		changes.EventFloor = c.state.Events[0].Sequence
	}
	if len(c.state.Observations) > 0 {
		changes.ObservationFloor = c.state.Observations[0].Sequence
	}
	eventValues := c.pendingEvents
	observationValues := c.pendingObservations
	for kind, keys := range c.dirty {
		for entityKey := range keys {
			var value any
			sequence := uint64(0)
			switch kind {
			case "sessions":
				s := c.state.Sessions[entityKey]
				if s == nil {
					changes.Deletes = append(changes.Deletes, journal.Entity{Kind: kind, Key: entityKey})
					continue
				}
				bare := *s
				bare.Tools = nil
				bare.Lifecycle = nil
				bare.Scheduling = nil
				value = bare
			case "requests":
				value = c.state.Requests[entityKey]
			case "events":
				event, ok := eventValues[entityKey]
				if !ok {
					continue
				}
				value = event
				sequence = event.Sequence
			case "observations":
				item, ok := observationValues[entityKey]
				if !ok {
					continue
				}
				value = item
				sequence = item.Sequence
			case "tools", "lifecycle", "scheduling":
				split := strings.LastIndex(entityKey, "/")
				if split < 0 {
					return changes, fmt.Errorf("invalid receipt key")
				}
				s := c.state.Sessions[entityKey[:split]]
				if s == nil {
					continue
				}
				id := entityKey[split+1:]
				switch kind {
				case "tools":
					value = s.Tools[id]
				case "lifecycle":
					value = s.Lifecycle[id]
				case "scheduling":
					value = s.Scheduling[id]
				}
			default:
				return changes, fmt.Errorf("unknown entity kind")
			}
			data, err := json.Marshal(value)
			if err != nil {
				return changes, err
			}
			changes.Entities = append(changes.Entities, journal.Entity{Kind: kind, Key: entityKey, Data: data, Sequence: sequence})
		}
	}
	journal.SortEntities(changes.Entities)
	return changes, nil
}
