package control

import (
	"context"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/dynamo"
)

func (c *Controller) FinalizeDynamo(ctx context.Context, tenant, id string) (dynamo.FinalizeReceipt, error) {
	bridge, ok := c.cfg.Signals.(*DynamoBridge)
	if !ok {
		return dynamo.FinalizeReceipt{}, problem(409, "native_dynamo_unavailable", "native Dynamo bridge is required")
	}
	ctx = c.operationContext(ctx, tenant, id, "native_finalize")
	c.lock()
	if err := c.ready(); err != nil {
		c.mu.Unlock()
		return dynamo.FinalizeReceipt{}, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		c.mu.Unlock()
		return dynamo.FinalizeReceipt{}, err
	}
	if s.ActiveTools != 0 {
		c.mu.Unlock()
		return dynamo.FinalizeReceipt{}, problem(409, "active_tools", "native finalization requires completed tools")
	}
	for _, r := range c.state.Requests {
		if r.TenantID == tenant && r.SessionID == id && (r.Status == "PENDING" || r.Status == "DISPATCHING" || r.Status == "OUTCOME_UNKNOWN") {
			c.mu.Unlock()
			return dynamo.FinalizeReceipt{}, problem(409, "active_model_requests", "native finalization requires resolved model outcomes")
		}
	}
	snapshot := copyValue(s)
	c.operations.Add(1)
	defer c.operations.Done()
	c.mu.Unlock()
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	receipt, err := bridge.Finalize(probe, snapshot)
	if err != nil {
		return receipt, err
	}
	c.lock()
	defer c.mu.Unlock()
	c.recordFinalizationLocked(snapshot, receipt, "")
	if err = c.persistContext(ctx); err != nil {
		return dynamo.FinalizeReceipt{}, err
	}
	return receipt, nil
}

func (c *Controller) recordFinalizationLocked(snapshot *Session, receipt dynamo.FinalizeReceipt, completedID string) {
	s := c.state.Sessions[key(snapshot.TenantID, snapshot.SessionID)]
	if s == nil || s.Epoch != snapshot.Epoch {
		return
	}
	for _, r := range c.state.Requests {
		if r.TenantID == s.TenantID && r.SessionID == s.SessionID && r.ID != completedID && (r.Status == "PENDING" || r.Status == "DISPATCHING" || r.Status == "OUTCOME_UNKNOWN") {
			return
		}
	}
	s.DynamoFinalized = true
	s.DynamoFinalizedEpoch = snapshot.Epoch
	s.DynamoFinalizedSnapshot = &receipt.Snapshot
	c.event(s, "native.finalized", completedID)
}
