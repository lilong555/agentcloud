package control

// SessionStatus returns bounded polling fields under the durable commit gate.
func (c *Controller) SessionStatus(tenant, id string) (*DiagnosticSession, error) {
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	result := diagnosticSession(s)
	return &result, nil
}

// ToolStatus reads one durable receipt without accessing or waking its Actor.
// The commit gate preserves the same visibility boundary as session reads.
func (c *Controller) ToolStatus(tenant, id, operation string) (*ToolReceipt, error) {
	if !validID(operation) {
		return nil, problem(400, "invalid_tool_id", "valid tool operation identity is required")
	}
	c.lock()
	defer c.mu.Unlock()
	if err := c.ready(); err != nil {
		return nil, err
	}
	s, err := c.find(tenant, id)
	if err != nil {
		return nil, err
	}
	receipt := s.Tools[operation]
	if receipt == nil {
		return nil, problem(404, "tool_not_found", "tool receipt not found")
	}
	result := copyValue(receipt)
	if result.Status == "OUTCOME_UNKNOWN" {
		result.Recovery = unknownTools(s)
	}
	return result, nil
}
