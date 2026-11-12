package control

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// ToolRecoveryHint describes an unresolved effect without authorizing another write.
type ToolRecoveryHint struct {
	Code              string   `json:"code"`
	UnknownOperations []string `json:"unknown_operations"`
	AutomaticRetry    bool     `json:"automatic_retry"`
	AllowedTools      []string `json:"allowed_tools"`
	NextAction        string   `json:"next_action"`
	Message           string   `json:"message"`
}

func toolRecoveryHint(ids []string) *ToolRecoveryHint {
	return &ToolRecoveryHint{Code: "OUTCOME_UNKNOWN", UnknownOperations: ids,
		AllowedTools: []string{"file_read"}, NextAction: "inspect_workspace_read_only",
		Message: "工具副作用状态未知。先读取工作区文件并核对原操作回执；保留原操作ID，完成对账后再决定写入。shell 和写操作保持关闭。"}
}

func runtimeUnknownHint(err error) *ToolRecoveryHint {
	var response *runtimeHTTPError
	if !errors.As(err, &response) || response.code != "OUTCOME_UNKNOWN" {
		return nil
	}
	var details struct {
		Unknown []struct {
			ID string `json:"tool_op_id"`
		} `json:"unknown_operations"`
	}
	_ = json.Unmarshal(response.details, &details)
	ids := make([]string, 0, len(details.Unknown))
	for _, item := range details.Unknown {
		if item.ID != "" {
			ids = append(ids, item.ID)
		}
	}
	return toolRecoveryHint(ids)
}

func unknownTools(s *Session) *ToolRecoveryHint {
	ids := map[string]bool{}
	if s.ToolRecovery != nil {
		for _, id := range s.ToolRecovery.UnknownOperations {
			ids[id] = true
		}
	}
	for id, r := range s.Tools {
		if r.Status == "OUTCOME_UNKNOWN" {
			ids[id] = true
		}
	}
	if len(ids) == 0 && s.ToolRecovery == nil {
		return nil
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	sort.Strings(list)
	return toolRecoveryHint(list)
}

func unknownToolProblem(s *Session) error {
	return &APIError{Code: "OUTCOME_UNKNOWN", Status: 409,
		Message: "工具结果需要只读检查与对账，安全点及写操作保持关闭",
		Details: map[string]any{"epoch": s.Epoch, "recovery": unknownTools(s)}}
}

// Inspection needs a known running Actor. An uncertain lifecycle never grants it.
func toolInspectionAllowed(s *Session) bool {
	if unknownTools(s) == nil || s.State != "RECOVERY_REQUIRED" && s.State != "RUNNING" {
		return false
	}
	for _, receipt := range s.Lifecycle {
		if receipt.Status == "RUNNING" || receipt.Status == "OUTCOME_UNKNOWN" || receipt.Status == "WAITING_CAPACITY" {
			return false
		}
	}
	for _, receipt := range s.Scheduling {
		if receipt.Status == "RUNNING" || receipt.Status == "OUTCOME_UNKNOWN" {
			return false
		}
	}
	return true
}

// Only a pre-mutation rejection with a verified gate cancellation carries this type.
type quiesceRejected struct {
	code     string
	recovery *ToolRecoveryHint
}

func (e *quiesceRejected) Error() string { return "safe point rejected before snapshot: " + e.code }

func quiesceProblem(code, message string, retryAfter time.Time, recovery *ToolRecoveryHint) error {
	status := 409
	if code == "DRAIN_TIMEOUT" {
		status = 504
	}
	return &APIError{Code: code, Message: message, Status: status,
		Details: map[string]any{"retry_after": retryAfter, "recovery": recovery}}
}

// abortQuiesceLocked preserves the current epoch and every tool receipt. The
// caller has proved that SuspendActor has not run and the runtime gate reopened.
func (c *Controller) abortQuiesceLocked(ctx context.Context, s *Session, receipt *LifecycleReceipt, rejection *quiesceRejected) error {
	now := time.Now().UTC()
	receipt.Status, receipt.ErrorCode, receipt.Error = "ABORTED", rejection.code, rejection.Error()
	c.markEntity("lifecycle", key(s.TenantID, s.SessionID)+"/"+receipt.OperationID)
	receipt.RetryAfter = now.Add(c.cfg.QuiesceCooldown)
	s.QuiesceRetryAfter = receipt.RetryAfter
	s.State, s.Gate = "RUNNING", "OPEN"
	if rejection.recovery != nil {
		s.ToolRecovery = rejection.recovery
	}
	if hint := unknownTools(s); hint != nil {
		s.ToolRecovery = hint
		s.State, s.Gate = "RECOVERY_REQUIRED", "CLOSED"
	}
	if rejection.code == "DRAIN_TIMEOUT" {
		s.QuiesceTimeouts++
		c.observeLocked(ctx, "quiesce_timeout_total", 1, "drain_deadline_before_actor_mutation")
	}
	// Publish both receipts in the same commit. A crash before ModelBoundary's
	// latency bookkeeping must not turn this proven abort into an unknown action.
	if decision := s.Scheduling[receipt.OperationID]; decision != nil && decision.Status == "RUNNING" {
		decision.Status, decision.ErrorCode, decision.Error = "ABORTED", rejection.code, rejection.Error()
		decision.ResultEpoch, decision.SessionState, decision.FinishedAt = s.Epoch, s.State, &now
		decision.RetryAfter, decision.Recovery = receipt.RetryAfter, copyValue(s.ToolRecovery)
		c.markEntity("scheduling", key(s.TenantID, s.SessionID)+"/"+receipt.OperationID)
	}
	c.event(s, "lifecycle.quiesce.aborted", rejection.code)
	// A client disconnect must not discard the safe cancellation result.
	commitCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	if err := c.persistContext(commitCtx); err != nil {
		return err
	}
	return quiesceProblem(rejection.code, rejection.Error(), receipt.RetryAfter, s.ToolRecovery)
}

func asQuiesceRejection(err error) *quiesceRejected {
	var rejected *quiesceRejected
	if errors.As(err, &rejected) {
		return rejected
	}
	return nil
}
