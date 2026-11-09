package control

import (
	"context"
	"encoding/json"
	"time"
)

type Actor struct {
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Version            int64  `json:"version"`
	SnapshotUUID       string `json:"snapshot_uuid,omitempty"`
	SnapshotGeneration int64  `json:"snapshot_generation,omitempty"`
	ActualFidelity     string `json:"actual_fidelity"`
	BootID             string `json:"boot_id"`
	RetainedTag        string `json:"retained_tag,omitempty"`
	RetainedTagUID     string `json:"retained_tag_uid,omitempty"`
	SnapshotFidelity   string `json:"snapshot_fidelity,omitempty"`
}
type Session struct {
	Scheduling        map[string]*BoundaryDecision `json:"scheduling,omitempty"`
	RunningSince      time.Time                    `json:"running_since,omitempty"`
	LastResumedAt     time.Time                    `json:"last_resumed_at,omitempty"`
	TenantID          string                       `json:"tenant_id"`
	WorkspaceID       string                       `json:"workspace_id"`
	SessionID         string                       `json:"session_id"`
	Mode              string                       `json:"mode"`
	Profile           string                       `json:"profile,omitempty"`
	AblationGroup     string                       `json:"ablation_group,omitempty"`
	State             string                       `json:"state"`
	Gate              string                       `json:"gate"`
	Epoch             uint64                       `json:"epoch"`
	BarrierGeneration uint64                       `json:"barrier_generation"`
	ReceiptWatermark  uint64                       `json:"receipt_watermark"`
	NextReceipt       uint64                       `json:"next_receipt"`
	SignalSequence    uint64                       `json:"signal_sequence"`
	Actor             Actor                        `json:"actor"`
	ActorUID          string                       `json:"actor_uid"`
	Backend           string                       `json:"backend"`
	ActualFidelity    string                       `json:"actual_fidelity"`
	ActiveTools       int                          `json:"active_tools"`
	Tools             map[string]*ToolReceipt      `json:"tools"`
	Lifecycle         map[string]*LifecycleReceipt `json:"lifecycle"`
	Manifest          *Manifest                    `json:"manifest,omitempty"`
	CreatedAt         time.Time                    `json:"created_at"`
	UpdatedAt         time.Time                    `json:"updated_at"`
}
type Manifest struct {
	ID                 string    `json:"manifest_id"`
	State              string    `json:"state"`
	Epoch              uint64    `json:"epoch"`
	BarrierGeneration  uint64    `json:"barrier_generation"`
	ReceiptWatermark   uint64    `json:"receipt_watermark"`
	ActorUID           string    `json:"actor_uid"`
	SnapshotUUID       string    `json:"snapshot_uuid"`
	SnapshotGeneration int64     `json:"snapshot_generation"`
	WorkspaceDigest    string    `json:"workspace_digest,omitempty"`
	Fidelity           string    `json:"fidelity"`
	CheckpointID       string    `json:"checkpoint_id,omitempty"`
	RetainedTag        string    `json:"retained_tag,omitempty"`
	RetainedTagUID     string    `json:"retained_tag_uid,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
}
type ToolRequest struct {
	ID        string `json:"tool_op_id"`
	Epoch     uint64 `json:"epoch"`
	Kind      string `json:"kind"`
	Path      string `json:"path,omitempty"`
	Content   string `json:"content,omitempty"`
	Command   string `json:"command,omitempty"`
	TimeoutMS int    `json:"timeout_ms,omitempty"`
}
type ToolResult struct {
	Content  string `json:"content,omitempty"`
	Stdout   string `json:"stdout,omitempty"`
	Stderr   string `json:"stderr,omitempty"`
	ExitCode int    `json:"exit_code"`
}
type ToolReceipt struct {
	ID          string     `json:"tool_op_id"`
	Epoch       uint64     `json:"epoch"`
	Status      string     `json:"status"`
	PayloadHash string     `json:"payload_hash"`
	Sequence    uint64     `json:"receipt_seq"`
	Result      ToolResult `json:"result"`
	Error       string     `json:"error,omitempty"`
	Deadline    time.Time  `json:"deadline,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}
type LifecycleRequest struct {
	OperationID   string `json:"operation_id"`
	ExpectedEpoch uint64 `json:"expected_epoch"`
	CheckpointID  string `json:"checkpoint_id,omitempty"`
}
type LifecycleReceipt struct {
	OperationID string    `json:"operation_id"`
	Action      string    `json:"action"`
	Epoch       uint64    `json:"epoch"`
	PayloadHash string    `json:"payload_hash"`
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
	Manifest    *Manifest `json:"manifest,omitempty"`
}
type ModelRequest struct {
	TerminalTurn   bool            `json:"terminal_turn,omitempty"`
	ID             string          `json:"logical_request_id"`
	TenantID       string          `json:"tenant_id"`
	SessionID      string          `json:"session_id"`
	PayloadHash    string          `json:"payload_hash"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	AttemptID      string          `json:"attempt_id,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	FirstToken     string          `json:"first_token,omitempty"`
	FirstTokenKind string          `json:"first_token_kind,omitempty"`
	FirstTokenAt   *time.Time      `json:"first_token_at,omitempty"`
	Error          string          `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	ClaimedEpoch   uint64          `json:"claimed_epoch,omitempty"`
}
type Event struct {
	Sequence  uint64    `json:"sequence"`
	At        time.Time `json:"at"`
	TenantID  string    `json:"tenant_id"`
	SessionID string    `json:"session_id"`
	Kind      string    `json:"kind"`
	Detail    string    `json:"detail,omitempty"`
}
type State struct {
	Version       int                      `json:"version"`
	Sessions      map[string]*Session      `json:"sessions"`
	Requests      map[string]*ModelRequest `json:"requests"`
	Events        []Event                  `json:"events"`
	EventSequence uint64                   `json:"event_sequence"`
}
type CreateSession struct {
	SessionID     string `json:"session_id"`
	WorkspaceID   string `json:"workspace_id"`
	Mode          string `json:"mode"`
	Profile       string `json:"profile,omitempty"`
	AblationGroup string `json:"ablation_group,omitempty"`
	ActorName     string `json:"actor_name,omitempty"`
}
type Backend interface {
	Name() string
	Ensure(context.Context, *Session) (Actor, error)
	Tool(context.Context, *Session, ToolRequest) (ToolResult, error)
	Checkpoint(context.Context, *Session, string) (Actor, string, error)
	Suspend(context.Context, *Session) (Actor, error)
	Resume(context.Context, *Session) (Actor, error)
}
type SignalSink interface {
	Send(context.Context, *Session, string) error
}
