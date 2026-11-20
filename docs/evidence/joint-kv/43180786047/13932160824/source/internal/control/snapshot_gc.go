package control

import (
	"context"
	"errors"
	"sort"
	"time"

	substrateapi "github.com/lilong555/agentcloud/internal/adapters/substrate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SnapshotGCConfig struct {
	Disabled    bool
	Keep        int
	Batch       int
	Interval    time.Duration
	OrphanGrace time.Duration
	RetryBase   time.Duration
	RetryMax    time.Duration
	Timeout     time.Duration
}

func (cfg *SnapshotGCConfig) defaults() error {
	if cfg.Keep == 0 {
		cfg.Keep = 2
	}
	if cfg.Batch == 0 {
		cfg.Batch = 8
	}
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.OrphanGrace == 0 {
		cfg.OrphanGrace = time.Hour
	}
	if cfg.RetryBase == 0 {
		cfg.RetryBase = time.Second
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 5 * time.Minute
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Keep < 1 || cfg.Keep > 256 || cfg.Batch < 1 || cfg.Batch > 256 || cfg.Interval <= 0 || cfg.OrphanGrace <= 0 || cfg.RetryBase <= 0 || cfg.RetryMax < cfg.RetryBase || cfg.Timeout <= 0 {
		return errors.New("snapshot GC requires keep/batch in 1..256 and positive durations with retry_max >= retry_base")
	}
	return nil
}

// SnapshotGCState belongs to the tenant/workspace's unique execution Session.
// Tombstones preserve the immutable UID across journal reloads and historical
// receipt reconciliation, including a DELETE whose response was lost.
type SnapshotGCState struct {
	Tags map[string]*SnapshotGCRecord `json:"tags"`
}

type SnapshotGCRecord struct {
	OperationID        string    `json:"operation_id"`
	ManifestID         string    `json:"manifest_id,omitempty"`
	TagName            string    `json:"tag_name"`
	TagUID             string    `json:"tag_uid,omitempty"`
	ActorName          string    `json:"actor_name"`
	ActorUID           string    `json:"actor_uid"`
	SnapshotUUID       string    `json:"snapshot_uuid,omitempty"`
	SnapshotGeneration int64     `json:"snapshot_generation,omitempty"`
	State              string    `json:"state"`
	CreatedAt          time.Time `json:"created_at"`
	CommittedAt        time.Time `json:"committed_at,omitempty"`
	VerifiedAt         time.Time `json:"verified_at,omitempty"`
	DeleteState        string    `json:"delete_state,omitempty"`
	DeleteAttempts     uint64    `json:"delete_attempts,omitempty"`
	DeleteStartedAt    time.Time `json:"delete_started_at,omitempty"`
	DeletedAt          time.Time `json:"deleted_at,omitempty"`
	RetryAfter         time.Time `json:"retry_after,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	VerifyFailures     uint64    `json:"verify_failures,omitempty"`
}

type snapshotTagBackend interface {
	SnapshotTag(context.Context, string) (substrateapi.SnapshotTag, error)
	DeleteSnapshotTag(context.Context, substrateapi.SnapshotTag) error
}

func (b *SubstrateBackend) SnapshotTag(ctx context.Context, name string) (substrateapi.SnapshotTag, error) {
	return b.cfg.Client.SnapshotTag(ctx, name)
}
func (b *SubstrateBackend) DeleteSnapshotTag(ctx context.Context, tag substrateapi.SnapshotTag) error {
	return b.cfg.Client.DeleteSnapshotTag(ctx, tag)
}

func snapshotTagName(s *Session, operation string) string {
	return "ac-snapshot-" + payloadHash([]string{s.TenantID, s.SessionID, operation})[:24]
}

func (c *Controller) snapshotGCSupported() bool {
	_, supported := c.cfg.Backend.(snapshotTagBackend)
	return supported && !c.cfg.SnapshotGC.Disabled
}

func snapshotLedger(s *Session) *SnapshotGCState {
	if s.SnapshotGC == nil {
		s.SnapshotGC = &SnapshotGCState{}
	}
	if s.SnapshotGC.Tags == nil {
		s.SnapshotGC.Tags = map[string]*SnapshotGCRecord{}
	}
	return s.SnapshotGC
}

// Record the deterministic Tag name before any upstream checkpoint mutation.
func (c *Controller) recordSnapshotIntentLocked(s *Session, operation string) {
	if !c.snapshotGCSupported() {
		return
	}
	ledger := snapshotLedger(s)
	name := snapshotTagName(s, operation)
	if ledger.Tags[name] == nil {
		ledger.Tags[name] = &SnapshotGCRecord{OperationID: operation, TagName: name, ActorName: s.Actor.Name, ActorUID: s.Actor.UID, State: "PENDING", CreatedAt: time.Now().UTC()}
		c.markSession(s)
	}
}

func (c *Controller) recordCommittedSnapshotLocked(s *Session, operation string, manifest *Manifest) {
	if !c.snapshotGCSupported() || manifest == nil || manifest.State != "COMMITTED" || manifest.RetainedTag == "" || manifest.RetainedTagUID == "" || manifest.ActorUID == "" || manifest.SnapshotUUID == "" || manifest.SnapshotGeneration < 1 {
		return
	}
	ledger := snapshotLedger(s)
	prior := ledger.Tags[manifest.RetainedTag]
	if prior == nil && operation != "" && manifest.RetainedTag != snapshotTagName(s, operation) {
		return
	}
	if prior != nil && (prior.DeleteState != "" || prior.TagUID != "" && prior.TagUID != manifest.RetainedTagUID) {
		return
	}
	// Prepared suspend reuses the checkpoint's Tag and commit timestamp.
	if prior != nil && prior.State == "COMMITTED" {
		return
	}
	ledger.Tags[manifest.RetainedTag] = &SnapshotGCRecord{OperationID: operation, ManifestID: manifest.ID, TagName: manifest.RetainedTag, TagUID: manifest.RetainedTagUID,
		ActorName: s.Actor.Name, ActorUID: manifest.ActorUID, SnapshotUUID: manifest.SnapshotUUID, SnapshotGeneration: manifest.SnapshotGeneration,
		State: "COMMITTED", CreatedAt: manifest.CreatedAt, CommittedAt: manifest.CreatedAt, VerifiedAt: manifest.CreatedAt}
	c.markSession(s)
}

// Migration is deterministic and leaves durable deletion tombstones intact.
func (c *Controller) reconcileSnapshotGCLocked(s *Session) {
	if !c.snapshotGCSupported() {
		return
	}
	ids := make([]string, 0, len(s.Lifecycle))
	for id := range s.Lifecycle {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := s.Lifecycle[id]
		if r == nil || r.Action == "resume" {
			continue
		}
		if r.Status == "COMPLETED" && r.Manifest != nil {
			c.recordCommittedSnapshotLocked(s, id, r.Manifest)
		} else {
			c.recordSnapshotIntentLocked(s, id)
			record := s.SnapshotGC.Tags[snapshotTagName(s, id)]
			if record != nil && record.State == "PENDING" && (r.Status == "ABORTED" || r.Status == "FAILED") {
				record.State = "ORPHAN"
				c.markSession(s)
			}
		}
	}
	// The current manifest is protected even in journals lacking receipts.
	if s.Manifest != nil {
		c.recordCommittedSnapshotLocked(s, "", s.Manifest)
	}
}

func snapshotGCUnsettled(s *Session) bool {
	if s.ToolRecovery != nil {
		return true
	}
	switch s.State {
	case "CREATING", "QUIESCING", "SUSPENDING", "RESUMING", "RECOVERY_REQUIRED":
		return true
	}
	for _, r := range s.Lifecycle {
		if r == nil {
			return true
		}
		switch r.Status {
		case "COMPLETED", "ABORTED", "FAILED":
		default:
			return true
		}
	}
	for _, r := range s.Scheduling {
		if r == nil {
			return true
		}
		switch r.Status {
		case "COMPLETED", "ABORTED", "FAILED":
		default:
			return true
		}
	}
	for _, r := range s.Tools {
		if r == nil || r.Status == "OUTCOME_UNKNOWN" {
			return true
		}
	}
	return false
}

func (c *Controller) snapshotProtectedLocked(name, uid string) bool {
	for _, s := range c.state.Sessions {
		if s.Actor.RetainedTag == name || uid != "" && s.Actor.RetainedTagUID == uid {
			return true
		}
		if m := s.Manifest; m != nil && (m.RetainedTag == name || uid != "" && m.RetainedTagUID == uid) {
			return true
		}
		for _, r := range s.Lifecycle {
			if r == nil || r.Status == "COMPLETED" || r.Status == "ABORTED" || r.Status == "FAILED" {
				continue
			}
			if snapshotTagName(s, r.OperationID) == name {
				return true
			}
			if m := r.Manifest; m != nil && (m.RetainedTag == name || uid != "" && m.RetainedTagUID == uid) {
				return true
			}
		}
		for _, r := range s.Scheduling {
			if r == nil || r.Status == "COMPLETED" || r.Status == "ABORTED" || r.Status == "FAILED" {
				continue
			}
			if snapshotTagName(s, r.OperationID) == name {
				return true
			}
			if m := r.Manifest; m != nil && (m.RetainedTag == name || uid != "" && m.RetainedTagUID == uid) {
				return true
			}
		}
	}
	return false
}

func snapshotTagMatches(record *SnapshotGCRecord, tag substrateapi.SnapshotTag) bool {
	return record.TagName == tag.TagName && record.TagUID != "" && record.TagUID == tag.TagUID && record.ActorName == tag.SourceActorName && record.ActorUID == tag.SourceActorUID && record.SnapshotUUID != "" && record.SnapshotUUID == tag.SourceSnapshotUUID && record.SnapshotGeneration == int64(tag.SourceSnapshotGeneration)
}

func (c *Controller) wakeSnapshotGC() {
	if c.snapshotGCWake != nil {
		select {
		case c.snapshotGCWake <- struct{}{}:
		default:
		}
	}
}

func (c *Controller) startSnapshotGC() {
	if !c.snapshotGCSupported() {
		return
	}
	c.snapshotGCWake = make(chan struct{}, 1)
	c.workers.Add(1)
	go c.snapshotGCLoop()
}

func (c *Controller) snapshotGCLoop() {
	defer c.workers.Done()
	ticker := time.NewTicker(c.cfg.SnapshotGC.Interval)
	defer ticker.Stop()
	c.wakeSnapshotGC()
	cursor := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		case <-c.snapshotGCWake:
		}
		c.lock()
		keys := make([]string, 0, len(c.state.Sessions))
		for sessionKey := range c.state.Sessions {
			keys = append(keys, sessionKey)
		}
		c.mu.Unlock()
		sort.Strings(keys)
		remaining := c.cfg.SnapshotGC.Batch
		for i := 0; i < len(keys) && remaining > 0 && c.ctx.Err() == nil; i++ {
			index := (cursor + i) % len(keys)
			remaining -= c.collectSessionSnapshots(c.ctx, keys[index], remaining)
		}
		if len(keys) > 0 {
			cursor = (cursor + 1) % len(keys)
		}
	}
}

// A sweep verifies retained recovery points before deleting an older one.
// Verification reads are bounded by Keep+budget and deletion attempts by budget. The journal
// publishes immutable deletion intent before the adapter touches storage.
func (c *Controller) collectSessionSnapshots(ctx context.Context, sessionKey string, budget int) int {
	backend, ok := c.cfg.Backend.(snapshotTagBackend)
	if !ok || budget < 1 {
		return 0
	}
	c.lock()
	s := c.state.Sessions[sessionKey]
	if s == nil || c.ready() != nil || snapshotGCUnsettled(s) {
		c.mu.Unlock()
		return 0
	}
	c.reconcileSnapshotGCLocked(s)
	if len(c.dirty) > 0 {
		if err := c.persistContext(ctx); err != nil {
			c.mu.Unlock()
			return 0
		}
	}
	var committed, orphans []*SnapshotGCRecord
	if s.SnapshotGC != nil {
		for _, record := range s.SnapshotGC.Tags {
			if record == nil || record.DeleteState == "DELETED" || record.DeleteState == "QUARANTINED" {
				continue
			}
			if record.State == "COMMITTED" {
				committed = append(committed, copyValue(record))
			}
			if record.State == "ORPHAN" {
				orphans = append(orphans, copyValue(record))
			}
		}
	}
	c.mu.Unlock()
	sort.Slice(committed, func(i, j int) bool {
		if committed[i].CommittedAt.Equal(committed[j].CommittedAt) {
			return committed[i].TagName < committed[j].TagName
		}
		return committed[i].CommittedAt.After(committed[j].CommittedAt)
	})
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].TagName < orphans[j].TagName })
	kept := map[string]string{}
	attempts, reads := 0, 0
	for _, record := range append(committed, orphans...) {
		if attempts >= budget || reads >= c.cfg.SnapshotGC.Keep+budget || ctx.Err() != nil {
			break
		}
		isOrphan := record.State == "ORPHAN"
		if time.Now().Before(record.RetryAfter) {
			continue
		}
		if isOrphan && time.Since(record.CreatedAt) < c.cfg.SnapshotGC.OrphanGrace {
			continue
		}
		if len(kept) >= c.cfg.SnapshotGC.Keep || isOrphan {
			if time.Now().Before(record.RetryAfter) {
				continue
			}
			c.lock()
			protected := c.snapshotProtectedLocked(record.TagName, record.TagUID)
			c.mu.Unlock()
			if protected {
				continue
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, c.cfg.SnapshotGC.Timeout)
		tag, err := backend.SnapshotTag(readCtx, record.TagName)
		cancel()
		reads++
		if err != nil {
			c.snapshotVerificationFailure(ctx, sessionKey, record, err)
			continue
		}
		if isOrphan && record.TagUID == "" {
			if tag.SourceActorName != record.ActorName || tag.SourceActorUID != record.ActorUID || tag.TagUID == "" || tag.SourceSnapshotUUID == "" || tag.SourceSnapshotGeneration < 1 || tag.CreatedAt.IsZero() || time.Since(tag.CreatedAt) < c.cfg.SnapshotGC.OrphanGrace {
				continue
			}
			record.TagUID, record.SnapshotUUID, record.SnapshotGeneration = tag.TagUID, tag.SourceSnapshotUUID, int64(tag.SourceSnapshotGeneration)
		}
		if !snapshotTagMatches(record, tag) {
			c.snapshotVerificationFailure(ctx, sessionKey, record, status.Error(codes.FailedPrecondition, "snapshot GC Tag UID or ownership changed"))
			continue
		}
		if !isOrphan && len(kept) < c.cfg.SnapshotGC.Keep && record.DeleteState == "" && !tag.Deleting && tag.Durable && tag.SnapshotUUID != "" && tag.SnapshotURI != "" {
			kept[record.TagName] = record.TagUID
			continue
		}
		if !isOrphan && len(kept) < c.cfg.SnapshotGC.Keep {
			if record.DeleteState == "" {
				c.snapshotVerificationFailure(ctx, sessionKey, record, status.Error(codes.Unavailable, "committed snapshot Tag is awaiting durability verification"))
			}
			continue
		}
		if !c.claimSnapshotDelete(ctx, sessionKey, record, kept) {
			continue
		}
		attempts++
		deleteCtx, cancel := context.WithTimeout(ctx, c.cfg.SnapshotGC.Timeout)
		err = backend.DeleteSnapshotTag(deleteCtx, tag)
		cancel()
		if !c.finishSnapshotDelete(ctx, sessionKey, record, err) {
			break
		}
	}
	return attempts
}

func (c *Controller) claimSnapshotDelete(ctx context.Context, sessionKey string, observed *SnapshotGCRecord, kept map[string]string) bool {
	c.lock()
	defer c.mu.Unlock()
	s := c.state.Sessions[sessionKey]
	if s == nil || c.ready() != nil || snapshotGCUnsettled(s) || c.snapshotProtectedLocked(observed.TagName, observed.TagUID) {
		return false
	}
	r := snapshotLedger(s).Tags[observed.TagName]
	if r == nil || r.State != observed.State || r.ActorName != observed.ActorName || r.ActorUID != observed.ActorUID || r.DeleteState == "DELETED" || r.DeleteState == "QUARANTINED" || r.TagUID != "" && r.TagUID != observed.TagUID || time.Now().Before(r.RetryAfter) {
		return false
	}
	if r.State == "COMMITTED" {
		verified := 0
		for name, uid := range kept {
			if retained := s.SnapshotGC.Tags[name]; retained != nil && retained.TagUID == uid && retained.State == "COMMITTED" && retained.DeleteState == "" {
				verified++
				retained.VerifiedAt = time.Now().UTC()
			}
		}
		if verified < c.cfg.SnapshotGC.Keep {
			return false
		}
	} else if r.State != "ORPHAN" || time.Since(r.CreatedAt) < c.cfg.SnapshotGC.OrphanGrace {
		return false
	}
	r.TagUID, r.SnapshotUUID, r.SnapshotGeneration = observed.TagUID, observed.SnapshotUUID, observed.SnapshotGeneration
	r.DeleteState = "DELETE_PENDING"
	r.DeleteAttempts++
	r.DeleteStartedAt = time.Now().UTC()
	r.RetryAfter = r.DeleteStartedAt.Add(snapshotRetryDelay(c.cfg.SnapshotGC, r.DeleteAttempts))
	r.LastError = ""
	c.markSession(s)
	c.event(s, "snapshot.gc.delete_started", r.TagName)
	return c.persistContext(ctx) == nil
}

func (c *Controller) snapshotVerificationFailure(ctx context.Context, sessionKey string, observed *SnapshotGCRecord, verifyErr error) {
	c.lock()
	defer c.mu.Unlock()
	s := c.state.Sessions[sessionKey]
	if s == nil || s.SnapshotGC == nil || c.ready() != nil {
		return
	}
	r := s.SnapshotGC.Tags[observed.TagName]
	if r == nil || r.TagUID != observed.TagUID || r.DeleteState == "DELETED" {
		return
	}
	r.LastError = verifyErr.Error()
	switch status.Code(verifyErr) {
	case codes.NotFound:
		r.DeleteState, r.DeletedAt, r.RetryAfter = "DELETED", time.Now().UTC(), time.Time{}
		c.event(s, "snapshot.gc.absent", r.TagName)
	case codes.FailedPrecondition:
		r.DeleteState, r.RetryAfter = "QUARANTINED", time.Time{}
		c.event(s, "snapshot.gc.quarantined", r.TagName)
	default:
		r.VerifyFailures++
		r.RetryAfter = time.Now().UTC().Add(snapshotRetryDelay(c.cfg.SnapshotGC, r.VerifyFailures))
		c.event(s, "snapshot.gc.verify_retry", r.TagName)
	}
	c.markSession(s)
	_ = c.persistContext(ctx)
}

func (c *Controller) finishSnapshotDelete(ctx context.Context, sessionKey string, observed *SnapshotGCRecord, deleteErr error) bool {
	c.lock()
	defer c.mu.Unlock()
	s := c.state.Sessions[sessionKey]
	if s == nil || s.SnapshotGC == nil || c.unhealthy != nil {
		return false
	}
	r := s.SnapshotGC.Tags[observed.TagName]
	if r == nil || r.TagUID != observed.TagUID || r.DeleteState != "DELETE_PENDING" {
		return false
	}
	if deleteErr == nil {
		r.DeleteState, r.LastError, r.RetryAfter = "DELETED", "", time.Time{}
		r.DeletedAt = time.Now().UTC()
		c.event(s, "snapshot.gc.deleted", r.TagName)
	} else {
		r.LastError = deleteErr.Error()
		r.RetryAfter = time.Now().UTC().Add(snapshotRetryDelay(c.cfg.SnapshotGC, r.DeleteAttempts))
		c.event(s, "snapshot.gc.retry", r.TagName+": "+r.LastError)
	}
	c.markSession(s)
	return c.persistContext(ctx) == nil
}

func snapshotRetryDelay(cfg SnapshotGCConfig, attempts uint64) time.Duration {
	delay := cfg.RetryBase
	for i := uint64(1); i < attempts && delay < cfg.RetryMax; i++ {
		if delay > cfg.RetryMax/2 {
			return cfg.RetryMax
		}
		delay *= 2
	}
	return min(delay, cfg.RetryMax)
}
