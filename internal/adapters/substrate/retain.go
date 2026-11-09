package substrate

import (
	"context"
	"fmt"
	"strings"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RetainedSummary struct {
	TagName                  string `json:"tag_name"`
	TagUID                   string `json:"tag_uid"`
	TagVersion               int64  `json:"tag_version"`
	SourceActorUID           string `json:"source_actor_uid"`
	SourceSnapshotUUID       string `json:"source_snapshot_uuid"`
	SourceSnapshotGeneration int32  `json:"source_snapshot_generation"`
	SnapshotUUID             string `json:"snapshot_uuid"`
	SnapshotGeneration       int32  `json:"snapshot_generation"`
	SnapshotURI              string `json:"snapshot_uri"`
	Fidelity                 string `json:"fidelity"`
	Durable                  bool   `json:"durable"`
}

// RetainSnapshot reconciles the tag name before attempting a guarded copy.
func (c *Client) RetainSnapshot(ctx context.Context, actorName, actorUID, snapshotUUID string, generation int32, tagName string) (RetainedSummary, error) {
	if actorUID == "" || snapshotUUID == "" || generation < 1 || tagName == "" {
		return RetainedSummary{}, fmt.Errorf("snapshot retention requires actor UID, snapshot UUID, generation and tag name")
	}
	req := &ateapi.GetTagRequest{Tag: c.ref(tagName)}
	tag, err := c.control.GetTag(ctx, req)
	if status.Code(err) == codes.NotFound {
		tag, err = c.control.CreateTag(ctx, &ateapi.CreateTagRequest{Tag: &ateapi.Tag{Metadata: &ateapi.ResourceMetadata{Atespace: c.atespace, Name: tagName}, Scope: ateapi.TagScope_TAG_SCOPE_ATESPACE, SourceActor: c.ref(actorName)}, ExpectedUid: actorUID, ExpectedSnapshotUuid: snapshotUUID, ExpectedSnapshotGeneration: generation})
		if err != nil && ctx.Err() == nil {
			observed, observeErr := c.control.GetTag(ctx, req)
			if observeErr == nil {
				tag = observed
				err = nil
			}
		}
	}
	if err != nil {
		return RetainedSummary{}, err
	}
	source := tag.GetStatus()
	snapshot := source.GetSnapshot()
	summary := RetainedSummary{TagName: tag.GetMetadata().GetName(), TagUID: tag.GetMetadata().GetUid(), TagVersion: tag.GetMetadata().GetVersion(), SourceActorUID: source.GetSourceActorUid(), SourceSnapshotUUID: source.GetSourceSnapshotUuid(), SourceSnapshotGeneration: source.GetSourceSnapshotGeneration(), SnapshotUUID: snapshot.GetUuid(), SnapshotGeneration: snapshot.GetGeneration(), SnapshotURI: snapshot.GetDurableSnapshot().GetObject().GetSnapshotUri(), Fidelity: strings.TrimPrefix(snapshot.GetFidelity().String(), "SNAPSHOT_FIDELITY_"), Durable: snapshot.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED}
	if tag.GetMetadata().GetAtespace() != c.atespace || tag.GetSourceActor().GetAtespace() != c.atespace || tag.GetSourceActor().GetName() != actorName || summary.SourceActorUID != actorUID || summary.SourceSnapshotUUID != snapshotUUID || summary.SourceSnapshotGeneration != generation {
		return summary, status.Error(codes.FailedPrecondition, "retained tag source identity differs from the requested snapshot")
	}
	if tag.GetMetadata().GetDeleteTime() != nil || !summary.Durable || summary.SnapshotUUID == "" || summary.SnapshotURI == "" {
		return summary, status.Error(codes.FailedPrecondition, "retained tag is pending or deleting")
	}
	return summary, nil
}
