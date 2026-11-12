package substrate

import (
	"context"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SnapshotTag is an observation of one immutable Tag and its source identity.
type SnapshotTag struct {
	RetainedSummary
	SourceActorName string    `json:"source_actor_name"`
	CreatedAt       time.Time `json:"created_at"`
	Deleting        bool      `json:"deleting"`
}

// SnapshotTag reads only; GC must never recreate a missing retained Tag.
func (c *Client) SnapshotTag(ctx context.Context, name string) (SnapshotTag, error) {
	tag, err := c.control.GetTag(ctx, &ateapi.GetTagRequest{Tag: c.ref(name)})
	if err != nil {
		return SnapshotTag{}, err
	}
	if tag.GetMetadata().GetAtespace() != c.atespace || tag.GetMetadata().GetName() != name || tag.GetSourceActor().GetAtespace() != c.atespace {
		return SnapshotTag{}, status.Error(codes.FailedPrecondition, "snapshot Tag namespace or name differs")
	}
	source := tag.GetStatus()
	snapshot := source.GetSnapshot()
	result := SnapshotTag{RetainedSummary: RetainedSummary{
		TagName: name, TagUID: tag.GetMetadata().GetUid(), TagVersion: tag.GetMetadata().GetVersion(),
		SourceActorUID: source.GetSourceActorUid(), SourceSnapshotUUID: source.GetSourceSnapshotUuid(),
		SourceSnapshotGeneration: source.GetSourceSnapshotGeneration(), SnapshotUUID: snapshot.GetUuid(),
		SnapshotGeneration: snapshot.GetGeneration(), SnapshotURI: snapshot.GetDurableSnapshot().GetObject().GetSnapshotUri(),
		Durable: snapshot.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
	}, SourceActorName: tag.GetSourceActor().GetName(), Deleting: tag.GetMetadata().GetDeleteTime() != nil}
	if created := tag.GetMetadata().GetCreateTime(); created != nil && created.IsValid() {
		result.CreatedAt = created.AsTime()
	}
	return result, nil
}

// DeleteSnapshotTag resumes the upstream storage-first deletion workflow. Every
// attempt rereads ownership and fences the DELETE with the observed UID/version.
// Existing Actors borrowing the Tag keep it alive. Actor discovery is bounded;
// an incomplete inventory retains the Tag for a later retry.
func (c *Client) DeleteSnapshotTag(ctx context.Context, expected SnapshotTag) error {
	if expected.TagName == "" || expected.TagUID == "" || expected.SourceActorName == "" || expected.SourceActorUID == "" || expected.SourceSnapshotUUID == "" || expected.SourceSnapshotGeneration < 1 {
		return status.Error(codes.InvalidArgument, "snapshot GC requires complete Tag and source identity")
	}
	current, err := c.SnapshotTag(ctx, expected.TagName)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if current.TagUID != expected.TagUID || current.SourceActorName != expected.SourceActorName || current.SourceActorUID != expected.SourceActorUID || current.SourceSnapshotUUID != expected.SourceSnapshotUUID || current.SourceSnapshotGeneration != expected.SourceSnapshotGeneration || current.TagVersion < 1 {
		return status.Error(codes.FailedPrecondition, "snapshot GC Tag UID or ownership differs")
	}
	options := &ateapi.ListOptions{PageSize: 100}
	for pageNumber := 0; ; pageNumber++ {
		page, err := c.control.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: c.atespace, Options: options})
		if err != nil {
			return err
		}
		for _, actor := range page.GetActors() {
			source := actor.GetSourceTag()
			if source.GetAtespace() == c.atespace && source.GetName() == expected.TagName {
				return status.Error(codes.FailedPrecondition, "snapshot GC Tag is borrowed by an Actor")
			}
		}
		if page.GetNextPageToken() == "" {
			break
		}
		if pageNumber == 15 || page.GetNextPageToken() == options.PageToken {
			return status.Error(codes.ResourceExhausted, "snapshot GC Actor inventory exceeds bounded scan")
		}
		options.PageToken = page.GetNextPageToken()
	}
	_, err = c.control.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: c.ref(expected.TagName), Options: &ateapi.DeleteOptions{Uid: current.TagUID, Version: current.TagVersion}})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}
