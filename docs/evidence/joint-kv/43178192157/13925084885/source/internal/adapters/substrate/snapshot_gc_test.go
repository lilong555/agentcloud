package substrate

import (
	"context"
	"fmt"
	"testing"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type snapshotGCControl struct {
	*testControl
	deleteRequest *ateapi.DeleteTagRequest
	deleteError   error
	listError     error
	borrower      *ateapi.Actor
	pages         int
	listCalls     int
	reads         int
	replaceOnRead bool
}

func (s *snapshotGCControl) GetTag(ctx context.Context, request *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	s.reads++
	if s.replaceOnRead && s.reads > 1 {
		tag := proto.Clone(s.tag).(*ateapi.Tag)
		tag.Metadata.Uid = "31c9a4cc-6b3a-4302-8f4a-b659e10e4776"
		return tag, nil
	}
	return s.testControl.GetTag(ctx, request)
}

func (s *snapshotGCControl) ListActors(_ context.Context, request *ateapi.ListActorsRequest) (*ateapi.ListActorsResponse, error) {
	s.listCalls++
	if s.listError != nil {
		return nil, s.listError
	}
	if request.Atespace != "team-a" || request.Options.PageSize != 100 {
		return nil, status.Error(codes.InvalidArgument, "unexpected inventory scope")
	}
	page := &ateapi.ListActorsResponse{}
	if s.borrower != nil {
		page.Actors = []*ateapi.Actor{s.borrower}
	}
	if s.listCalls < s.pages {
		page.NextPageToken = fmt.Sprintf("page-%d", s.listCalls)
	}
	return page, nil
}

func (s *snapshotGCControl) DeleteTag(_ context.Context, request *ateapi.DeleteTagRequest) (*ateapi.Tag, error) {
	s.deleteRequest = request
	if request.Options.Uid != testTagUID || request.Options.Version != s.tag.Metadata.Version {
		return nil, status.Error(codes.FailedPrecondition, "DELETE fence differs")
	}
	if s.deleteError != nil {
		s.tag.Metadata.Version++
		s.tag.Metadata.DeleteTime = timestamppb.Now()
		return nil, s.deleteError
	}
	tag := s.tag
	s.tag = nil
	return tag, nil
}

func gcAdapterFixture() *snapshotGCControl {
	return &snapshotGCControl{testControl: &testControl{tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "retained", Uid: testTagUID, Version: 7, CreateTime: timestamppb.New(time.Now().Add(-2 * time.Hour))},
		SourceActor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor"},
		Status: &ateapi.TagStatus{SourceActorUid: testActorUID, SourceSnapshotUuid: testSnapshotUID, SourceSnapshotGeneration: 3,
			Snapshot: &ateapi.Snapshot{Uuid: testTagUID, Generation: 1, DurableSnapshot: &ateapi.SnapshotStorage{State: ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
				Object: &ateapi.ObjectSnapshot{SnapshotUri: "s3://snapshot/retained"}}}},
	}}}
}

func TestSnapshotGCAdapterFencesUIDVersionAndResumesStorageDeletion(t *testing.T) {
	server := gcAdapterFixture()
	client := testClient(t, server)
	ctx := context.Background()
	tag, err := client.SnapshotTag(ctx, "retained")
	if err != nil || tag.TagUID != testTagUID || tag.SourceActorName != "actor" || tag.CreatedAt.IsZero() || !tag.Durable {
		t.Fatalf("Tag observation=%+v %v", tag, err)
	}
	server.deleteError = status.Error(codes.Unavailable, "object storage release failed")
	if err := client.DeleteSnapshotTag(ctx, tag); status.Code(err) != codes.Unavailable {
		t.Fatalf("DELETE=%v", err)
	}
	if server.deleteRequest.Options.Uid != testTagUID || server.deleteRequest.Options.Version != 7 {
		t.Fatalf("DELETE fence=%v", server.deleteRequest)
	}
	server.deleteError = nil
	if err := client.DeleteSnapshotTag(ctx, tag); err != nil {
		t.Fatal(err)
	}
	if server.deleteRequest.Options.Version != 8 {
		t.Fatal("retry reused stale resource version")
	}
	if err := client.DeleteSnapshotTag(ctx, tag); err != nil {
		t.Fatal("absent Tag failed idempotent reconciliation", err)
	}
}

func TestSnapshotGCAdapterPreservesForeignIdentityAndBorrowedTags(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*snapshotGCControl, *SnapshotTag)
		want   codes.Code
	}{
		{name: "UID-replacement", mutate: func(s *snapshotGCControl, _ *SnapshotTag) { s.replaceOnRead = true }, want: codes.FailedPrecondition},
		{name: "foreign-owner", mutate: func(_ *snapshotGCControl, tag *SnapshotTag) { tag.SourceActorUID = "foreign-owner" }, want: codes.FailedPrecondition},
		{name: "different-source-snapshot", mutate: func(_ *snapshotGCControl, tag *SnapshotTag) { tag.SourceSnapshotGeneration++ }, want: codes.FailedPrecondition},
		{name: "borrowed-by-running-Actor", mutate: func(s *snapshotGCControl, _ *SnapshotTag) {
			s.borrower = &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: "clone"}, SourceTag: &ateapi.ObjectRef{Atespace: "team-a", Name: "retained"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING}}
		}, want: codes.FailedPrecondition},
		{name: "inventory-unavailable", mutate: func(s *snapshotGCControl, _ *SnapshotTag) {
			s.listError = status.Error(codes.Unavailable, "inventory unavailable")
		}, want: codes.Unavailable},
		{name: "inventory-bounded", mutate: func(s *snapshotGCControl, _ *SnapshotTag) { s.pages = 100 }, want: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := gcAdapterFixture()
			client := testClient(t, server)
			tag, err := client.SnapshotTag(context.Background(), "retained")
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(server, &tag)
			if err := client.DeleteSnapshotTag(context.Background(), tag); status.Code(err) != test.want {
				t.Fatalf("DELETE error=%v want=%s", err, test.want)
			}
			if server.deleteRequest != nil {
				t.Fatalf("unsafe DELETE sent: %v", server.deleteRequest)
			}
			if server.listCalls > 16 {
				t.Fatalf("unbounded inventory calls=%d", server.listCalls)
			}
		})
	}
}

func TestSnapshotGCAdapterRequiresExactAtespace(t *testing.T) {
	for _, mutate := range []func(*ateapi.Tag){
		func(tag *ateapi.Tag) { tag.Metadata.Atespace = "other" },
		func(tag *ateapi.Tag) { tag.SourceActor.Atespace = "other" },
		func(tag *ateapi.Tag) { tag.Metadata.Name = "different" },
	} {
		server := gcAdapterFixture()
		mutate(server.tag)
		client := testClient(t, server)
		if _, err := client.SnapshotTag(context.Background(), "retained"); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("foreign Tag accepted: %v", err)
		}
		if server.deleteRequest != nil {
			t.Fatal("foreign Tag deleted")
		}
	}
}
