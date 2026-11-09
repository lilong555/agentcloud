// probe exercises native Substrate preconditions against an explicitly owned Actor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func run() error {
	dir := flag.String("client-dir", "artifacts/substrate/client", "private API credentials")
	action := flag.String("action", "inspect", "inspect, stale-suspend, wrong-snapshot, pending-tag, replace-tag, rootfs-template")
	name := flag.String("actor", "", "explicit experiment Actor")
	uid := flag.String("uid", "", "observed Actor UID")
	version := flag.Int64("version", 0, "observed historical resource version")
	tagName := flag.String("tag", "", "explicit experiment Tag")
	templateName := flag.String("template", "agentcloud-matrix", "source ActorTemplate")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := substrate.New(ctx, substrate.Config{Address: "127.0.0.1:9443", Atespace: "agentcloud", ServerName: "api.ate-system.svc", CAFile: filepath.Join(*dir, "ca.pem"), TokenFile: filepath.Join(*dir, "token")})
	if err != nil {
		return err
	}
	defer c.Close()
	ref := func(n string) *ateapi.ObjectRef { return &ateapi.ObjectRef{Atespace: "agentcloud", Name: n} }
	result := map[string]any{"action": *action}
	if *action == "rootfs-template" {
		if !strings.HasPrefix(*name, "agentcloud-fault-") {
			return errors.New("fault template requires the experiment name prefix")
		}
		template, e := c.Control().GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref(*templateName)})
		if e != nil {
			return e
		}
		template.Metadata = &ateapi.ResourceMetadata{Atespace: "agentcloud", Name: *name}
		template.Status = nil
		template.SnapshotConfig.PreferredFidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		created, e := c.Control().CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: template})
		if e != nil {
			return e
		}
		result["object"] = map[string]string{"kind": "template", "name": *name, "uid": created.Metadata.Uid}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	actor, err := c.GetActor(ctx, *name)
	if err != nil {
		return err
	}
	if *uid != "" && actor.UID != *uid {
		return errors.New("Actor incarnation changed before injection")
	}
	result["actor"] = actor
	switch *action {
	case "inspect":
	case "stale-suspend":
		_, e := c.Suspend(ctx, *name, *uid, *version)
		if status.Code(e) != codes.Aborted && status.Code(e) != codes.FailedPrecondition {
			return fmt.Errorf("expected fenced suspension, got %v", e)
		}
		result["grpc_code"] = status.Code(e).String()
	case "wrong-snapshot":
		_, e := c.AwaitDurable(ctx, *name, actor.UID, "00000000-0000-4000-8000-000000000011", actor.SnapshotGeneration+1)
		if status.Code(e) != codes.FailedPrecondition && status.Code(e) != codes.NotFound {
			return fmt.Errorf("expected snapshot identity rejection, got %v", e)
		}
		result["grpc_code"] = status.Code(e).String()
	case "pending-tag":
		if actor.State != "SUSPENDED" {
			actor, err = c.Suspend(ctx, *name, actor.UID, actor.Version)
			if err != nil {
				return err
			}
		}
		if actor.Durable {
			return errors.New("injection did not produce a PENDING snapshot")
		}
		_, e := c.RetainSnapshot(ctx, actor.Name, actor.UID, actor.SnapshotUUID, actor.SnapshotGeneration, *tagName)
		if status.Code(e) != codes.FailedPrecondition {
			return fmt.Errorf("expected PENDING tag rejection, got %v", e)
		}
		_, e = c.Control().GetTag(ctx, &ateapi.GetTagRequest{Tag: ref(*tagName)})
		if status.Code(e) != codes.NotFound {
			return fmt.Errorf("PENDING snapshot created a retained tag: %v", e)
		}
		result["pending_actor"] = actor
		result["retained_tag_absent"] = true
	case "replace-tag":
		prior, e := c.Control().GetTag(ctx, &ateapi.GetTagRequest{Tag: ref(*tagName)})
		if e != nil {
			return e
		}
		_, e = c.Control().DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: ref(*tagName), Options: &ateapi.DeleteOptions{Uid: prior.Metadata.Uid, Version: prior.Metadata.Version}})
		if e != nil {
			return e
		}
		for {
			_, e = c.Control().GetTag(ctx, &ateapi.GetTagRequest{Tag: ref(*tagName)})
			if status.Code(e) == codes.NotFound {
				break
			}
			if e != nil {
				return e
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		replacement, e := c.RetainSnapshot(ctx, actor.Name, actor.UID, actor.SnapshotUUID, actor.SnapshotGeneration, *tagName)
		if e != nil {
			return e
		}
		if replacement.TagUID == prior.Metadata.Uid {
			return errors.New("tag replacement reused its UID")
		}
		result["old_uid"] = prior.Metadata.Uid
		result["object"] = map[string]string{"kind": "tag", "name": replacement.TagName, "uid": replacement.TagUID}
	default:
		return errors.New("unknown fault probe action")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
