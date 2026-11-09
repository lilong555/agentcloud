package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lilong555/agentcloud/internal/adapters/substrate"
	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	clientDir := flag.String("client-dir", "artifacts/substrate/client", "private client credentials")
	spec := flag.String("template", "artifacts/substrate/runtime-spec/actor-template.json", "private ActorTemplate")
	actorName := flag.String("actor", "", "existing actor requiring coordinator egress")
	tagName := flag.String("tag", "", "inspect retained snapshot identity and durability")
	workers := flag.Bool("workers", false, "inspect registered worker capacity")
	replaceTemplate := flag.Bool("replace-template", false, "recreate the task template with UID and version fencing")
	flag.Parse()
	if err := run(*clientDir, *spec, *actorName, *tagName, *workers, *replaceTemplate); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(clientDir, spec, actorName, tagName string, workers, replaceTemplate bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := substrate.New(ctx, substrate.Config{Address: "127.0.0.1:9443", Atespace: "agentcloud", ServerName: "api.ate-system.svc", CAFile: filepath.Join(clientDir, "ca.pem"), TokenFile: filepath.Join(clientDir, "token")})
	if err != nil {
		return err
	}
	defer client.Close()
	control := client.Control()
	if tagName != "" {
		tag, err := control.GetTag(ctx, &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "agentcloud", Name: tagName}})
		if err != nil {
			return err
		}
		data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(tag)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	if workers {
		result, err := control.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
		if err != nil {
			return err
		}
		data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(result)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	if actorName != "" {
		actor, err := client.GetActor(ctx, actorName)
		if err != nil {
			return err
		}
		if err := client.AllowCoordinatorEgress(ctx, actorName, "coordinator.agentcloud.test", 8092); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"actor": actor, "coordinator_egress": "coordinator.agentcloud.test:8092"})
	}
	_, err = control.GetAtespace(ctx, &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "agentcloud"}})
	if status.Code(err) == codes.NotFound {
		_, err = control.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "agentcloud"}}})
	}
	if err != nil {
		return err
	}
	data, err := os.ReadFile(spec)
	if err != nil {
		return err
	}
	template := &ateapi.ActorTemplate{}
	if err := protojson.Unmarshal(data, template); err != nil {
		return err
	}
	_, err = control.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: template})
	if status.Code(err) == codes.AlreadyExists && replaceTemplate {
		stored, readErr := control.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: template.Metadata.Atespace, Name: template.Metadata.Name}})
		if readErr != nil {
			return readErr
		}
		_, err = control.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: stored.Metadata.Atespace, Name: stored.Metadata.Name}, Options: &ateapi.DeleteOptions{Uid: stored.Metadata.Uid, Version: stored.Metadata.Version}})
		if err != nil {
			return err
		}
		_, err = control.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: template})
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"atespace": "agentcloud", "template": template.Metadata.Name, "image": template.Containers[0].Image})
}
