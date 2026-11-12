// cleanup releases only explicitly recorded experiment objects, with UID fencing.
package main

import (
	"context"
	"encoding/json"
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

type Object struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

func run() error {
	dir := flag.String("client-dir", "artifacts/substrate/client", "private credentials")
	input := flag.String("objects", "", "JSON array of exact actor/tag names and observed UIDs")
	flag.Parse()
	data, err := os.ReadFile(*input)
	if err != nil {
		return err
	}
	var objects []Object
	if err = json.Unmarshal(data, &objects); err != nil {
		return err
	}
	for _, item := range objects {
		if item.Name == "" || item.UID == "" || strings.ContainsAny(item.Name, "/\\") || (item.Kind != "actor" && item.Kind != "tag" && item.Kind != "template") {
			return fmt.Errorf("every object requires an exact name, UID and actor/tag/template kind")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client, err := substrate.New(ctx, substrate.Config{Address: "127.0.0.1:9443", Atespace: "agentcloud", ServerName: "api.ate-system.svc", CAFile: filepath.Join(*dir, "ca.pem"), TokenFile: filepath.Join(*dir, "token")})
	if err != nil {
		return err
	}
	defer client.Close()
	encoder := json.NewEncoder(os.Stdout)
	for _, item := range objects {
		ref := &ateapi.ObjectRef{Atespace: "agentcloud", Name: item.Name}
		options := &ateapi.DeleteOptions{Uid: item.UID}
		if item.Kind == "actor" {
			_, err = client.Control().DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref, Options: options})
		} else if item.Kind == "tag" {
			_, err = client.Control().DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: ref, Options: options})
		} else {
			_, err = client.Control().DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref, Options: options})
		}
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		if err = encoder.Encode(map[string]any{"object": item, "deleted_or_absent": true}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
