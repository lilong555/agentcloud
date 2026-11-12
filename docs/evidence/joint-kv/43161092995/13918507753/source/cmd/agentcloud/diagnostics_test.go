package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lilong555/agentcloud/internal/control"
)

func TestStatusAndWorkspaceTraceCLI(t *testing.T) {
	dir := t.TempDir()
	c, err := control.New(context.Background(), control.Config{DataDir: dir, Backend: &control.LocalBackend{Root: dir}, Tokens: map[string]string{"private-token": "tenant"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Create(context.Background(), "tenant", control.CreateSession{SessionID: "session", WorkspaceID: "workspace"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	for _, command := range [][]string{{"status", "--url", server.URL, "--token", "private-token", "--json"}, {"trace", "workspace", "--url", server.URL, "--token", "private-token"}, {"trace", "--url", server.URL, "--token", "private-token", "--json", "workspace"}} {
		var out, stderr bytes.Buffer
		if err := run(command, &out, &stderr); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "workspace") || strings.Contains(out.String(), "private-token") {
			t.Fatalf("diagnostic %s", out.String())
		}
		if strings.Join(command, " ") == strings.Join([]string{"status", "--url", server.URL, "--token", "private-token", "--json"}, " ") && !json.Valid(out.Bytes()) {
			t.Fatal("invalid JSON")
		}
	}
	var out, stderr bytes.Buffer
	if err := run([]string{"trace", "workspace", "--url", server.URL, "--token", "another-token"}, &out, &stderr); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unauthorized result %v", err)
	}
}
