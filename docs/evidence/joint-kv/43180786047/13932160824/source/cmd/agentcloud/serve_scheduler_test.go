package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestServeSchedulerShadowRequiresBothControls(t *testing.T) {
	for _, args := range [][]string{
		{"serve", "--scheduler-shadow"},
		{"serve", "--scheduler-shadow", "--enable-autonomous-scheduler"},
		{"serve", "--scheduler-shadow", "--enable-experiment-controls"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "scheduler shadow requires") {
			t.Fatalf("args=%v error=%v", args, err)
		}
	}
}

func TestServeHelpAdvertisesV2AndShadow(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"serve", "--help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	help := stdout.String() + stderr.String()
	if !strings.Contains(help, "agentcloud-v2") || !strings.Contains(help, "scheduler-shadow") {
		t.Fatalf("scheduler flags missing: %s", help)
	}
}
