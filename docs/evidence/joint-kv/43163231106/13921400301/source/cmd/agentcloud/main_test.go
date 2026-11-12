package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayDemo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"replay", "--input", "../../fixtures/signals/demo.jsonl"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(stdout.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.ExecutionKind != "contract-replay" || r.UpstreamExecution || r.Summary.Records != 17 || r.Summary.UpdatesRejected != 2 || r.Summary.EvaluationsAccepted != 9 {
		t.Fatalf("report metadata/summary: %+v", r.Summary)
	}
	want := []string{"no_signal", "state_debounce", "suspended_acting", "reasoning_preserved", "resuming_baseline", "recovery_hold", "parked_acting", "ttl_expired", "heavy_tool_acting"}
	var got []string
	for _, entry := range r.Outcomes {
		if entry.Decision != nil {
			got = append(got, entry.Decision.Reason)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("decision reasons = %v", got)
	}
}

func TestReplayRejectsMalformedAndBackwardTrace(t *testing.T) {
	for _, input := range []string{
		`{"type":"register","at":"2026-11-09T00:00:00Z","extra":true}`,
		`{"type":"register","at":"2026-11-09T00:00:00Z"} {}`,
		`{"type":"register","at":"2026-11-09T00:00:00Z"}`,
		`{"type":"unknown","at":"2026-11-09T00:00:00Z"}`,
		"",
	} {
		if _, err := replay(strings.NewReader(input), "input"); err == nil {
			t.Fatalf("accepted malformed trace %q", input)
		}
	}
	input := `{"type":"register","at":"2026-11-09T00:00:01Z","binding":{"source_id":"source","tenant_id":"tenant","workspace_id":"workspace","session_id":"session","actor_uid":"actor","epoch":1}}` + "\n" + `{"type":"evaluate","at":"2026-11-09T00:00:00Z","query":{}}`
	if _, err := replay(strings.NewReader(input), "input"); err == nil {
		t.Fatal("accepted backwards trace")
	}
}

func TestCLIProducesAtomicReport(t *testing.T) {
	output := filepath.Join(t.TempDir(), "nested", "report.json")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"replay", "--input", "../../fixtures/signals/demo.jsonl", "--output", output}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) || stdout.Len() != 0 {
		t.Fatal("invalid file report")
	}
}
