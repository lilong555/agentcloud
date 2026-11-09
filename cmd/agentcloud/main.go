package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lilong555/agentcloud/internal/policy"
	"github.com/lilong555/agentcloud/internal/signals"
)

type query struct {
	TenantID       string               `json:"tenant_id"`
	WorkspaceID    string               `json:"workspace_id"`
	SessionID      string               `json:"session_id"`
	ProgramStatus  policy.ProgramStatus `json:"program_status"`
	BaselineTokens uint64               `json:"baseline_tokens"`
	// BaselineWeight is the upstream static Acting weight. Reasoning uses 1.
	BaselineWeight float64 `json:"baseline_weight"`
}

type record struct {
	Type    string                      `json:"type"`
	At      time.Time                   `json:"at"`
	Binding *signals.Binding            `json:"binding,omitempty"`
	Update  *signals.SandboxStateUpdate `json:"update,omitempty"`
	Query   *query                      `json:"query,omitempty"`
}

type outcome struct {
	Line     int              `json:"line"`
	Type     string           `json:"type"`
	At       time.Time        `json:"at"`
	Accepted bool             `json:"accepted"`
	Reason   string           `json:"reason"`
	Detail   string           `json:"detail,omitempty"`
	Decision *policy.Decision `json:"decision,omitempty"`
}

type summary struct {
	Records               int `json:"records"`
	RegistrationsAccepted int `json:"registrations_accepted"`
	UpdatesAccepted       int `json:"updates_accepted"`
	UpdatesRejected       int `json:"updates_rejected"`
	EvaluationsAccepted   int `json:"evaluations_accepted"`
	RecordsRejected       int `json:"records_rejected"`
}

type report struct {
	SchemaVersion      int            `json:"schema_version"`
	ExecutionKind      string         `json:"execution_kind"`
	UpstreamExecution  bool           `json:"upstream_execution"`
	TrustSource        string         `json:"trust_source"`
	Input              string         `json:"input"`
	SignalConfig       signals.Config `json:"signal_config"`
	PolicyConfig       policy.Config  `json:"policy_config"`
	FutureSkewMS       int64          `json:"future_skew_ms"`
	RecoveryHoldMS     int64          `json:"recovery_hold_ms"`
	MinimumResidenceMS int64          `json:"minimum_residence_ms"`
	Summary            summary        `json:"summary"`
	Outcomes           []outcome      `json:"outcomes"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "serve" {
		return serve(args[1:], stdout, stderr)
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: agentcloud serve [flags] | replay --input TRACE.jsonl [--output REPORT.json]")
	}
	if args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		_, err := fmt.Fprintln(stdout, "agentcloud serve [flags]                  Durable HTTP coordinator\nagentcloud replay --input TRACE.jsonl [--output REPORT.json]\nJSONL types: register, update, evaluate. Execution: CPU contract-replay.")
		return err
	}
	if args[0] != "replay" {
		return fmt.Errorf("unknown command %q", args[0])
	}
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "explicit trusted JSONL fixture")
	output := flags.String("output", "-", "JSON decision report; - writes stdout")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *input == "" || flags.NArg() != 0 {
		return fmt.Errorf("replay requires --input and accepts no positional arguments")
	}
	if *output != "-" {
		inputPath, err := filepath.Abs(*input)
		if err != nil {
			return err
		}
		outputPath, err := filepath.Abs(*output)
		if err != nil {
			return err
		}
		if inputPath == outputPath {
			return fmt.Errorf("input and output must use separate paths")
		}
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := replay(f, *input)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if *output == "-" {
		_, err = stdout.Write(data)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(*output), ".agentcloud-report-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, *output)
}

func replay(input io.Reader, name string) (report, error) {
	signalConfig, policyConfig := signals.DefaultConfig(), policy.DefaultConfig()
	tracker, err := signals.NewTracker(signalConfig)
	if err != nil {
		return report{}, err
	}
	schedulerPolicy, err := policy.New(policyConfig)
	if err != nil {
		return report{}, err
	}
	r := report{SchemaVersion: 1, ExecutionKind: "contract-replay", UpstreamExecution: false, TrustSource: "explicit_local_fixture", Input: name, SignalConfig: signalConfig, PolicyConfig: policyConfig, FutureSkewMS: signalConfig.FutureSkew.Milliseconds(), RecoveryHoldMS: signalConfig.RecoveryHold.Milliseconds(), MinimumResidenceMS: policyConfig.MinimumResidence.Milliseconds(), Outcomes: []outcome{}}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var previous time.Time
	line := 0
	for scanner.Scan() {
		line++
		data := bytes.TrimSpace(scanner.Bytes())
		if len(data) == 0 {
			continue
		}
		var item record
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&item); err != nil {
			return report{}, fmt.Errorf("line %d: %w", line, err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return report{}, fmt.Errorf("line %d: expected exactly one JSON object", line)
		}
		if item.At.IsZero() || item.At.Before(previous) {
			return report{}, fmt.Errorf("line %d: at must be present and monotonically nondecreasing", line)
		}
		previous = item.At
		result := outcome{Line: line, Type: item.Type, At: item.At, Accepted: true, Reason: "accepted"}
		switch item.Type {
		case "register":
			if item.Binding == nil || item.Update != nil || item.Query != nil {
				return report{}, fmt.Errorf("line %d: register requires only binding", line)
			}
			err = tracker.Register(*item.Binding)
			if err == nil {
				r.Summary.RegistrationsAccepted++
			}
		case "update":
			if item.Update == nil || item.Binding != nil || item.Query != nil {
				return report{}, fmt.Errorf("line %d: update requires only update", line)
			}
			err = tracker.Accept(*item.Update, item.At)
			if err == nil {
				r.Summary.UpdatesAccepted++
			} else {
				r.Summary.UpdatesRejected++
			}
		case "evaluate":
			if item.Query == nil || item.Binding != nil || item.Update != nil {
				return report{}, fmt.Errorf("line %d: evaluate requires only query", line)
			}
			q := item.Query
			var view signals.View
			view, err = tracker.View(signals.Key{TenantID: q.TenantID, WorkspaceID: q.WorkspaceID, SessionID: q.SessionID}, item.At)
			if err == nil {
				var decision policy.Decision
				decision, err = schedulerPolicy.Decide(view, item.At, q.ProgramStatus, q.BaselineTokens, q.BaselineWeight)
				if err == nil {
					result.Decision = &decision
					r.Summary.EvaluationsAccepted++
				}
			}
		default:
			return report{}, fmt.Errorf("line %d: unknown record type %q", line, item.Type)
		}
		if err != nil {
			result.Accepted, result.Reason, result.Detail = false, signals.Code(err), err.Error()
			r.Summary.RecordsRejected++
		}
		r.Summary.Records++
		r.Outcomes = append(r.Outcomes, result)
	}
	if err := scanner.Err(); err != nil {
		return report{}, err
	}
	if r.Summary.Records == 0 {
		return report{}, fmt.Errorf("input trace is empty")
	}
	return r, nil
}
