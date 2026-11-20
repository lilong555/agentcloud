package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lilong555/agentcloud/internal/journal"
)

func researchExport(args []string, stdout, stderr io.Writer) error {
	f := flag.NewFlagSet("research-export", flag.ContinueOnError)
	f.SetOutput(stderr)
	runID := f.String("run-id", "", "captured run identity")
	output := f.String("output", "", "new private JSONL export path")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *runID == "" || *output == "" || f.NArg() != 0 {
		return errors.New("research-export requires --run-id and --output")
	}
	dsn := os.Getenv("AGENTCLOUD_DATABASE_URL")
	if dsn == "" {
		return errors.New("set AGENTCLOUD_DATABASE_URL")
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(*output), ".research-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err = journal.ExportResearch(ctx, dsn, *runID, file); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	// Exclusive link avoids replacing a previously sealed export.
	if err = os.Link(file.Name(), *output); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(*output))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
