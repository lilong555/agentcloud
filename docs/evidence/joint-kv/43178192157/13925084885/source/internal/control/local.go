package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// LocalBackend provides an explicit local-process development environment.
// The Substrate backend supplies process isolation and real runtime snapshots.
type LocalBackend struct {
	Root       string
	AllowShell bool
}

func (b *LocalBackend) Name() string { return "local-process" }
func (b *LocalBackend) workspace(s *Session) string {
	return filepath.Join(b.Root, "workspaces", s.TenantID, s.WorkspaceID)
}
func (b *LocalBackend) Ensure(ctx context.Context, s *Session) (Actor, error) {
	if err := ctx.Err(); err != nil {
		return Actor{}, err
	}
	if err := os.MkdirAll(b.workspace(s), 0700); err != nil {
		return Actor{}, err
	}
	return Actor{Name: s.SessionID, UID: newID(), Version: 1, ActualFidelity: "VOLUMES", BootID: newID()}, nil
}

type boundedBuffer struct {
	mu       sync.Mutex
	data     []byte
	maximum  int
	exceeded bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.maximum - len(b.data)
	if len(p) > remaining {
		b.data = append(b.data, p[:remaining]...)
		b.exceeded = true
		return len(p), nil
	}
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *boundedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return string(b.data) }

func (b *LocalBackend) Tool(ctx context.Context, s *Session, input ToolRequest) (ToolResult, error) {
	root, err := os.OpenRoot(b.workspace(s))
	if err != nil {
		return ToolResult{}, err
	}
	defer root.Close()
	if input.Kind == "file_read" || input.Kind == "file_write" {
		if !filepath.IsLocal(input.Path) || input.Path == "." {
			return ToolResult{}, errors.New("a relative file path is required")
		}
		if input.Kind == "file_read" {
			file, err := root.Open(input.Path)
			if err != nil {
				return ToolResult{}, err
			}
			defer file.Close()
			data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
			if len(data) > 1<<20 {
				return ToolResult{}, errors.New("file exceeds 1 MiB")
			}
			return ToolResult{Content: string(data)}, err
		}
		if len(input.Content) > 1<<20 {
			return ToolResult{}, errors.New("file exceeds 1 MiB")
		}
		if err := root.MkdirAll(filepath.Dir(input.Path), 0700); err != nil {
			return ToolResult{}, err
		}
		temporary := filepath.Join(filepath.Dir(input.Path), ".tool-"+newID())
		file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return ToolResult{}, err
		}
		defer root.Remove(temporary)
		_, err = file.WriteString(input.Content)
		if err == nil {
			err = file.Sync()
		}
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = root.Rename(temporary, input.Path)
		}
		if err == nil {
			dir, e := root.Open(filepath.Dir(input.Path))
			if e != nil {
				return ToolResult{}, e
			}
			err = dir.Sync()
			dir.Close()
		}
		return ToolResult{Content: input.Content}, err
	}
	if !b.AllowShell {
		return ToolResult{}, errors.New("local shell is disabled; select --allow-local-shell for development")
	}
	if input.Command == "" || len(input.Command) > 65536 {
		return ToolResult{}, errors.New("command is required and must fit 64 KiB")
	}
	command := exec.CommandContext(ctx, "/bin/sh", "-c", input.Command)
	command.Dir = b.workspace(s)
	command.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + b.workspace(s), "LANG=C.UTF-8", "PYTHONUNBUFFERED=1"}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, stderr := &boundedBuffer{maximum: 1 << 20}, &boundedBuffer{maximum: 1 << 20}
	command.Stdout, command.Stderr = stdout, stderr
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	command.WaitDelay = 2e9
	err = command.Run()
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	result := ToolResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if command.ProcessState != nil {
		result.ExitCode = command.ProcessState.ExitCode()
	}
	if ctx.Err() != nil {
		return result, fmt.Errorf("tool deadline or shutdown: %w", ctx.Err())
	}
	if stdout.exceeded || stderr.exceeded {
		return result, errors.New("tool output exceeded retained limit")
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return result, nil
	}
	return result, err
}

func copyTree(ctx context.Context, source, destination string) (string, error) {
	if err := os.MkdirAll(destination, 0700); err != nil {
		return "", err
	}
	digest := sha256.New()
	var size int64
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("snapshots require symlink-free workspaces")
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("snapshots require regular files")
		}
		size += info.Size()
		if size > 64<<20 {
			return errors.New("local snapshot exceeds 64 MiB")
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm()&0700)
		if err != nil {
			return err
		}
		fmt.Fprintf(digest, "%s\x00%d\x00", relative, info.Size())
		_, err = io.Copy(io.MultiWriter(output, digest), file)
		if err == nil {
			err = output.Sync()
		}
		if e := output.Close(); err == nil {
			err = e
		}
		return err
	})
	if err != nil {
		return "", err
	}
	err = syncTree(destination)
	return hex.EncodeToString(digest.Sum(nil)), err
}
func syncTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		defer dir.Close()
		return dir.Sync()
	})
}
func (b *LocalBackend) Checkpoint(ctx context.Context, s *Session, operation string) (Actor, string, error) {
	actor := s.Actor
	id := newID()
	parent := filepath.Join(b.Root, "snapshots", s.TenantID, s.WorkspaceID)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return actor, "", err
	}
	temporary, err := os.MkdirTemp(parent, ".snapshot-")
	if err != nil {
		return actor, "", err
	}
	defer os.RemoveAll(temporary)
	digest, err := copyTree(ctx, b.workspace(s), temporary)
	if err != nil {
		return actor, "", err
	}
	if err = os.Rename(temporary, filepath.Join(parent, id)); err != nil {
		return actor, "", err
	}
	dir, err := os.Open(parent)
	if err != nil {
		return actor, "", err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return actor, "", err
	}
	actor.SnapshotUUID = id
	actor.SnapshotGeneration++
	actor.Version++
	actor.ActualFidelity = "VOLUMES"
	return actor, digest, nil
}
func (b *LocalBackend) Suspend(ctx context.Context, s *Session) (Actor, error) {
	if err := ctx.Err(); err != nil {
		return Actor{}, err
	}
	actor := s.Actor
	actor.Version++
	return actor, nil
}
func (b *LocalBackend) Resume(ctx context.Context, s *Session) (Actor, error) {
	actor := s.Actor
	if !validID(actor.SnapshotUUID) || strings.Contains(actor.SnapshotUUID, "..") {
		return actor, errors.New("snapshot identity is invalid")
	}
	source := filepath.Join(b.Root, "snapshots", s.TenantID, s.WorkspaceID, actor.SnapshotUUID)
	workspace := b.workspace(s)
	temporary, err := os.MkdirTemp(filepath.Dir(workspace), ".restore-")
	if err != nil {
		return actor, err
	}
	defer os.RemoveAll(temporary)
	digest, err := copyTree(ctx, source, temporary)
	if err != nil {
		return actor, err
	}
	if s.Manifest == nil || digest != s.Manifest.WorkspaceDigest {
		return actor, errors.New("snapshot digest does not match committed manifest")
	}
	backup := workspace + ".previous-" + newID()
	if err = os.Rename(workspace, backup); err != nil {
		return actor, err
	}
	if err = os.Rename(temporary, workspace); err != nil {
		_ = os.Rename(backup, workspace)
		return actor, err
	}
	defer os.RemoveAll(backup)
	dir, err := os.Open(filepath.Dir(workspace))
	if err != nil {
		return actor, err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return actor, err
	}
	actor.Version++
	actor.BootID = newID()
	actor.ActualFidelity = "VOLUMES"
	return actor, nil
}
