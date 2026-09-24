package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// newTestStore opens a fresh store in a temp directory and registers
// cleanup. Every test therefore runs without network or external state.
func newTestStore(t *testing.T, opts Options) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(ctx, dir, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	})
	return s, dir
}

// digestOf mirrors the content address used by the store.
func digestOf(p []byte) string {
	sum := sha256.Sum256(p)
	return digestPrefix + hex.EncodeToString(sum[:])
}

// putBytes is a convenience wrapper around Put for []byte.
func putBytes(t *testing.T, s *Store, name string, p []byte) VersionInfo {
	t.Helper()
	vi, err := s.Put(context.Background(), name, bytes.NewReader(p))
	if err != nil {
		t.Fatalf("Put(%q, %d bytes): %v", name, len(p), err)
	}
	return vi
}

// readAll reads a full object version through an already-open reader.
func readAll(t *testing.T, rc io.ReadCloser, err error) []byte {
	t.Helper()
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer rc.Close()
	out, rerr := io.ReadAll(rc)
	if rerr != nil {
		t.Fatalf("read body: %v", rerr)
	}
	return out
}

// getBody reads the current version's full body.
func getBody(t *testing.T, s *Store, name string) []byte {
	t.Helper()
	rc, _, err := s.Get(context.Background(), name)
	return readAll(t, rc, err)
}

// getVersionBody reads a specific version's full body.
func getVersionBody(t *testing.T, s *Store, name string, v int64) []byte {
	t.Helper()
	rc, _, err := s.GetVersion(context.Background(), name, v)
	return readAll(t, rc, err)
}

// openBody opens the current version and returns its reader without
// consuming it (for corruption tests that read deliberately).
func openBody(t *testing.T, s *Store, name string) io.ReadCloser {
	t.Helper()
	rc, _, err := s.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("open %q: %v", name, err)
	}
	return rc
}

// blobFileCount counts committed blob files on disk (excluding tmp).
func blobFileCount(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	root := filepath.Join(dir, blobsDirName)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk blobs: %v", err)
	}
	return n
}

// stagingCount counts leftover staging files.
func stagingCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, tmpDirName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read tmp: %v", err)
	}
	return len(entries)
}

// osWriteByteFlip flips one bit in the first data byte of a blob file,
// simulating silent on-disk corruption for read-verification tests.
func osWriteByteFlip(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return fmt.Errorf("cannot flip empty file")
	}
	b[0] ^= 0xFF
	return os.WriteFile(path, b, 0o644)
}

// writeStagingLeftover drops a file into tmp/ to simulate an interrupted
// staged write that never reached a content address.
func writeStagingLeftover(dir, name string, data []byte) error {
	return os.WriteFile(filepath.Join(dir, tmpDirName, name), data, 0o600)
}

// writeManifestLeftover drops a file into the store root that looks like
// an interrupted atomic manifest commit.
func writeManifestLeftover(dir, name string, data []byte) error {
	return os.WriteFile(filepath.Join(dir, name), data, 0o600)
}

// countManifestTemp counts leftover .manifest-*.tmp files in the root.
func countManifestTemp(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	n := 0
	for _, e := range entries {
		if isManifestTemp(e.Name()) {
			n++
		}
	}
	return n
}

// stringsReader returns an io.Reader for a literal string.
func stringsReader(s string) io.Reader { return strings.NewReader(s) }

// atomicCAS flips a 0/1 int32 exactly once and reports whether it did.
func atomicCAS(p *int32) bool { return atomic.CompareAndSwapInt32(p, 0, 1) }
