package objectstore

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRetainIdempotentNoDiskSideEffect proves the extend-only rule is a
// true no-op on disk, not just an in-memory one: repeating an identical
// hold and issuing a weaker request perform no manifest commit at all.
//
// Two independent checks back this claim:
//
//   - manifest.json's mtime/content must not change across the no-ops;
//   - with the store directory made read-only, a genuine commit fails,
//     yet identical/weaker Retain calls still succeed (nothing is written).
func TestRetainIdempotentNoDiskSideEffect(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only directory cannot be enforced while running as root")
	}
	dir := t.TempDir()
	opts := Options{}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "o", []byte("audit-version-1"))
	expiry := clk.now().Add(24 * time.Hour)

	// First timed hold: real commit.
	if _, err := s.Retain(ctx, "o", v1.Version, expiry); err != nil {
		t.Fatalf("initial retain: %v", err)
	}
	// Permanent hold: another real (strengthening) commit.
	if _, err := s.Retain(ctx, "o", v1.Version, time.Time{}); err != nil {
		t.Fatalf("strengthen to permanent: %v", err)
	}

	manifestPath := filepath.Join(dir, manifestName)
	st0, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	content0, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	// Settle mtime granularity before the no-op calls.
	time.Sleep(10 * time.Millisecond)

	// Identical permanent repeat and weaker timed requests: no commit.
	if ri, err := s.Retain(ctx, "o", v1.Version, time.Time{}); err != nil || !ri.Permanent {
		t.Fatalf("repeat permanent: ri=%+v err=%v", ri, err)
	}
	if ri, err := s.Retain(ctx, "o", v1.Version, clk.now().Add(time.Hour)); err != nil || !ri.Permanent {
		t.Fatalf("weaker timed on permanent: ri=%+v err=%v", ri, err)
	}

	st1, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	if !st1.ModTime().Equal(st0.ModTime()) {
		t.Fatalf("identical/weaker retain rewrote manifest: mtime %s -> %s",
			st0.ModTime(), st1.ModTime())
	}
	if content1, err := os.ReadFile(manifestPath); err != nil || !equalBytes(content0, content1) {
		t.Fatalf("identical/weaker retain changed manifest content")
	}
	t.Logf("judgment: identical permanent repeat and weaker timed request caused zero manifest writes")

	// Boundary: after a timed hold lapsed naturally, any valid future
	// request re-strengthens the version (the recorded expiry is in the
	// past, so every future instant is stronger) and therefore commits.
	expiredV := putBytes(t, s, "e", []byte("expired-hold-version"))
	if _, err := s.Retain(ctx, "e", expiredV.Version, clk.now().Add(time.Minute)); err != nil {
		t.Fatalf("retain e: %v", err)
	}
	clk.advance(2 * time.Hour) // hold now expired
	stExp, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	near := clk.now().Add(time.Minute)
	if ri, err := s.Retain(ctx, "e", expiredV.Version, near); err != nil || !ri.Retained || !ri.Until.Equal(near) {
		t.Fatalf("future request over an expired hold must re-strengthen and commit: ri=%+v err=%v", ri, err)
	}
	if st2, err := os.Stat(manifestPath); err != nil || st2.ModTime().Equal(stExp.ModTime()) {
		t.Fatalf("re-strengthening an expired hold must rewrite manifest: %v %v", st2, err)
	}
	t.Logf("judgment: any future request over an expired record strengthens and commits (no false no-op)")

	// Now the timed-hold variant on a different version, with the directory
	// read-only: identical and weaker requests must still succeed because
	// they must not attempt any write.
	v2 := putBytes(t, s, "p", []byte("audit-version-2"))
	expiry2 := clk.now().Add(24 * time.Hour)
	// First establish the hold while the directory is writable.
	if _, err := s.Retain(ctx, "p", v2.Version, expiry2); err != nil {
		t.Fatalf("initial timed retain: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod read-only: %v", err)
	}
	// Keep blobs/ traversable for reads (walkBlobs only needs read/execute);
	// only the root directory write bit matters for manifest commits.
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	s2, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("reopen read-only: %v", err)
	}
	// The fake clock is process-local; re-install it so the stored (still
	// future at "now") expiry keeps classifying as active.
	withFakeClock(s2, base)

	// Identical repeat and weaker (shorter) request: no write attempted.
	if ri, err := s2.Retain(ctx, "p", v2.Version, expiry2); err != nil || !ri.Retained {
		t.Fatalf("identical timed retain under read-only dir: ri=%+v err=%v", ri, err)
	}
	if ri, err := s2.Retain(ctx, "p", v2.Version, clk.now().Add(time.Hour)); err != nil ||
		!ri.Retained || !ri.Until.Equal(expiry2) {
		t.Fatalf("weaker timed retain under read-only dir must succeed without shortening: ri=%+v err=%v", ri, err)
	}
	// A genuinely stronger request must fail exactly because it must write.
	if _, strongerErr := s2.Retain(ctx, "p", v2.Version, clk.now().Add(7*24*time.Hour)); !errors.Is(strongerErr, fs.ErrPermission) {
		t.Fatalf("strengthening retain on read-only dir: got %v, want a permission error", strongerErr)
	} else {
		t.Logf("judgment: strengthening retain still commits and therefore fails: %v", strongerErr)
	}
	// The query side needs no write either.
	if ri, err := s2.Retention(ctx, "p", v2.Version); err != nil || !ri.Retained {
		t.Fatalf("retention query: ri=%+v err=%v", ri, err)
	}
	// Restore writability before cleanup closes the store cleanly.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("restore writable: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
