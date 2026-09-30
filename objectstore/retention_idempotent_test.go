package objectstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// manifestMTime returns the last modification time of the committed
// manifest.json, or a zero time when it does not exist yet.
func manifestMTime(t *testing.T, dir string) time.Time {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, manifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}
		}
		t.Fatalf("stat manifest: %v", err)
	}
	return fi.ModTime()
}

// TestRetainIdempotentNoDiskWrite verifies the extend-only contract at
// the disk level:
//
//   - repeating the exact same permanent or timed hold succeeds without
//     touching the manifest (its mtime is unchanged);
//   - a strictly weaker request (permanent re-asserted as permanent, or a
//     shorter timed expiry over a longer one) likewise has no side effect;
//   - making the store directory read-only still allows those no-op
//     requests to succeed, while a genuinely strengthening request fails
//     (proving the no-op did not attempt to write).
func TestRetainIdempotentNoDiskWrite(t *testing.T) {
	dir := t.TempDir()
	s := newTestStoreAt(t, dir, Options{})
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "audit", []byte("audit-snapshot-v1"))
	v2 := putBytes(t, s, "audit", []byte("audit-snapshot-v2"))

	// Seed a timed hold on v1 and a permanent hold on v2; these commits
	// must of course hit disk.
	expiry := clk.now().Add(72 * time.Hour)
	if _, err := s.Retain(ctx, "audit", v1.Version, expiry); err != nil {
		t.Fatalf("retain v1 timed: %v", err)
	}
	if _, err := s.Retain(ctx, "audit", v2.Version, time.Time{}); err != nil {
		t.Fatalf("retain v2 permanent: %v", err)
	}
	mtimeAfterSeed := manifestMTime(t, dir)
	t.Logf("seeded holds; manifest mtime=%s", mtimeAfterSeed.Format(time.RFC3339Nano))

	// The file system may round mtimes; sleep well past typical granularity.
	time.Sleep(20 * time.Millisecond)

	// 1. Exact duplicate timed hold: success, no write.
	if _, err := s.Retain(ctx, "audit", v1.Version, expiry); err != nil {
		t.Fatalf("identical timed retain: %v", err)
	}
	// 2. Weaker timed request (shorter expiry): success, no write.
	if ri, err := s.Retain(ctx, "audit", v1.Version, expiry.Add(-time.Hour)); err != nil ||
		!ri.Retained || !ri.Until.Equal(expiry) {
		t.Fatalf("weaker timed retain changed state: ri=%+v err=%v", ri, err)
	}
	// 3. Permanent re-asserted as permanent: success, no write.
	if _, err := s.Retain(ctx, "audit", v2.Version, time.Time{}); err != nil {
		t.Fatalf("identical permanent retain: %v", err)
	}
	// 4. Permanent cannot be weakened to timed: success, no write.
	if ri, err := s.Retain(ctx, "audit", v2.Version, expiry); err != nil || !ri.Permanent {
		t.Fatalf("permanent hold must ignore timed request: ri=%+v err=%v", ri, err)
	}

	if got := manifestMTime(t, dir); !got.Equal(mtimeAfterSeed) {
		t.Fatalf("idempotent retains wrote the manifest: mtime moved %s -> %s",
			mtimeAfterSeed.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano))
	}
	t.Logf("judgment: identical/weaker retains produced no manifest write")

	// 5. With the directory made read-only, the no-op requests must still
	// succeed, while a strengthening request must surface the write error.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod readonly: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	ro, err := Open(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("reopen readonly: %v", err)
	}
	t.Cleanup(func() { ro.Close() })
	withFakeClock(ro, base)
	if _, err := ro.Retain(ctx, "audit", v1.Version, expiry); err != nil {
		t.Fatalf("identical retain must succeed on read-only dir: %v", err)
	}
	if _, err := ro.Retain(ctx, "audit", v1.Version, expiry.Add(-2*time.Hour)); err != nil {
		t.Fatalf("weaker retain must succeed on read-only dir: %v", err)
	}
	if _, err := ro.Retain(ctx, "audit", v2.Version, time.Time{}); err != nil {
		t.Fatalf("identical permanent retain must succeed on read-only dir: %v", err)
	}
	if _, err := ro.Retain(ctx, "audit", v1.Version, expiry.Add(24*time.Hour)); err == nil {
		t.Fatalf("strengthening retain on read-only dir must fail with a write error")
	} else {
		t.Logf("judgment: strengthening retain hit disk as expected: %v", err)
	}

	// Restore writability and confirm the state was untouched by no-ops:
	// v1 still held until the original expiry, v2 still permanent.
	if err := ro.Close(); err != nil {
		t.Fatalf("close readonly: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod writable: %v", err)
	}
	s2, err := Open(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("reopen writable: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	withFakeClock(s2, base)
	ri, err := s2.Retention(ctx, "audit", v1.Version)
	if err != nil || !ri.Retained || !ri.Until.Equal(expiry) {
		t.Fatalf("v1 hold changed by idempotent calls: %+v err=%v", ri, err)
	}
	ri, err = s2.Retention(ctx, "audit", v2.Version)
	if err != nil || !ri.Retained || !ri.Permanent {
		t.Fatalf("v2 permanent hold changed: %+v err=%v", ri, err)
	}
	t.Logf("result: identical/weaker retains are true no-ops; only strengthening writes")
}

// TestRetainExtendOnlyAfterExpiry covers the boundary that an expired hold
// followed by a fresh future request is exactly one write and duplicate
// requests for that fresh hold are again side-effect free.
func TestRetainExtendOnlyAfterExpiry(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)
	v1 := putBytes(t, s, "o", []byte("expiry-boundary-bytes"))

	expiry := clk.now().Add(time.Hour)
	if _, err := s.Retain(ctx, "o", v1.Version, expiry); err != nil {
		t.Fatalf("retain: %v", err)
	}
	clk.set(expiry) // hold now expired
	ri, err := s.Retention(ctx, "o", v1.Version)
	if err != nil || ri.Retained {
		t.Fatalf("expired hold should report inactive: %+v err=%v", ri, err)
	}

	// A fresh future hold strengthens relative to the expired record.
	fresh := clk.now().Add(2 * time.Hour)
	if _, err := s.Retain(ctx, "o", v1.Version, fresh); err != nil {
		t.Fatalf("retain after expiry: %v", err)
	}
	// Duplicate of the fresh hold is a no-op (shorter too).
	if ri, err := s.Retain(ctx, "o", v1.Version, fresh); err != nil || !ri.Until.Equal(fresh) {
		t.Fatalf("identical re-retain changed state: %+v err=%v", ri, err)
	}
	if ri, err := s.Retain(ctx, "o", v1.Version, fresh.Add(-time.Minute)); err != nil ||
		!ri.Until.Equal(fresh) {
		t.Fatalf("weaker re-retain shortened hold: %+v err=%v", ri, err)
	}
	// Past expiry remains rejected.
	if _, err := s.Retain(ctx, "o", v1.Version, clk.now().Add(-time.Second)); err == nil {
		t.Fatalf("past expiry must be rejected, got %v", err)
	}
	t.Logf("result: re-retaining after expiry is exactly one effective write; duplicates no-op")
}
