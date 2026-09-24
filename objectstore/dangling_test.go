package objectstore

import (
	"context"
	"errors"
	"os"
	"testing"
)

// TestMissingBlobIsRejected deletes a committed blob out from under
// metadata (simulating external data loss) and asserts:
//   - reads are explicitly rejected (never empty/partial success);
//   - Recover lists the dangling digest as missing;
//   - Verify reports it under Missing.
func TestMissingBlobIsRejected(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	vi := putBytes(t, s, "m", []byte("physically-lost-content"))
	path, err := s.blobs.blobPath(vi.Digest)
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove blob: %v", err)
	}
	t.Logf("input: removed committed blob %s while metadata still references it", vi.Digest)

	_, _, gerr := s.Get(ctx, "m")
	if !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("read of missing blob: got %v want ErrNotFound", gerr)
	}
	t.Logf("judgment: Get explicitly rejected missing blob: %v", gerr)

	rep, err := s.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rep.MissingBlobs) != 1 || rep.MissingBlobs[0] != vi.Digest {
		t.Fatalf("recover did not flag missing blob: %+v", rep)
	}
	t.Logf("Recover result: checked=%d missing=%v (dangling refs surfaced, not hidden)",
		rep.CheckedVersions, rep.MissingBlobs)

	vrep, err := s.Verify(ctx)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(vrep.Missing) != 1 {
		t.Fatalf("verify missing list wrong: %+v", vrep)
	}
	t.Logf("Verify result: missing=%v; dir=%s", vrep.Missing, dir)
}

// TestGetVersionErrorOrdering verifies state errors take precedence and
// remain distinguishable across the lifecycle.
func TestGetVersionErrorOrdering(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "o", []byte("one"))
	putBytes(t, s, "o", []byte("two"))

	// unknown version beats everything
	if _, _, err := s.GetVersion(ctx, "o", 77); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown version: %v", err)
	}
	// delete v1, then it reports ErrDeleted (but object current still v2)
	if _, err := s.DeleteVersion(ctx, "o", 1); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "o", 1); !errors.Is(err, ErrDeleted) {
		t.Fatalf("deleted v1: %v", err)
	}
	// purge v1, then it reports ErrCollected
	if _, err := s.PurgeVersion(ctx, "o", 1); err != nil {
		t.Fatalf("purge v1: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "o", 1); !errors.Is(err, ErrCollected) {
		t.Fatalf("collected v1: %v", err)
	}
	// current v2 still fine
	if string(getBody(t, s, "o")) != "two" {
		t.Fatalf("current read broken")
	}
	t.Logf("result: ErrNotFound < ErrDeleted < ErrCollected distinguished consistently")
}
