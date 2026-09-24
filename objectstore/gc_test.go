package objectstore

import (
	"context"
	"errors"
	"testing"
)

// TestPurgeVersionBasics verifies:
//   - current version cannot be purged (ErrCurrent);
//   - purging a non-current version keeps its metadata as Collected and
//     later reads fail with ErrCollected (distinct from ErrDeleted);
//   - the blob is physically deleted only when no version still uses it;
//   - purge is idempotent.
func TestPurgeVersionBasics(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "o", []byte("one-content"))
	v2 := putBytes(t, s, "o", []byte("two-content"))

	// Current cannot be purged.
	if _, err := s.PurgeVersion(ctx, "o", v2.Version); !errors.Is(err, ErrCurrent) {
		t.Fatalf("purge current: got %v want ErrCurrent", err)
	}
	t.Logf("judgment: purging current version rejected with ErrCurrent (basis: live data must stay)")

	// Purge the old v1. Its blob is uniquely referenced so it must go.
	pi, err := s.PurgeVersion(ctx, "o", 1)
	if err != nil {
		t.Fatalf("PurgeVersion v1: %v", err)
	}
	if pi.State != StateCollected {
		t.Fatalf("expected collected state, got %v", pi.State)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("unique old blob should be removed, blobs=%d", blobFileCount(t, dir))
	}
	// Reading v1 must explicitly fail with ErrCollected.
	if _, _, gerr := s.GetVersion(ctx, "o", 1); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("purged read: got %v want ErrCollected", gerr)
	}
	// Current v2 remains fully readable.
	if string(getBody(t, s, "o")) != "two-content" {
		t.Fatalf("current version harmed by purge")
	}
	// Idempotent: purging again is a no-op success.
	if _, err := s.PurgeVersion(ctx, "o", 1); err != nil {
		t.Fatalf("repeated purge: %v", err)
	}
	// Restoring a collected version is impossible with ErrCollected.
	if _, err := s.RestoreVersion(ctx, "o", 1); !errors.Is(err, ErrCollected) {
		t.Fatalf("restore collected: got %v want ErrCollected", err)
	}
	t.Logf("result: v1 collected & unrecoverable, blob deleted, v2 intact, purge idempotent")
}

// TestPurgeKeepsSharedBlob verifies reclamation cannot delete a blob
// still referenced by another version. Two objects share identical bytes;
// purging one version leaves the shared blob for the surviving reader.
func TestPurgeKeepsSharedBlob(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	shared := []byte("shared-by-many")
	putBytes(t, s, "a", shared)
	putBytes(t, s, "b", shared)
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("setup: expected exactly 1 shared blob, got %d", blobFileCount(t, dir))
	}

	// Give object a a second, different current version so v1 is purgable.
	putBytes(t, s, "a", []byte("a-only-second"))
	if _, err := s.PurgeVersion(ctx, "a", 1); err != nil {
		t.Fatalf("purge a v1: %v", err)
	}
	if blobFileCount(t, dir) != 2 {
		t.Fatalf("shared blob must survive purge: blobs=%d", blobFileCount(t, dir))
	}
	if string(getBody(t, s, "b")) != string(shared) {
		t.Fatalf("object b lost shared data after purging a")
	}
	t.Logf("result: shared blob retained while any version references it; blobs=%d",
		blobFileCount(t, dir))
}

// TestRepeatedSweepIsStable runs Sweep many times and asserts referenced
// data is never affected and stats converge (idempotency).
func TestRepeatedSweepIsStable(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "x", []byte("xxx"))
	putBytes(t, s, "y", []byte("yyy"))
	// Create an orphan: crashed-style installed blob with no metadata.
	orphanDigest, _, err := s.blobs.put(stringsReader("orphan-data"), "")
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	if ok, _, _ := s.blobs.exists(orphanDigest); !ok {
		t.Fatalf("orphan blob not staged")
	}

	var before Stats
	for i := 0; i < 5; i++ {
		st, err := s.Sweep(ctx)
		if err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
		t.Logf("sweep #%d: orphans=%d physical=%d referenced=%d", i, st.OrphanBlobs,
			st.PhysicalBytes, st.ReferencedBlobs)
		if string(getBody(t, s, "x")) != "xxx" || string(getBody(t, s, "y")) != "yyy" {
			t.Fatalf("sweep corrupted live object on iteration %d", i)
		}
		if i == 0 {
			before = st
		} else {
			if st.OrphanBlobs != before.OrphanBlobs || st.PhysicalBytes != before.PhysicalBytes {
				t.Fatalf("sweep not idempotent at iteration %d: %+v vs %+v", i, before, st)
			}
		}
	}
	if got := blobFileCount(t, dir); got != 2 {
		t.Fatalf("expected 2 referenced blobs after sweep, got %d", got)
	}
	t.Logf("result: repeated sweep stable, orphan gone, 2 referenced blobs intact")
}

// TestSweepInterruptedRestart simulates a crash during blob deletion
// (after metadata is already committed) and asserts a later Sweep
// finishes the job without ever breaking readable versions.
func TestSweepInterruptedRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SweepDeletedVersions: true}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	// Create a soft-deleted object whose bytes will be eligible for GC,
	// plus a live object sharing nothing, to prove it is spared.
	v1 := putBytes(t, s, "gone", []byte("will-be-deleted"))
	putBytes(t, s, "alive", []byte("stay-alive"))
	if _, derr := s.Delete(ctx, "gone"); derr != nil {
		t.Fatalf("delete: %v", derr)
	}

	var stopped int32
	s.blobs.crashAfterSweepCommit = func() error {
		if atomicCAS(&stopped) {
			t.Logf("simulated GC crash after metadata commit, before blob deletion")
			return errors.New("simulated GC interruption after downgrade commit")
		}
		return nil
	}
	if _, serr := s.Sweep(ctx); serr == nil {
		t.Fatalf("expected interrupted sweep to surface error")
	}

	// After an interrupted destructive sweep the metadata already marks
	// the deleted version collected (commit-first order). Reopen and rerun
	// without the hook to finish reclamation.
	s2 := reopen(t, s, dir, opts)
	st, serr := s2.Sweep(ctx)
	if serr != nil {
		t.Fatalf("resumed sweep: %v", serr)
	}
	if _, _, gerr := s2.GetVersion(ctx, "gone", v1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("deleted version should be collected after resume: %v", gerr)
	}
	if string(getBody(t, s2, "alive")) != "stay-alive" {
		t.Fatalf("live object harmed by interrupted GC")
	}
	if st.OrphanBlobs != 0 {
		t.Fatalf("resumed sweep left orphan: %+v", st)
	}
	t.Logf("result: GC resumed after crash, collected=%d orphans=%d, live data intact",
		v1.Version, st.OrphanBlobs)
}

// TestConservativeSweepKeepsDeletedData verifies the default policy never
// reclaims soft-deleted bytes: a conservative Sweep removes only true
// orphans and keeps deleted versions restorable.
func TestConservativeSweepKeepsDeletedData(t *testing.T) {
	s, dir := newTestStore(t, Options{}) // SweepDeletedVersions=false
	ctx := context.Background()
	putBytes(t, s, "d", []byte("deleted-but-kept"))
	if _, err := s.Delete(ctx, "d"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("conservative sweep must keep deleted blob, blobs=%d", blobFileCount(t, dir))
	}
	if st.ReclaimableBytes != 0 {
		t.Fatalf("deleted bytes must not count reclaimable by default: %d", st.ReclaimableBytes)
	}
	rv, err := s.Restore(ctx, "d")
	if err != nil || string(getBody(t, s, "d")) != "deleted-but-kept" {
		t.Fatalf("deleted data must remain restorable: rv=%v err=%v", rv, err)
	}
	t.Logf("result: default GC preserves soft-deleted bytes; Restore succeeds")
}
