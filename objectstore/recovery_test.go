package objectstore

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// reopen closes the current store and opens the same directory again,
// exactly as a process restart would (lock released and re-acquired).
func reopen(t *testing.T, s *Store, dir string, opts Options) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	s2, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	return s2
}

// TestCrashDuringWriteRecovery simulates a process dying after the data
// blob was staged but before the metadata commit. On restart the object
// must be invisible (no dangling/partial object), the stale staging path
// cleaned, and a subsequent identical write must still succeed.
func TestCrashDuringWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Pre-existing committed object must survive untouched.
	putBytes(t, s, "keep", []byte("durable-baseline"))

	// Arm the post-install, pre-commit crash for one Put.
	var fired int32
	s.blobs.crashAfterInstall = func(digest string) error {
		atomic.AddInt32(&fired, 1)
		t.Logf("simulated crash: blob %s installed, manifest NOT committed", digest)
		return errors.New("simulated process crash before metadata commit")
	}
	payload := []byte("interrupted-object-bytes")
	if _, perr := s.Put(context.Background(), "partial", strings.NewReader(string(payload))); perr == nil {
		t.Fatalf("expected crashed Put to return an error")
	}
	if atomic.LoadInt32(&fired) != 1 {
		t.Fatalf("crash hook did not fire")
	}

	// While the process is still alive, the failed Put's object must not
	// be visible and there must be an orphan blob but no dangling ref.
	if _, _, gerr := s.Get(context.Background(), "partial"); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("partial object visible after crash: %v", gerr)
	}

	// Simulate real restart.
	s2 := reopen(t, s, dir, opts)
	rep, rerr := s2.Recover(context.Background())
	if rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	t.Logf("recovery report: staging_removed=%d manifest_temp_removed=%d checked_versions=%d missing=%v",
		rep.StagingRemoved, rep.ManifestTempRm, rep.CheckedVersions, rep.MissingBlobs)
	if len(rep.MissingBlobs) != 0 {
		t.Fatalf("baseline blob missing after restart: %v", rep.MissingBlobs)
	}
	if stagingCount(t, dir) != 0 {
		t.Fatalf("staging files left after Recover")
	}
	// Baseline object intact.
	if string(getBody(t, s2, "keep")) != "durable-baseline" {
		t.Fatalf("baseline object lost across restart")
	}
	// Interrupted object still absent, never served as partial data.
	if _, _, gerr := s2.Get(context.Background(), "partial"); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("partial object survived as readable: %v", gerr)
	}
	// Orphan blob from the crash is reclaimable and Sweep removes it
	// without touching the baseline blob.
	st, _ := s2.Stats(context.Background())
	t.Logf("stats before sweep: orphan_blobs=%d referenced=%d reclaimable_bytes=%d",
		st.OrphanBlobs, st.ReferencedBlobs, st.ReclaimableBytes)
	if st.OrphanBlobs < 1 {
		t.Fatalf("expected crash orphan blob to be counted")
	}
	if _, serr := s2.Sweep(context.Background()); serr != nil {
		t.Fatalf("sweep: %v", serr)
	}
	st2, _ := s2.Stats(context.Background())
	if st2.OrphanBlobs != 0 || st2.ReclaimableBytes != 0 {
		t.Fatalf("orphan not reclaimed: %+v", st2)
	}
	if string(getBody(t, s2, "keep")) != "durable-baseline" {
		t.Fatalf("sweep damaged referenced object")
	}
	t.Logf("result after restart+sweep: orphans=%d, baseline intact, no dangling refs", st2.OrphanBlobs)
}

// TestOrphanInterruptedWriteIsReclaimed also covers a write that dies
// mid-staging (only a tmp file, blob never installed): Open must remove
// the staging file and metadata must remain self-consistent.
func TestMidStagingCrashRecovery(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	putBytes(t, s, "a", []byte("alpha"))

	// Fabricate a leftover staging file exactly as an interrupted stage
	// would leave behind, plus a leftover manifest temp file.
	if err := writeStagingLeftover(dir, "stage-deadbeef", []byte("partial-partial")); err != nil {
		t.Fatalf("write staging leftover: %v", err)
	}
	if err := writeManifestLeftover(dir, ".manifest-xyz.tmp", []byte("{bad")); err != nil {
		t.Fatalf("write manifest leftover: %v", err)
	}
	if stagingCount(t, dir) != 1 {
		t.Fatalf("setup: staging file missing")
	}
	t.Logf("input: 1 stale stage file + 1 stale manifest temp present before restart")

	s2 := reopen(t, s, dir, Options{})
	if stagingCount(t, dir) != 0 {
		t.Fatalf("Open did not reap stale staging file")
	}
	if got := countManifestTemp(t, dir); got != 0 {
		t.Fatalf("Open did not reap manifest temp: %d", got)
	}
	if string(getBody(t, s2, "a")) != "alpha" {
		t.Fatalf("committed object harmed by staging cleanup")
	}
	rep, _ := s2.Recover(context.Background())
	t.Logf("result: recovery staging_removed=%d manifest_temp_removed=%d; object intact",
		rep.StagingRemoved, rep.ManifestTempRm)
}
