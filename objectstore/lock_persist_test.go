package objectstore

import (
	"context"
	"errors"
	"testing"
)

// TestProcessLockExclusive verifies a second open of the same directory
// while one Store is live is rejected with ErrLocked, and that closing
// the first releases the lock so a later open succeeds.
func TestProcessLockExclusive(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	s2, err := Open(context.Background(), dir, Options{})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second concurrent open: got %v want ErrLocked", err)
	}
	t.Logf("judgment: concurrent second Open rejected with ErrLocked: %v", err)
	_ = s2
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s3, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatalf("open after close should succeed: %v", err)
	}
	t.Logf("result: lock released on Close, reopened successfully")
	_ = s3.Close()
}

// TestPersistAcrossReopen verifies versions, tombstones and collected
// state all survive a process restart reconstructed from disk.
func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	putBytes(t, s, "doc", []byte("rev1"))
	putBytes(t, s, "doc", []byte("rev2"))
	putBytes(t, s, "gone", []byte("bye"))
	if _, err := s.PurgeVersion(ctx, "doc", 1); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := s.Delete(ctx, "gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	s2 := reopen(t, s, dir, Options{})
	if string(getBody(t, s2, "doc")) != "rev2" {
		t.Fatalf("current version not persisted")
	}
	if _, _, err := s2.GetVersion(ctx, "doc", 1); !errors.Is(err, ErrCollected) {
		t.Fatalf("collected state lost across reopen: %v", err)
	}
	if _, _, err := s2.Get(ctx, "gone"); !errors.Is(err, ErrDeleted) {
		t.Fatalf("delete tombstone lost across reopen: %v", err)
	}
	vs, _ := s2.Versions(ctx, "doc")
	t.Logf("result after restart: doc versions=%d, v1 collected, current=rev2, gone deleted", len(vs))
}

// TestEmptyStoreSweepAndStats verifies operations on a brand new empty
// store do not error and report zero usage.
func TestEmptyStoreSweepAndStats(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats empty: %v", err)
	}
	if st != (Stats{}) {
		t.Fatalf("empty store stats nonzero: %+v", st)
	}
	if _, err := s.Sweep(ctx); err != nil {
		t.Fatalf("sweep empty: %v", err)
	}
	if _, err := s.Recover(ctx); err != nil {
		t.Fatalf("recover empty: %v", err)
	}
	rep, err := s.Verify(ctx)
	if err != nil {
		t.Fatalf("verify empty: %v", err)
	}
	if rep.BlobsChecked != 0 || len(rep.Corrupt) != 0 || len(rep.Missing) != 0 {
		t.Fatalf("empty verify nonzero: %+v", rep)
	}
	if _, err := s.Versions(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("versions unknown: got %v want ErrNotFound", err)
	}
	if _, err := s.Delete(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete unknown: got %v want ErrNotFound", err)
	}
	if _, err := s.Restore(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("restore unknown: got %v want ErrNotFound", err)
	}
	t.Logf("result: empty store stats=%+v, sweep/recover/verify clean, unknown names -> ErrNotFound", st)
}

// TestPutAfterDeleteCreatesNewVersion verifies writing to a deleted
// object resurrects it with a fresh version number while history of the
// deleted version is retained.
func TestPutAfterDeleteCreatesNewVersion(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "m", []byte("old"))
	if _, err := s.Delete(ctx, "m"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	nv := putBytes(t, s, "m", []byte("new"))
	if nv.Version != 2 {
		t.Fatalf("put after delete expected version 2, got %d", nv.Version)
	}
	if string(getBody(t, s, "m")) != "new" {
		t.Fatalf("resurrected read wrong")
	}
	vs, _ := s.Versions(ctx, "m")
	// v1 remains in history as Deleted; v2 is the new current.
	if len(vs) != 2 || vs[0].State != StateDeleted || vs[1].State != StateActive {
		t.Fatalf("history after resurrect wrong: %+v", vs)
	}
	t.Logf("result: write after delete -> new current v%d, v1 retained as deleted", nv.Version)
}

// TestDeleteVersionThenRestoreVersion covers the independent per-version
// soft delete and restore of a non-current version.
func TestDeleteVersionThenRestoreVersion(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "p", []byte("p1"))
	putBytes(t, s, "p", []byte("p2"))
	if _, err := s.DeleteVersion(ctx, "p", 1); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "p", 1); !errors.Is(err, ErrDeleted) {
		t.Fatalf("deleted v1 read: %v", err)
	}
	if _, err := s.RestoreVersion(ctx, "p", 1); err != nil {
		t.Fatalf("restore v1: %v", err)
	}
	if string(getVersionBody(t, s, "p", 1)) != "p1" {
		t.Fatalf("restored v1 data wrong")
	}
	// Current must remain v2 throughout.
	if string(getBody(t, s, "p")) != "p2" {
		t.Fatalf("current pointer disturbed by per-version restore")
	}
	t.Logf("result: per-version delete/restore independent; current stayed v2")
}
