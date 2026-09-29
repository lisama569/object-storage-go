package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentWritesAndWatermarkSweep runs Put writers concurrently with
// repeated watermark-driven sweeps, while one old version is permanently
// retained. Invariants checked afterwards:
//
//   - every committed version number exists exactly once and reads intact;
//   - the retained old version is still present and protected;
//   - DiskBytes equals the actual number of blob files' total size (no
//     dangling reference, no metadata/data skew);
//   - no read ever returns ErrCollected/ErrDeleted for a current version.
func TestConcurrentWritesAndWatermarkSweep(t *testing.T) {
	dir := t.TempDir()
	// Very low mark so sweeps constantly try to reclaim; writers keep
	// pushing usage above it.
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	first := putBytes(t, s, "hot", []byte("retained-first-version!!"))
	if _, err := s.Retain(ctx, "hot", first.Version, time.Time{}); err != nil {
		t.Fatalf("retain first: %v", err)
	}

	const writers, writesPer = 8, 12
	const total = writers * writesPer
	var wg sync.WaitGroup
	errs := make(chan error, total+2)

	// Sweepers run continuously; ErrWatermarkExceeded is the expected
	// outcome (active data must not be removed).
	sweepDone := make(chan struct{})
	for k := 0; k < 2; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-sweepDone:
					return
				default:
				}
				if _, err := s.Sweep(ctx); err != nil && !errors.Is(err, ErrWatermarkExceeded) {
					errs <- fmt.Errorf("sweeper: %w", err)
					return
				}
			}
		}()
	}

	// Writers create distinct new versions.
	var wgw sync.WaitGroup
	for w := 0; w < writers; w++ {
		wgw.Add(1)
		go func(w int) {
			defer wgw.Done()
			for i := 0; i < writesPer; i++ {
				payload := []byte(fmt.Sprintf("hot-%02d-%03d-padding-bytes-xxxxxx", w, i))
				if _, err := s.Put(ctx, "hot", bytes.NewReader(payload)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wgw.Wait()
	close(sweepDone)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent op failed: %v", err)
	}

	// Validate the final state thoroughly.
	vs, err := s.Versions(ctx, "hot")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(vs) != total+1 {
		t.Fatalf("versions lost/duplicated under concurrent sweep: got %d want %d", len(vs), total+1)
	}
	seen := map[int64]bool{}
	for _, v := range vs {
		if seen[v.Version] {
			t.Fatalf("duplicate version %d", v.Version)
		}
		seen[v.Version] = true
		if v.State == StateCollected {
			t.Fatalf("hot version %d collected despite being live history", v.Version)
		}
		body := getVersionBody(t, s, "hot", v.Version)
		if v.Digest != digestOf(body) {
			t.Fatalf("version %d body mismatch after concurrent sweep", v.Version)
		}
	}
	// The retained first version must still be readable and still held.
	ri, err := s.Retention(ctx, "hot", first.Version)
	if err != nil || !ri.Retained || !ri.Permanent {
		t.Fatalf("retained first version after concurrent sweeps: %+v err=%v", ri, err)
	}
	if string(getVersionBody(t, s, "hot", first.Version)) != "retained-first-version!!" {
		t.Fatalf("retained bytes damaged under concurrent sweeps")
	}
	// Metadata/data skew check: current body readable; stats vs disk agree.
	if body := getBody(t, s, "hot"); len(body) == 0 {
		t.Fatalf("current version unreadable")
	}
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	var diskActual int64
	if err := s.blobs.walkBlobs(func(digest string, size int64) error {
		diskActual += size
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if diskActual != st.DiskBytes {
		t.Fatalf("disk skew: walk=%d stats=%d", diskActual, st.DiskBytes)
	}
	// Every referenced digest must physically exist (no dangling ref).
	rep, err := s.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rep.MissingBlobs) != 0 {
		t.Fatalf("dangling references after concurrent sweep: %v", rep.MissingBlobs)
	}
	t.Logf("result: %d versions + 1 retained survived %d writers x sweeps; disk=%d consistent",
		total, writers, diskActual)
}

// TestConcurrentRetainSweepRelease runs many concurrent aggressive sweeps
// while even-numbered versions are permanently held: held versions must
// never be collected. After all holds are released (itself concurrent with
// trailing sweeps) the formerly protected data may then be reclaimed.
func TestConcurrentRetainSweepRelease(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SweepDeletedVersions: true}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	const n = 20
	versions := make([]VersionInfo, n)
	for i := 0; i < n; i++ {
		versions[i] = putBytes(t, s, "obj", []byte(fmt.Sprintf("snapshot-%04d-padding!!", i)))
	}

	// Phase A (deterministic): soft-delete every non-current version, then
	// permanently hold the even-numbered ones.
	for i := 0; i < n-1; i++ {
		if _, err := s.DeleteVersion(ctx, "obj", versions[i].Version); err != nil {
			t.Fatalf("delete v%d: %v", i, err)
		}
	}
	for i := 0; i < n-1; i += 2 {
		if _, err := s.Retain(ctx, "obj", versions[i].Version, time.Time{}); err != nil {
			t.Fatalf("retain v%d: %v", i, err)
		}
	}

	// Phase B: many sweepers concurrently. Unheld (odd) versions may be
	// collected; held (even) versions must survive every one of them.
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if _, err := s.Sweep(ctx); err != nil {
					errs <- fmt.Errorf("sweeper %d: %w", k, err)
					return
				}
			}
		}(k)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent sweep failed: %v", err)
	}
	for i := 0; i < n-1; i++ {
		vi := mustVersions(t, s, "obj")[i]
		if i%2 == 0 {
			if vi.State == StateCollected || !vi.Retained {
				t.Fatalf("held even version v%d harmed by aggressive sweep: state=%v retained=%v",
					versions[i].Version, vi.State, vi.Retained)
			}
			// It is still soft-deleted (reads as ErrDeleted) but its bytes
			// must physically survive: check the content-addressed blob
			// directly without mutating version state.
			if ok, _, err := s.blobs.exists(vi.Digest); err != nil || !ok {
				t.Fatalf("held v%d blob missing after sweeps: ok=%v err=%v", i, ok, err)
			}
		} else if vi.State != StateCollected {
			t.Fatalf("unheld odd version v%d should have been collected", versions[i].Version)
		}
	}
	t.Logf("result: 4x40 concurrent sweeps spared every held version, collected all unheld ones")

	// Phase C: release the holds concurrently with trailing sweeps, then
	// settle. All formerly held versions must eventually be collected; the
	// current version stays intact; no dangling references may exist at any
	// final state.
	errs = make(chan error, 20)
	var wg2 sync.WaitGroup
	for i := 0; i < n-1; i += 2 {
		wg2.Add(1)
		go func(v int64) {
			defer wg2.Done()
			if _, err := s.ReleaseRetention(ctx, "obj", v); err != nil {
				errs <- fmt.Errorf("release v%d: %w", v, err)
			}
		}(versions[i].Version)
	}
	for k := 0; k < 3; k++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for i := 0; i < 20; i++ {
				s.Sweep(ctx)
			}
		}()
	}
	wg2.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("release/sweep failed: %v", err)
	}
	if _, err := s.Sweep(ctx); err != nil {
		t.Fatalf("settle sweep: %v", err)
	}
	if body := getBody(t, s, "obj"); string(body) != fmt.Sprintf("snapshot-%04d-padding!!", n-1) {
		t.Fatalf("current version damaged: %q", body)
	}
	rep, err := s.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rep.MissingBlobs) != 0 {
		t.Fatalf("dangling references: %v", rep.MissingBlobs)
	}
	for _, vi := range mustVersions(t, s, "obj")[:n-1] {
		if vi.State != StateCollected {
			t.Fatalf("version v%d should be collected after release: %v", vi.Version, vi.State)
		}
	}
	t.Logf("result: after release, trailing sweeps converged; current intact; no dangling refs")
}

func mustVersions(t *testing.T, s *Store, name string) []VersionInfo {
	t.Helper()
	vs, err := s.Versions(context.Background(), name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	return vs
}

// TestSweepHonorsCancellation verifies that a cancelled context aborts a
// watermark reclaim pass before any commit when checked, and that an
// already-committed-but-partial pass stays self-consistent and can be
// rerun.
func TestSweepHonorsCancellation(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	putBytes(t, s, "live", []byte("live-bytes!!!!!!!!!!!!!!"))
	d1 := putBytes(t, s, "g1", []byte("g1-bytes!!!!!!!!!!!!!!!!"))
	putBytes(t, s, "g2", []byte("g2-bytes!!!!!!!!!!!!!!!!"))
	if _, err := s.Delete(ctx, "g1"); err != nil {
		t.Fatalf("delete g1: %v", err)
	}
	if _, err := s.Delete(ctx, "g2"); err != nil {
		t.Fatalf("delete g2: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Sweep(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled sweep: got %v want context.Canceled", err)
	}
	// Nothing was collected before the boundary check.
	vs, _ := s.Versions(ctx, "g1")
	if vs[0].State == StateCollected {
		t.Fatalf("cancelled sweep must not mutate metadata")
	}
	// A normal rerun still works and converges (with expected over-mark).
	if _, err := s.Sweep(ctx); !errors.Is(err, ErrWatermarkExceeded) {
		t.Fatalf("rerun after cancel: %v", err)
	}
	if _, _, gerr := s.GetVersion(ctx, "g1", d1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("g1 should be collected by the rerun: %v", gerr)
	}
	if string(getBody(t, s, "live")) != "live-bytes!!!!!!!!!!!!!!" {
		t.Fatalf("live data damaged")
	}
	t.Logf("result: cancelled sweep mutates nothing; rerun converges safely")
}
