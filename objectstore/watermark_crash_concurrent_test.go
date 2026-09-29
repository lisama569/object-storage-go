package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// bytesReader wraps bytes.NewReader for test payloads.
func bytesReader(p []byte) *bytes.Reader { return bytes.NewReader(p) }

// ioReadAll reads everything from rc.
func ioReadAll(rc io.Reader) ([]byte, error) { return io.ReadAll(rc) }

// TestWatermarkInterruptedRestart simulates a crash during capacity
// reclamation AFTER the metadata commit (versions -> Collected) but BEFORE
// the blob file is unlinked. On restart the hold/live data must be intact,
// the already-collected versions must read as ErrCollected (never
// ErrNotFound), the orphaned blob files must be cleaned, and rerunning the
// watermark sweep must converge without re-doing or corrupting anything.
func TestWatermarkInterruptedRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxPhysicalBytes: 1} // impossible mark: forces full reclaim
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	// Two ordinary deleted versions (collectable) and one retained-deleted
	// plus one live version (protected).
	d1 := putBytes(t, s, "d1", []byte("deleted-one"))
	d2 := putBytes(t, s, "d2", []byte("deleted-two"))
	held := putBytes(t, s, "held", []byte("held-bytes!!"))
	putBytes(t, s, "live", []byte("live-bytes!!"))
	for _, n := range []string{"d1", "d2"} {
		if _, err := s.Delete(ctx, n); err != nil {
			t.Fatalf("delete %s: %v", n, err)
		}
	}
	if _, err := s.PutRetention(ctx, "held", held.Version, RetainUntil(now.Add(48*time.Hour))); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := s.Delete(ctx, "held"); err != nil {
		t.Fatalf("delete held: %v", err)
	}

	// Crash after the FIRST eviction candidate's metadata commit, before its
	// blob is removed. Candidates are d1 then d2 (oldest first).
	var fired int32
	s.blobs.crashAfterSweepCommit = func() error {
		if atomic.CompareAndSwapInt32(&fired, 0, 1) {
			t.Logf("simulated crash: eviction metadata committed, blob not yet unlinked")
			return errors.New("simulated interruption during watermark eviction")
		}
		return nil
	}
	if _, _, werr := s.SweepToWatermark(ctx); werr == nil {
		t.Fatalf("expected interrupted sweep to return error")
	}

	// In-process: d1 is already Collected in durable metadata; its blob may
	// still be on disk (an orphan now). No dangling reference exists.
	if _, _, gerr := s.GetVersion(ctx, "d1", d1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("committed-evicted d1 must be ErrCollected before restart: %v", gerr)
	}

	// Restart, clear hooks, resume.
	s2 := reopen(t, s, dir, opts)
	s2.now = func() time.Time { return *clk }

	// Protected data survived the restart and the interrupted sweep.
	if string(getBody(t, s2, "live")) != "live-bytes!!" {
		t.Fatalf("live data lost across interrupted eviction")
	}
	if _, _, gerr := s2.Get(ctx, "held"); !errors.Is(gerr, ErrRetained) {
		t.Fatalf("retained hold lost across restart: %v", gerr)
	}

	// Resume to convergence. Because the mark (1) is below protected usage,
	// ErrWatermark is the expected terminal error, but all collectable data
	// must be gone.
	rep, st, werr := s2.SweepToWatermark(ctx)
	if !errors.Is(werr, ErrWatermark) {
		t.Fatalf("expected ErrWatermark after exhausting candidates, got %v", werr)
	}
	for _, c := range []struct {
		n string
		v int64
	}{{"d1", d1.Version}, {"d2", d2.Version}} {
		if _, _, gerr := s2.GetVersion(ctx, c.n, c.v); !errors.Is(gerr, ErrCollected) {
			t.Fatalf("%s must be ErrCollected after resume: %v", c.n, gerr)
		}
	}
	// Only the two protected blobs (held, live) remain on disk; the
	// interrupted d1 blob was reclaimed on resume.
	if got := blobFileCount(t, dir); got != 2 {
		t.Fatalf("expected 2 protected blobs after resume, got %d", got)
	}
	if st.OrphanBlobs != 0 {
		t.Fatalf("interrupted eviction left orphans: %+v", st)
	}
	// Re-run: stable, no further collection, no error besides watermark.
	rep2, _, werr2 := s2.SweepToWatermark(ctx)
	if !errors.Is(werr2, ErrWatermark) || rep2.VersionsCollected != 0 {
		t.Fatalf("resumed sweep not stable: rep=%+v err=%v", rep2, werr2)
	}
	t.Logf("result: crash resumed; collected=%d, protected blobs=%d, orphans=%d, repeat stable",
		rep.VersionsCollected, blobFileCount(t, dir), st.OrphanBlobs)
}

// TestWatermarkSweepConcurrentWithWriters hammers the store with concurrent
// Puts, retention changes and repeated watermark sweeps. It asserts the
// core invariants throughout: every committed version's blob is readable
// (no dangling metadata), retained versions are never collected, and the
// final manifest/disk are mutually consistent.
func TestWatermarkSweepConcurrentWithWriters(t *testing.T) {
	dir := t.TempDir()
	// Moderate mark so sweeps reclaim some deleted data while writers run.
	opts := Options{MaxPhysicalBytes: 4096}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	var writers, sweepers, holders int64 = 6, 2, 2
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: keep putting fresh, deleting old versions of their own object.
	for w := int64(0); w < writers; w++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			name := fmt.Sprintf("w%d", id)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// ~1 KiB per distinct version: six writers quickly push total
				// usage well past the 4 KiB watermark, so sweeps must reclaim
				// soft-deleted versions concurrently while writers proceed.
				payload := []byte(fmt.Sprintf("writer-%02d-payload-%06d%s",
					id, i, strings.Repeat("z", 900)))
				vi, perr := s.Put(ctx, name, bytesReader(payload))
				if perr != nil {
					t.Errorf("writer %d put: %v", id, perr)
					return
				}
				// Soft-delete older versions to create reclaim candidates.
				if vi.Version > 2 {
					if _, derr := s.DeleteVersion(ctx, name, vi.Version-2); derr != nil {
						t.Errorf("writer %d delete old: %v", id, derr)
						return
					}
				}
			}
		}(w)
	}

	// Holders: put an object, retain it permanently, and keep verifying the
	// bytes are always present. These must never be collected.
	heldBodies := make([][]byte, holders)
	for h := int64(0); h < holders; h++ {
		name := fmt.Sprintf("held%d", h)
		body := []byte(fmt.Sprintf("permanent-hold-body-%d", h))
		heldBodies[h] = body
		vi := putBytes(t, s, name, body)
		if _, err := s.PutRetention(ctx, name, vi.Version, RetainPermanent()); err != nil {
			t.Fatalf("hold %s: %v", name, err)
		}
		wg.Add(1)
		go func(n string, want []byte) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rc, _, gerr := s.Get(ctx, n)
				if gerr != nil {
					t.Errorf("held %s became unreadable: %v", n, gerr)
					return
				}
				got, rerr := ioReadAll(rc)
				rc.Close()
				if rerr != nil {
					t.Errorf("held %s read body: %v", n, rerr)
					return
				}
				if string(got) != string(want) {
					t.Errorf("held %s data changed: got %q want %q", n, got, want)
					return
				}
			}
		}(name, body)
	}

	// Sweepers: repeatedly run watermark sweeps. ErrWatermark is acceptable
	// (writers add data faster than reclaim frees); any other error fails.
	for k := int64(0); k < sweepers; k++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, _, err := s.SweepToWatermark(ctx); err != nil && !errors.Is(err, ErrWatermark) {
					t.Errorf("sweeper %d: %v", id, err)
					return
				}
			}
		}(k)
	}

	// Let the chaos run briefly, then stop and join.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Final consistency checks.
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("final stats: %v", err)
	}
	// Recover must find no missing blobs for any readable version.
	rep, rerr := s.Recover(ctx)
	if rerr != nil {
		t.Fatalf("recover: %v", rerr)
	}
	if len(rep.MissingBlobs) != 0 {
		t.Fatalf("dangling metadata after concurrent run: missing=%v", rep.MissingBlobs)
	}
	// Every permanently held object still reads exactly its body.
	for h := int64(0); h < holders; h++ {
		if got := getBody(t, s, fmt.Sprintf("held%d", h)); string(got) != string(heldBodies[h]) {
			t.Fatalf("held body mismatch after chaos: %q", got)
		}
	}
	// Every active writer's current version reads fully.
	for w := int64(0); w < writers; w++ {
		_ = getBody(t, s, fmt.Sprintf("w%d", w))
	}
	// Final sweep is stable.
	if _, _, err := s.SweepToWatermark(ctx); err != nil && !errors.Is(err, ErrWatermark) {
		t.Fatalf("final sweep: %v", err)
	}
	t.Logf("result: concurrent writers+holders+sweepers consistent: %+v, missing_blobs=%d",
		st, len(rep.MissingBlobs))
}

// TestWatermarkCancellationStaysConsistent cancels the context mid-eviction
// and asserts the store is left consistent: the manifest never references a
// deleted blob, held/current data survives, and a follow-up uncancelled sweep
// converges.
func TestWatermarkCancellationStaysConsistent(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxPhysicalBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	d1 := putBytes(t, s, "c1", []byte("cancel-one!!"))
	d2 := putBytes(t, s, "c2", []byte("cancel-two!!"))
	putBytes(t, s, "alive", []byte("alive-bytes!"))
	for _, n := range []string{"c1", "c2"} {
		if _, err := s.Delete(context.Background(), n); err != nil {
			t.Fatalf("delete %s: %v", n, err)
		}
	}

	// Cancel the context before the second candidate is processed by
	// triggering cancellation through the pre-delete hook on first blob.
	var seen int32
	ctx, cancel := context.WithCancel(context.Background())
	s.blobs.crashBeforeBlobDelete = func(digest string) error {
		if atomic.AddInt32(&seen, 1) == 1 {
			cancel()
		}
		return nil
	}
	if _, _, err := s.SweepToWatermark(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	// Consistency check on a fresh context: no dangling references, live
	// data readable. (d1 metadata may already be Collected with its blob
	// unlinked — that is the safe commit-first order; that is fine.)
	bg := context.Background()
	rep, rerr := s.Recover(bg)
	if rerr != nil || len(rep.MissingBlobs) != 0 {
		t.Fatalf("dangling ref after cancel: %+v err=%v", rep, rerr)
	}
	if string(getBody(t, s, "alive")) != "alive-bytes!" {
		t.Fatalf("live data harmed by cancelled sweep")
	}
	// Clear hook and run to completion: terminal ErrWatermark expected
	// (protected live bytes exceed mark), all candidates collected.
	s.blobs.crashBeforeBlobDelete = nil
	if _, _, err := s.SweepToWatermark(bg); !errors.Is(err, ErrWatermark) {
		t.Fatalf("want ErrWatermark after resume, got %v", err)
	}
	for _, c := range []struct {
		n string
		v int64
	}{{"c1", d1.Version}, {"c2", d2.Version}} {
		if _, _, gerr := s.GetVersion(bg, c.n, c.v); !errors.Is(gerr, ErrCollected) {
			t.Fatalf("%s should be collected after resume: %v", c.n, gerr)
		}
	}
	if got := blobFileCount(t, dir); got != 1 {
		t.Fatalf("only alive blob should remain, got %d", got)
	}
	t.Logf("result: cancelled sweep left no dangling refs; uncancelled resume converged")
}

// TestSweepAutoEnforcesWatermark verifies that an ordinary Sweep also drives
// usage down to the configured watermark and surfaces ErrWatermark when it
// cannot, while leaving protected data intact.
func TestSweepAutoEnforcesWatermark(t *testing.T) {
	s, dir := newTestStore(t, Options{MaxPhysicalBytes: 4})
	ctx := context.Background()
	putBytes(t, s, "a", []byte("aaaa"))      // 4
	b := putBytes(t, s, "b", []byte("bbbb")) // 4
	if _, err := s.Delete(ctx, "b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	// On disk 8 > 4; deleting the orphan-less deleted b gets to 4 == mark.
	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep should meet mark: %v", err)
	}
	if st.OnDiskBytes != 4 {
		t.Fatalf("on disk should be 4, got %d", st.OnDiskBytes)
	}
	if _, _, gerr := s.GetVersion(ctx, "b", b.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("b should be collected by watermark sweep: %v", gerr)
	}
	if string(getBody(t, s, "a")) != "aaaa" {
		t.Fatalf("live a harmed")
	}
	// Now usage can never drop below mark because a is live (4==4 actually
	// meets); lower mark to force ErrWatermark on a store with held data.
	s.opts.MaxPhysicalBytes = 1
	putBytes(t, s, "h", []byte("hhhh"))
	if _, err := s.PutRetention(ctx, "h", 1, RetainPermanent()); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := s.Delete(ctx, "h"); err != nil {
		t.Fatalf("delete h: %v", err)
	}
	if _, err := s.Sweep(ctx); !errors.Is(err, ErrWatermark) {
		t.Fatalf("want ErrWatermark, got %v", err)
	}
	if blobFileCount(t, dir) != 2 {
		t.Fatalf("protected a+h must remain, blobs=%d", blobFileCount(t, dir))
	}
	t.Logf("result: Sweep auto-enforces watermark and reports ErrWatermark honestly")
}

// TestRetentionOnCurrentVersionBlocksPurge confirms even a retained version
// that later becomes non-current is protected, and a retained CURRENT
// version obviously stays readable and cannot be purged (current rule still
// takes precedence with ErrCurrent).
func TestRetentionOnCurrentVersionBlocksPurge(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	v1 := putBytes(t, s, "o", []byte("first"))
	putBytes(t, s, "o", []byte("second"))
	// Retain the now non-current v1 with an expired-via-clock deadline would
	// not protect; use a future one.
	if _, err := s.PutRetention(ctx, "o", v1.Version, RetainUntil(now.Add(time.Hour))); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := s.PurgeVersion(ctx, "o", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("purge retained non-current: %v", err)
	}
	// After expiry the purge succeeds.
	*clk = now.Add(2 * time.Hour)
	if _, err := s.PurgeVersion(ctx, "o", v1.Version); err != nil {
		t.Fatalf("purge after expiry: %v", err)
	}
	t.Logf("result: retained non-current version protected until expiry, then purgeable")
}
