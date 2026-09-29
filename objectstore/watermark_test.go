package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWatermarkReclaimsInPriorityOrder verifies that when DiskBytes
// exceeds the mark, Sweep first removes true orphans, then soft-deleted
// unretained versions (oldest first), and stops exactly at the mark
// without touching active data or newer deleted data unnecessarily.
func TestWatermarkReclaimsInPriorityOrder(t *testing.T) {
	dir := t.TempDir()
	// Mark chosen so that removing the orphan plus the OLDEST deleted blob
	// is just enough: live 10 + two deleted 14 + orphan 14 = 52 on disk;
	// after orphans 38; collecting the 14-byte oldest leaves 24 <= 30.
	opts := Options{WatermarkBytes: 30}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	// Three distinct blobs: one live (10 bytes), two deleted (10 bytes
	// each), plus a 10-byte orphan. All same size makes accounting exact.
	putBytes(t, s, "live", []byte("live-bytes"))          // 10 bytes
	d1 := putBytes(t, s, "old", []byte("old-deleted-xx")) // 14 bytes
	// Ensure deterministic ordering: d1 is older.
	time.Sleep(2 * time.Millisecond)
	d2 := putBytes(t, s, "new", []byte("new-deleted-xx")) // 14 bytes
	if _, err := s.Delete(ctx, "old"); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	if _, err := s.Delete(ctx, "new"); err != nil {
		t.Fatalf("delete new: %v", err)
	}
	orphanDigest, _, err := s.blobs.put(stringsReader("orphan-bytes!!"), "") // 13 bytes
	if err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	_ = orphanDigest

	stBefore, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if !stBefore.OverWatermark || stBefore.WatermarkBytes != 30 {
		t.Fatalf("expected over watermark before sweep: %+v", stBefore)
	}
	t.Logf("before: disk=%d watermark=%d reclaimable=%d",
		stBefore.DiskBytes, stBefore.WatermarkBytes, stBefore.ReclaimableBytes)

	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// Orphan + the older deleted blob are gone; newer deleted blob must
	// remain (priority), and active data must be intact.
	if st.DiskBytes > 30 {
		t.Fatalf("usage not brought to mark: %+v", st)
	}
	if st.OverWatermark {
		t.Fatalf("should not report over watermark after success: %+v", st)
	}
	if _, _, gerr := s.GetVersion(ctx, "old", d1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("oldest deleted should be collected: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "new", d2.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("newer deleted must survive (priority): %v", gerr)
	}
	if _, err := s.RestoreVersion(ctx, "new", d2.Version); err != nil {
		t.Fatalf("surviving deleted version must be restorable: %v", err)
	}
	if string(getVersionBody(t, s, "new", d2.Version)) != "new-deleted-xx" {
		t.Fatalf("newer deleted bytes must still be restorable")
	}
	if string(getBody(t, s, "live")) != "live-bytes" {
		t.Fatalf("active data sacrificed by watermark reclaim")
	}
	t.Logf("after: disk=%d orphans gone, oldest deleted collected, newer kept, live intact", st.DiskBytes)
}

// TestWatermarkNeverTouchesRetainedAndActive verifies that retained
// versions and active data survive even extreme watermark pressure; Sweep
// reports ErrWatermarkExceeded instead of deleting them.
func TestWatermarkNeverTouchesRetainedAndActive(t *testing.T) {
	dir := t.TempDir()
	// Mark at 1 byte: no safe combination can get there because all live
	// and retained data must stay.
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	putBytes(t, s, "live", []byte("live-bytes-never-sacrificed!!"))
	hd := putBytes(t, s, "held", []byte("held-bytes-never-sacrificed!!"))
	if _, err := s.Delete(ctx, "held"); err != nil {
		t.Fatalf("delete held: %v", err)
	}
	if _, err := s.Retain(ctx, "held", hd.Version, clk.now().Add(24*time.Hour)); err != nil {
		t.Fatalf("retain: %v", err)
	}
	dj := putBytes(t, s, "gone", []byte("expendable-deleted-bytes!!"))
	if _, err := s.Delete(ctx, "gone"); err != nil {
		t.Fatalf("delete gone: %v", err)
	}

	st, err := s.Sweep(ctx)
	if !errors.Is(err, ErrWatermarkExceeded) {
		t.Fatalf("want ErrWatermarkExceeded, got st=%+v err=%v", st, err)
	}
	t.Logf("judgment: after reclaiming all safe data, still disk=%d > mark=1 -> %v",
		st.DiskBytes, err)
	if !st.OverWatermark {
		t.Fatalf("stats must report OverWatermark on the explicit failure: %+v", st)
	}
	// Expendable data was actually reclaimed; protected data intact.
	if _, _, gerr := s.GetVersion(ctx, "gone", dj.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("expendable deleted data should be collected: %v", gerr)
	}
	if string(getBody(t, s, "live")) != "live-bytes-never-sacrificed!!" {
		t.Fatalf("active data sacrificed")
	}
	// A held soft-deleted version still reads as ErrDeleted (the state is
	// deleted); the point of the hold is that its bytes cannot be
	// reclaimed. Restore it and verify the bytes are physically there.
	if _, _, gerr := s.GetVersion(ctx, "held", hd.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("held deleted version reads as deleted: %v", gerr)
	}
	if _, err := s.RestoreVersion(ctx, "held", hd.Version); err != nil {
		t.Fatalf("retained version must be restorable: %v", err)
	}
	if string(getVersionBody(t, s, "held", hd.Version)) != "held-bytes-never-sacrificed!!" {
		t.Fatalf("retained data sacrificed")
	}
	ri, _ := s.Retention(ctx, "held", hd.Version)
	if !ri.Retained {
		t.Fatalf("retained state lost under watermark pressure")
	}
}

// TestWatermarkNoOpWhenBelowMark verifies Sweep with a watermark does
// nothing destructive while usage is below the mark (conservative
// behavior preserved).
func TestWatermarkNoOpWhenBelowMark(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WatermarkBytes: 1 << 20} // 1 MiB, trivially above test data
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
		t.Fatalf("below-mark sweep must not reclaim deleted data, blobs=%d", blobFileCount(t, dir))
	}
	if st.OverWatermark {
		t.Fatalf("must not report over watermark: %+v", st)
	}
	if _, err := s.Restore(ctx, "d"); err != nil {
		t.Fatalf("deleted data must stay restorable: %v", err)
	}
	t.Logf("result: watermark sweep below mark is conservative, deleted data kept")
}

// TestWatermarkRepeatedSweepStable verifies idempotency: once the mark is
// reached, further sweeps change nothing; when still over, they keep
// failing with ErrWatermarkExceeded without extra damage.
func TestWatermarkRepeatedSweepStable(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	putBytes(t, s, "live", []byte("persistent-live-bytes!!!"))
	d1 := putBytes(t, s, "g1", []byte("gone-one-bytes!!!!!!!!"))
	putBytes(t, s, "g2", []byte("gone-two-bytes!!!!!!!!"))
	if _, err := s.Delete(ctx, "g1"); err != nil {
		t.Fatalf("delete g1: %v", err)
	}
	if _, err := s.Delete(ctx, "g2"); err != nil {
		t.Fatalf("delete g2: %v", err)
	}

	st1, err1 := s.Sweep(ctx)
	if !errors.Is(err1, ErrWatermarkExceeded) {
		t.Fatalf("first sweep: %v %+v", err1, st1)
	}
	st2, err2 := s.Sweep(ctx)
	if !errors.Is(err2, ErrWatermarkExceeded) {
		t.Fatalf("second sweep: %v %+v", err2, st2)
	}
	if st2.DiskBytes != st1.DiskBytes {
		t.Fatalf("repeated sweep not convergent: %d vs %d", st1.DiskBytes, st2.DiskBytes)
	}
	if st2.DeletedVersions != 0 {
		t.Fatalf("both deleted versions should have been collected once: %+v", st2)
	}
	if string(getBody(t, s, "live")) != "persistent-live-bytes!!!" {
		t.Fatalf("live data damaged by repeated sweeps")
	}
	if _, _, gerr := s.GetVersion(ctx, "g1", d1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("g1 should be collected: %v", gerr)
	}
	t.Logf("result: repeated watermark sweeps converge, disk=%d, no repeated destruction", st2.DiskBytes)
}

// TestWatermarkInterruptedRestart simulates a crash between the
// commit-metadata-first step and blob deletion during watermark reclaim,
// then reopens and reruns: the orphaned blob is cleaned and consistency
// holds.
func TestWatermarkInterruptedRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	putBytes(t, s, "live", []byte("live-bytes!!!!!!!!!!!!!!"))
	dv := putBytes(t, s, "dead", []byte("dead-bytes!!!!!!!!!!!!!!"))
	if _, err := s.Delete(ctx, "dead"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var stopped int32
	s.blobs.crashBeforeBlobDelete = func(digest string) error {
		if atomicCAS(&stopped) {
			t.Logf("simulated watermark crash after metadata commit, before blob delete (%s)", digest)
			return errors.New("simulated interruption during watermark reclaim")
		}
		return nil
	}
	if _, err := s.Sweep(ctx); err == nil {
		t.Fatalf("interrupted sweep should surface error")
	}
	// Metadata already marks "dead" collected; the physical blob lingers
	// as an orphan. Reopen and rerun with no hook.
	s2 := reopen(t, s, dir, opts)
	if _, _, gerr := s2.GetVersion(ctx, "dead", dv.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("after crash metadata should already be collected: %v", gerr)
	}
	st, err := s2.Sweep(ctx)
	// Live data alone still exceeds mark=1, so ErrWatermarkExceeded is the
	// expected terminal result — but the lingering orphan must be gone.
	if !errors.Is(err, ErrWatermarkExceeded) {
		t.Fatalf("resumed sweep terminal state: %v %+v", err, st)
	}
	if st.OrphanBlobs != 0 {
		t.Fatalf("interrupted group's blob should be cleaned on rerun: %+v", st)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("expected only live blob left, got %d", blobFileCount(t, dir))
	}
	if string(getBody(t, s2, "live")) != "live-bytes!!!!!!!!!!!!!!" {
		t.Fatalf("live data harmed by interrupted watermark reclaim")
	}
	t.Logf("result: interrupted watermark reclaim resumed cleanly, no dangling reference")
}
