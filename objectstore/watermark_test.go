package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWatermarkBelowIsNoop verifies that when usage is already within the
// watermark, no soft-deleted/active data is touched.
func TestWatermarkBelowIsNoop(t *testing.T) {
	s, dir := newTestStore(t, Options{MaxPhysicalBytes: 1 << 30})
	ctx := context.Background()
	d := []byte("some-data-that-fits")
	v := putBytes(t, s, "o", d)
	if _, err := s.Delete(ctx, "o"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rep, st, err := s.SweepToWatermark(ctx)
	if err != nil {
		t.Fatalf("sweep below watermark: %v", err)
	}
	if rep.VersionsCollected != 0 {
		t.Fatalf("nothing should be collected below watermark: %+v", rep)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("deleted blob must be kept below watermark")
	}
	// Still restorable.
	if _, err := s.Restore(ctx, "o"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if string(getBody(t, s, "o")) != string(d) {
		t.Fatalf("data changed")
	}
	if st.OnDiskBytes != int64(len(d)) {
		t.Fatalf("OnDiskBytes=%d want %d", st.OnDiskBytes, len(d))
	}
	t.Logf("result: below-watermark sweep is a no-op; v%d bytes kept, restore ok", v.Version)
}

// TestWatermarkOrphansFirst verifies orphan blobs are reclaimed before any
// soft-deleted version is sacrificed, and that doing so alone can satisfy
// the watermark without touching readable/deleted metadata.
func TestWatermarkOrphansFirst(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxPhysicalBytes: 15}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	// 10 bytes referenced; watermark 15.
	putBytes(t, s, "live", []byte("0123456789"))
	// Seed a big orphan via the internal staging path (crashed write).
	if _, _, err := s.blobs.put(stringsReader("orphan-orphan-orphan"), ""); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	// A soft-deleted 5-byte version exists too; it must NOT be needed,
	// because after the orphan is removed 10+5=15 == watermark.
	dv := putBytes(t, s, "del", []byte("del12")) // 5 bytes
	if _, err := s.Delete(ctx, "del"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	before, _ := s.Stats(ctx)
	if before.OrphanBlobs != 1 {
		t.Fatalf("setup: want 1 orphan, got %d", before.OrphanBlobs)
	}

	rep, st, err := s.SweepToWatermark(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if rep.OrphanBlobsRemoved != 1 {
		t.Fatalf("orphan not removed first: %+v", rep)
	}
	if rep.VersionsCollected != 0 {
		t.Fatalf("deleted version sacrificed even though orphans sufficed: %+v", rep)
	}
	if st.OnDiskBytes > opts.MaxPhysicalBytes {
		t.Fatalf("usage above watermark: %d > %d", st.OnDiskBytes, opts.MaxPhysicalBytes)
	}
	if _, _, gerr := s.GetVersion(ctx, "del", dv.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("deleted version should remain restorable: %v", gerr)
	}
	t.Logf("result: orphans reclaimed first (%d), deleted version untouched, on_disk=%d<=watermark",
		rep.OrphanBlobsRemoved, st.OnDiskBytes)
}

// TestWatermarkPriorityAndProtection builds several soft-deleted versions
// in the three reclaim classes plus a retained one and a live one, sets a
// tight watermark, and asserts the deterministic order and that protected
// data survives even when ErrWatermark is returned.
func TestWatermarkPriorityAndProtection(t *testing.T) {
	dir := t.TempDir()
	// Watermark very low: only a couple of small blobs can remain.
	opts := Options{MaxPhysicalBytes: 5}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	// All payloads are distinct (so distinct blobs), 8 bytes each.
	mk := func(name, body string) VersionInfo {
		v := putBytes(t, s, name, []byte(body))
		if _, err := s.Delete(ctx, name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
		return v
	}
	released := mk("released", "released") // class 1
	expired := mk("expired-", "expired-")  // class 2
	ordinary := mk("ordinary", "ordinary") // class 3
	// retained-deleted: active hold, must never be collected.
	retained := putBytes(t, s, "retained", []byte("retained"))
	if _, err := s.PutRetention(ctx, "retained", retained.Version, RetainUntil(now.Add(24*time.Hour))); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := s.Delete(ctx, "retained"); err != nil {
		t.Fatalf("delete retained: %v", err)
	}
	// live (active) version: can never be sacrificed.
	live := putBytes(t, s, "live", []byte("live-live"))

	// Release the "released" hold; expire the "expired" one.
	if _, err := s.PutRetention(ctx, "released", released.Version, RetainUntil(now.Add(time.Hour))); err != nil {
		t.Fatalf("pre-hold released: %v", err)
	}
	if _, err := s.ReleaseRetention(ctx, "released", released.Version); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.PutRetention(ctx, "expired-", expired.Version, RetainUntil(now.Add(time.Hour))); err != nil {
		t.Fatalf("pre-hold expired: %v", err)
	}
	*clk = now.Add(2 * time.Hour) // expired hold lapses

	rep, _, werr := s.SweepToWatermark(ctx)
	// Live blob (9) + retained blob (8) = 17 bytes are protected, > 5, so
	// the watermark cannot be met.
	if !errors.Is(werr, ErrWatermark) {
		t.Fatalf("want ErrWatermark, got %v", werr)
	}
	if rep.BelowWatermark {
		t.Fatalf("report must say not below watermark: %+v", rep)
	}

	// Exactly the three non-protected deleted versions were collected, in
	// class order released -> expired -> ordinary.
	for _, c := range []struct {
		name    string
		version int64
	}{
		{"released", released.Version},
		{"expired-", expired.Version},
		{"ordinary", ordinary.Version},
	} {
		if _, _, gerr := s.GetVersion(ctx, c.name, c.version); !errors.Is(gerr, ErrCollected) {
			t.Fatalf("%s should be collected: %v", c.name, gerr)
		}
	}
	if rep.VersionsCollected != 3 {
		t.Fatalf("want 3 versions collected, got %d", rep.VersionsCollected)
	}
	// Protected data intact.
	if _, _, gerr := s.GetVersion(ctx, "retained", retained.Version); !errors.Is(gerr, ErrRetained) {
		t.Fatalf("retained-deleted must remain held: %v", gerr)
	}
	if string(getBody(t, s, "live")) != "live-live" {
		t.Fatalf("live version sacrificed: %v", live)
	}
	if blobFileCount(t, dir) != 2 {
		t.Fatalf("only live+retained blobs must remain, got %d", blobFileCount(t, dir))
	}
	t.Logf("result: order released>expired>ordinary; protected live+retained survive; ErrWatermark returned")
}

// TestWatermarkStopsAsSoonAsBelow verifies reclaim stops immediately once
// the mark is reached, leaving lower-priority deleted versions restorable.
func TestWatermarkStopsAsSoonAsBelow(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxPhysicalBytes: 18} // two 9-byte blobs
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	a := putBytes(t, s, "a", []byte("aaaaaaaaa")) // 9
	b := putBytes(t, s, "b", []byte("bbbbbbbbb")) // 9
	c := putBytes(t, s, "c", []byte("ccccccccc")) // 9
	for _, n := range []string{"a", "b", "c"} {
		if _, err := s.Delete(ctx, n); err != nil {
			t.Fatalf("delete %s: %v", n, err)
		}
	}
	rep, st, err := s.SweepToWatermark(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// 27 -> must drop to <=18: exactly one (9-byte) version collected, and
	// reclaim must stop there.
	if rep.VersionsCollected != 1 || st.OnDiskBytes != 18 {
		t.Fatalf("expected exactly one version collected to 18 bytes: %+v on_disk=%d",
			rep, st.OnDiskBytes)
	}
	// Deterministic: "a" (oldest) is collected; b and c stay.
	if _, _, gerr := s.GetVersion(ctx, "a", a.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("oldest (a) should be collected first: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "b", b.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("b should be spared once below mark: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "c", c.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("c should be spared once below mark: %v", gerr)
	}
	t.Logf("result: reclaim stopped at on_disk=%d<=watermark, b/c restorable", st.OnDiskBytes)
}

// TestWatermarkRepeatedIsStable verifies running SweepToWatermark repeatedly
// converges and never re-collects or corrupts anything.
func TestWatermarkRepeatedIsStable(t *testing.T) {
	s, dir := newTestStore(t, Options{MaxPhysicalBytes: 1})
	ctx := context.Background()
	putBytes(t, s, "live", []byte("xx")) // 2 bytes, protected, can't meet mark
	for i := 0; i < 4; i++ {
		rep, _, err := s.SweepToWatermark(ctx)
		if !errors.Is(err, ErrWatermark) {
			t.Fatalf("iter %d: want ErrWatermark got %v", i, err)
		}
		if rep.VersionsCollected != 0 {
			t.Fatalf("iter %d: re-collected versions: %+v", i, rep)
		}
	}
	if string(getBody(t, s, "live")) != "xx" {
		t.Fatalf("protected live data harmed by repeated sweep")
	}
	_ = dir
	t.Logf("result: repeated watermark sweeps stable, protected data intact")
}

// TestWatermarkSharedBlobSafety verifies a digest shared by a live version
// and a deleted version is never evicted, while a different purely-deleted
// blob is.
func TestWatermarkSharedBlobSafety(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxPhysicalBytes: 0} // set below after seeding
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	shared := []byte("shared-bytes!") // 13
	putBytes(t, s, "live1", shared)   // active
	dv := putBytes(t, s, "del1", shared)
	if _, err := s.Delete(ctx, "del1"); err != nil {
		t.Fatalf("delete del1: %v", err)
	}
	// Tight watermark to force eviction attempts.
	s.opts.MaxPhysicalBytes = 1
	rep, _, werr := s.SweepToWatermark(ctx)
	if !errors.Is(werr, ErrWatermark) {
		t.Fatalf("shared+protected blob cannot meet mark, want ErrWatermark got %v", werr)
	}
	if rep.VersionsCollected != 0 {
		t.Fatalf("deleted version sharing a live blob must not be collected: %+v", rep)
	}
	if string(getBody(t, s, "live1")) != string(shared) {
		t.Fatalf("live shared data lost")
	}
	if _, _, gerr := s.GetVersion(ctx, "del1", dv.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("shared deleted version wrongly changed: %v", gerr)
	}
	t.Logf("result: shared-with-live blob fully protected")
}
