package objectstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// fixedBody pads tag with spaces to exactly n bytes, giving distinct
// digests of an exact, known size for watermark accounting.
func fixedBody(tag string, n int) []byte {
	if len(tag) > n {
		tag = tag[:n]
	}
	return []byte(fmt.Sprintf("%-*s", n, tag))
}

// TestWatermarkReclaimOrderReleasedBeforeExpired reproduces the reported
// priority bug: one soft-deleted version had its hold explicitly released
// early, another soft-deleted version had its timed hold expire naturally,
// and only one of them needs to be reclaimed to meet the mark. The
// released one must be picked even though the expired one is older. The
// release marker must survive a restart, and the choice must be identical
// on repeated runs.
func TestWatermarkReclaimOrderReleasedBeforeExpired(t *testing.T) {
	for run := 0; run < 3; run++ {
		t.Run(fmt.Sprintf("run%d", run), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{WatermarkBytes: 35}
			s, err := Open(context.Background(), dir, opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			ctx := context.Background()
			base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			clk := withFakeClock(s, base)

			// Creation order makes the eventually-expired blob OLDER than
			// the eventually-released one: under a pure oldest-first rule
			// the wrong victim would be picked.
			ev := putBytes(t, s, "exp", fixedBody("exp", 20)) // 20 bytes, oldest
			time.Sleep(5 * time.Millisecond)
			rv := putBytes(t, s, "rel", fixedBody("rel", 20)) // 20 bytes, newer
			time.Sleep(5 * time.Millisecond)
			putBytes(t, s, "live", fixedBody("live", 10)) // 10 bytes, must survive

			// Delete first (audit scenario), then hold both.
			for _, n := range []string{"exp", "rel"} {
				if _, err := s.Delete(ctx, n); err != nil {
					t.Fatalf("delete %s: %v", n, err)
				}
			}
			if _, err := s.Retain(ctx, "exp", ev.Version, clk.now().Add(time.Hour)); err != nil {
				t.Fatalf("retain exp: %v", err)
			}
			if _, err := s.Retain(ctx, "rel", rv.Version, time.Time{}); err != nil {
				t.Fatalf("retain rel: %v", err)
			}

			// One hold expires naturally; the other is released early.
			clk.advance(2 * time.Hour)
			if _, err := s.ReleaseRetention(ctx, "rel", rv.Version); err != nil {
				t.Fatalf("release rel: %v", err)
			}
			ri, err := s.Retention(ctx, "exp", ev.Version)
			if err != nil || ri.Retained {
				t.Fatalf("exp hold should be expired: %+v err=%v", ri, err)
			}

			// Restart: the released-vs-expired distinction is durable.
			s = reopen(t, s, dir, opts)
			clk = withFakeClock(s, base.Add(2*time.Hour))

			// disk = 50; mark 35; reclaiming exactly one 20-byte blob -> 30.
			st, err := s.Sweep(ctx)
			if err != nil {
				t.Fatalf("sweep: %v st=%+v", err, st)
			}
			if st.DiskBytes > 35 {
				t.Fatalf("usage not brought to mark: %+v", st)
			}
			if _, _, gerr := s.GetVersion(ctx, "rel", rv.Version); !errors.Is(gerr, ErrCollected) {
				t.Fatalf("run %d: released blob must be reclaimed first, got %v", run, gerr)
			}
			if _, _, gerr := s.GetVersion(ctx, "exp", ev.Version); !errors.Is(gerr, ErrDeleted) {
				t.Fatalf("run %d: expired blob must survive the partial reclaim, got %v", run, gerr)
			}
			if body := getBody(t, s, "live"); len(body) != 10 || string(body) != string(fixedBody("live", 10)) {
				t.Fatalf("active data sacrificed: %q", body)
			}
			// A second sweep changes nothing further (convergent).
			st2, err := s.Sweep(ctx)
			if err != nil || st2.DiskBytes != st.DiskBytes {
				t.Fatalf("second sweep must converge: %+v err=%v", st2, err)
			}
			t.Logf("run %d: released collected before older expired; disk=%d stable", run, st2.DiskBytes)
		})
	}
}

// TestWatermarkReclaimTierOrderFull exercises the complete ordering
// across all three classes: explicitly released, naturally expired, and
// never-held. A never-held candidate is deliberately the OLDEST blob to
// prove tier beats raw age; the newest candidate is the released one and
// must still go first.
func TestWatermarkReclaimTierOrderFull(t *testing.T) {
	dir := t.TempDir()
	// disk = live 10 + three deleted 20 = 70.
	// mark 55: reclaiming one 20-byte blob -> 50 (released must go first).
	// mark 35: two -> 30 (released + expired; never-held survives).
	opts := Options{WatermarkBytes: 55}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	// Oldest first: never-held, expired, released(newest).
	nv := putBytes(t, s, "never", fixedBody("never", 20))
	time.Sleep(5 * time.Millisecond)
	ev := putBytes(t, s, "exp", fixedBody("exp", 20))
	time.Sleep(5 * time.Millisecond)
	rv := putBytes(t, s, "rel", fixedBody("rel", 20))
	time.Sleep(5 * time.Millisecond)
	putBytes(t, s, "live", fixedBody("live", 10))

	if _, err := s.Delete(ctx, "never"); err != nil {
		t.Fatalf("delete never: %v", err)
	}
	for _, c := range []struct {
		name string
		v    int64
	}{{"exp", ev.Version}, {"rel", rv.Version}} {
		if _, err := s.Delete(ctx, c.name); err != nil {
			t.Fatalf("delete %s: %v", c.name, err)
		}
	}
	if _, err := s.Retain(ctx, "exp", ev.Version, clk.now().Add(time.Hour)); err != nil {
		t.Fatalf("retain exp: %v", err)
	}
	if _, err := s.Retain(ctx, "rel", rv.Version, clk.now().Add(2*time.Hour)); err != nil {
		t.Fatalf("retain rel: %v", err)
	}
	clk.advance(90 * time.Minute) // exp expired; rel still active until +2h
	if _, err := s.ReleaseRetention(ctx, "rel", rv.Version); err != nil {
		t.Fatalf("release rel while its hold is still effective: %v", err)
	}

	// mark 55: exactly one group needed -> the released newest blob.
	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep mark55: %v", err)
	}
	if st.DiskBytes != 50 {
		t.Fatalf("after one reclaim want disk=50, got %+v", st)
	}
	if _, _, gerr := s.GetVersion(ctx, "rel", rv.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("released (newest) must be first victim: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "exp", ev.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("expired must survive first reclaim: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "never", nv.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("never-held oldest must survive first reclaim: %v", gerr)
	}
	t.Logf("judgment: tier beats age - released newest collected before older never-held/expired")

	// Reopen with a tighter mark: two groups needed -> expired goes before
	// never-held; never-held survives even though it is the oldest blob.
	s = reopen(t, s, dir, Options{WatermarkBytes: 35})
	withFakeClock(s, base.Add(90*time.Minute))
	st, err = s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep mark35: %v", err)
	}
	if st.DiskBytes != 30 {
		t.Fatalf("after two reclaims want disk=30, got %+v", st)
	}
	if _, _, gerr := s.GetVersion(ctx, "exp", ev.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("expired must be the second victim: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "never", nv.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("never-held must outlive expired reclaim: %v", gerr)
	}
	if body := getBody(t, s, "live"); string(body) != string(fixedBody("live", 10)) {
		t.Fatalf("active data sacrificed: %q", body)
	}
	t.Logf("judgment: expired before never-held; never-held oldest kept; active intact")

	// Tightest mark forces the never-held group too; the active blob alone
	// still exceeds mark=1, so the result is the explicit failure.
	s = reopen(t, s, dir, Options{WatermarkBytes: 1})
	st, err = s.Sweep(ctx)
	if !errors.Is(err, ErrWatermarkExceeded) {
		t.Fatalf("want ErrWatermarkExceeded, got %v %+v", err, st)
	}
	if _, _, gerr := s.GetVersion(ctx, "never", nv.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("never-held reclaimed last: %v", gerr)
	}
	if !st.OverWatermark || st.DiskBytes != 10 {
		t.Fatalf("only the active blob should remain, got %+v", st)
	}
	t.Logf("result: released > expired > never-held ordering holds; protected/active never sacrificed")
}

// TestWatermarkRetainClearsReleaseMarker verifies the release marker is
// bookkeeping, not permanent history: releasing, then placing a new hold
// that later expires naturally, must classify the version as "expired"
// again (it must not jump ahead of genuinely released data).
func TestWatermarkRetainClearsReleaseMarker(t *testing.T) {
	dir := t.TempDir()
	// live 10 + two deleted 20 = 50; mark 35 requires exactly one reclaim.
	opts := Options{WatermarkBytes: 35}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	xv := putBytes(t, s, "rearmed", fixedBody("rearmed", 20)) // older
	time.Sleep(5 * time.Millisecond)
	rv := putBytes(t, s, "released", fixedBody("released", 20)) // newer
	time.Sleep(5 * time.Millisecond)
	putBytes(t, s, "live", fixedBody("live", 10))

	for _, n := range []string{"rearmed", "released"} {
		if _, err := s.Delete(ctx, n); err != nil {
			t.Fatalf("delete %s: %v", n, err)
		}
	}
	// "rearmed": release an early hold, then re-arm a longer hold and let
	// that later hold expire naturally. Its stale release marker must be
	// gone, so it classifies as expired, not released.
	if _, err := s.Retain(ctx, "rearmed", xv.Version, clk.now().Add(time.Hour)); err != nil {
		t.Fatalf("first retain rearmed: %v", err)
	}
	if _, err := s.ReleaseRetention(ctx, "rearmed", xv.Version); err != nil {
		t.Fatalf("release rearmed early: %v", err)
	}
	if _, err := s.Retain(ctx, "rearmed", xv.Version, clk.now().Add(3*time.Hour)); err != nil {
		t.Fatalf("re-arm rearmed: %v", err)
	}
	// "released": a hold that is effective right now, then released.
	if _, err := s.Retain(ctx, "released", rv.Version, clk.now().Add(6*time.Hour)); err != nil {
		t.Fatalf("retain released: %v", err)
	}
	clk.advance(4 * time.Hour) // rearmed's second hold expired; released still held
	if _, err := s.ReleaseRetention(ctx, "released", rv.Version); err != nil {
		t.Fatalf("release released: %v", err)
	}
	// Releasing the already-inactive version now must be a no-op marker.
	if _, err := s.ReleaseRetention(ctx, "rearmed", xv.Version); err != nil {
		t.Fatalf("release expired version must succeed: %v", err)
	}

	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if st.DiskBytes != 30 {
		t.Fatalf("want disk=30 after one reclaim, got %+v", st)
	}
	if _, _, gerr := s.GetVersion(ctx, "released", rv.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("currently-released (newer) must precede rearmed-then-expired: %v", gerr)
	}
	if _, _, gerr := s.GetVersion(ctx, "rearmed", xv.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("rearmed version must classify as expired and survive: %v", gerr)
	}
	t.Logf("result: re-arming after release clears the released-early classification")
}
