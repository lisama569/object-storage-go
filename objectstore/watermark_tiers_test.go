package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// releasedAndExpiredSetup builds one live blob plus three same-sized
// soft-deleted blobs whose only relevant difference is *why* they became
// reclaimable:
//
//   - "released": held, then explicitly released early;
//   - "expired" : held with a short timed hold that lapses naturally;
//   - "plain"  : soft-deleted without ever being held.
//
// Creation order is deliberately the opposite of the required reclaim
// order (plain is oldest) so that the result cannot pass by age accident.
func releasedAndExpiredSetup(t *testing.T, opts Options) (*Store, VersionInfo, VersionInfo, VersionInfo) {
	t.Helper()
	s, _ := newTestStore(t, opts)
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	putBytes(t, s, "live", []byte("LIVE-bytes-32-characters-xxxxxx")) // 32
	// plain first (oldest Created); it must lose to every hold tier.
	plainV := putBytes(t, s, "plain", []byte("PLAIN-bytes-32-characters-xxxxx")) // 32
	time.Sleep(2 * time.Millisecond)
	expiredV := putBytes(t, s, "expired", []byte("EXPIRED-bytes-32-characters-xxx")) // 32
	time.Sleep(2 * time.Millisecond)
	releasedV := putBytes(t, s, "released", []byte("RELEASED-bytes-32-characters-xx")) // 32

	for _, nm := range []string{"plain", "expired", "released"} {
		if _, err := s.Delete(ctx, nm); err != nil {
			t.Fatalf("delete %s: %v", nm, err)
		}
	}
	if _, err := s.Retain(ctx, "released", releasedV.Version, time.Time{}); err != nil {
		t.Fatalf("retain released-candidate: %v", err)
	}
	if _, err := s.ReleaseRetention(ctx, "released", releasedV.Version); err != nil {
		t.Fatalf("early release: %v", err)
	}
	if _, err := s.Retain(ctx, "expired", expiredV.Version, clk.now().Add(time.Hour)); err != nil {
		t.Fatalf("retain expired-candidate: %v", err)
	}
	clk.advance(2 * time.Hour) // natural expiry, without ReleaseRetention
	return s, releasedV, expiredV, plainV
}

// TestWatermarkReleasedBeforeExpiredOrder is the headline regression:
// with one early-released and one naturally expired version and room to
// reclaim exactly one, the RELEASED version must be the victim even when
// the expired one is older/younger — the tier, not age, decides; ties and
// ordering stay deterministic across reruns.
func TestWatermarkReleasedBeforeExpiredOrder(t *testing.T) {
	// Disk = 4 blobs x 32 = 128. Phase 2 leaves no orphans. To drop to the
	// mark exactly one 32-byte group must be collected: 128 - 32 = 96 <= 97,
	// while doing nothing leaves 128 > 97.
	opts := Options{WatermarkBytes: 97}

	run := func() (releasedCollected, expiredCollected, plainCollected bool) {
		s, releasedV, expiredV, plainV := releasedAndExpiredSetup(t, opts)
		ctx := context.Background()
		st, err := s.Sweep(ctx)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if st.DiskBytes > opts.WatermarkBytes {
			t.Fatalf("usage not brought to mark: %+v", st)
		}
		_, _, gerrR := s.GetVersion(ctx, "released", releasedV.Version)
		_, _, gerrE := s.GetVersion(ctx, "expired", expiredV.Version)
		_, _, gerrP := s.GetVersion(ctx, "plain", plainV.Version)
		return errors.Is(gerrR, ErrCollected), errors.Is(gerrE, ErrCollected), errors.Is(gerrP, ErrCollected)
	}

	for i := 0; i < 5; i++ {
		rc, ec, pc := run()
		if !rc {
			t.Fatalf("iter %d: early-released version must be reclaimed first", i)
		}
		if ec {
			t.Fatalf("iter %d: naturally expired version must be kept when only one fits", i)
		}
		if pc {
			t.Fatalf("iter %d: never-held (oldest) version must lose to both hold tiers", i)
		}
	}
	t.Logf("result: across 5 fresh stores, released is always collected before expired/plain")
}

// TestWatermarkTierOrderFullSequence verifies the complete ordering
// orphan -> released -> expired -> never-held under increasing pressure,
// and that active/held data is never sacrificed while
// ErrWatermarkExceeded is reported once nothing safe remains.
func TestWatermarkTierOrderFullSequence(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	putBytes(t, s, "live", []byte("LIVE-bytes-32-characters-xxxxxx")) // 32
	plainV := putBytes(t, s, "plain", []byte("PLAIN-bytes-32-characters-xxxxx"))
	time.Sleep(2 * time.Millisecond)
	expiredV := putBytes(t, s, "expired", []byte("EXPIRED-bytes-32-characters-xxx"))
	time.Sleep(2 * time.Millisecond)
	releasedV := putBytes(t, s, "released", []byte("RELEASED-bytes-32-characters-xx"))
	heldV := putBytes(t, s, "held", []byte("HELD-bytes-32-characters-xxxx"))
	// An orphan blob, cleaned before any tier is touched.
	if _, _, err := s.blobs.put(stringsReader("ORPHAN-bytes-32-characters-xx"), ""); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	for _, nm := range []string{"plain", "expired", "released", "held"} {
		if _, err := s.Delete(ctx, nm); err != nil {
			t.Fatalf("delete %s: %v", nm, err)
		}
	}
	if _, err := s.Retain(ctx, "released", releasedV.Version, time.Time{}); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if _, err := s.ReleaseRetention(ctx, "released", releasedV.Version); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.Retain(ctx, "expired", expiredV.Version, clk.now().Add(time.Hour)); err != nil {
		t.Fatalf("retain expired: %v", err)
	}
	if _, err := s.Retain(ctx, "held", heldV.Version, clk.now().Add(48*time.Hour)); err != nil {
		t.Fatalf("retain held: %v", err)
	}
	clk.advance(2 * time.Hour) // "expired" lapses; "held" stays active

	st, werr := s.Sweep(ctx)
	if !errors.Is(werr, ErrWatermarkExceeded) {
		t.Fatalf("want ErrWatermarkExceeded with live+held remaining, got %v %+v", werr, st)
	}
	stateOf := func(v VersionInfo, name string) VersionState {
		vs, err := s.Versions(ctx, name)
		if err != nil {
			t.Fatalf("versions %s: %v", name, err)
		}
		return vs[0].State
	}
	if stateOf(releasedV, "released") != StateCollected {
		t.Fatalf("released tier must be collected")
	}
	if stateOf(expiredV, "expired") != StateCollected {
		t.Fatalf("expired tier must be collected after released")
	}
	if stateOf(plainV, "plain") != StateCollected {
		t.Fatalf("never-held tier must be collected after expired")
	}
	if stateOf(heldV, "held") != StateDeleted {
		t.Fatalf("actively held version must remain soft-deleted, never collected")
	}
	if string(getBody(t, s, "live")) != "LIVE-bytes-32-characters-xxxxxx" {
		t.Fatalf("active data sacrificed")
	}
	// Held bytes are physically still there and restorable.
	if _, err := s.RestoreVersion(ctx, "held", heldV.Version); err != nil {
		t.Fatalf("held version restore: %v", err)
	}
	if string(getVersionBody(t, s, "held", heldV.Version)) != "HELD-bytes-32-characters-xxxx" {
		t.Fatalf("held bytes sacrificed")
	}
	// Orphan gone: only live + held blobs remain.
	if got := blobFileCount(t, dir); got != 2 {
		t.Fatalf("want 2 protected blobs left (live+held), got %d", got)
	}
	t.Logf("result: orphan -> released -> expired -> never-held; held and active spared; ErrWatermarkExceeded explicit")
}
