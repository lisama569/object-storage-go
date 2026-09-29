package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetainPermanentAndTimed covers placing permanent and timed holds,
// querying them, and the idempotency / extend-only rules.
func TestRetainPermanentAndTimed(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "o", []byte("v1-bytes"))
	putBytes(t, s, "o", []byte("v2-bytes"))

	// Permanent hold on the old version.
	ri, err := s.Retain(ctx, "o", v1.Version, time.Time{})
	if err != nil {
		t.Fatalf("Retain permanent: %v", err)
	}
	if !ri.Retained || !ri.Permanent || !ri.Until.IsZero() {
		t.Fatalf("want permanent hold, got %+v", ri)
	}
	t.Logf("result: v1 permanent hold active (basis: Retained=true Permanent=true)")

	// Repeating the same permanent hold must be a side-effect-free no-op
	// (no commit, no error). A shorter timed request cannot weaken it.
	short := clk.now().Add(time.Hour)
	if ri2, err := s.Retain(ctx, "o", v1.Version, time.Time{}); err != nil || !ri2.Permanent {
		t.Fatalf("repeat permanent hold changed state: ri=%+v err=%v", ri2, err)
	}
	if ri3, err := s.Retain(ctx, "o", v1.Version, short); err != nil || !ri3.Permanent {
		t.Fatalf("timed request must not weaken permanent hold: ri=%+v err=%v", ri3, err)
	}
	t.Logf("result: repeated/permanent-vs-timed retains are idempotent, hold never weakened")

	// Timed hold on v2.
	expiry := clk.now().Add(24 * time.Hour)
	ri, err = s.Retain(ctx, "o", 2, expiry)
	if err != nil || !ri.Retained || ri.Permanent || !ri.Until.Equal(expiry) {
		t.Fatalf("want timed hold until %s, got %+v err=%v", expiry, ri, err)
	}

	// Extend-only: an earlier expiry must be ignored.
	earlier := clk.now().Add(time.Hour)
	ri2, err := s.Retain(ctx, "o", 2, earlier)
	if err != nil || !ri2.Until.Equal(expiry) {
		t.Fatalf("retain must not shorten hold: got %+v err=%v", ri2, err)
	}
	// A later expiry extends.
	later := clk.now().Add(48 * time.Hour)
	ri2, err = s.Retain(ctx, "o", 2, later)
	if err != nil || !ri2.Until.Equal(later) {
		t.Fatalf("retain must extend hold: got %+v err=%v", ri2, err)
	}
	t.Logf("result: timed hold extend-only (shorter %s ignored, extended to %s)", earlier, later)

	// An expiry in the past is rejected.
	if _, err := s.Retain(ctx, "o", 2, clk.now().Add(-time.Minute)); err == nil {
		t.Fatalf("expected error for past expiry")
	}

	// Query via Retention, ListRetained and VersionInfo.
	got, err := s.Retention(ctx, "o", v1.Version)
	if err != nil || !got.Retained || !got.Permanent {
		t.Fatalf("Retention query: %+v err=%v", got, err)
	}
	list, err := s.ListRetained(ctx, "o")
	if err != nil || len(list) != 2 {
		t.Fatalf("ListRetained want 2 held versions, got %+v err=%v", list, err)
	}
	if list[0].Version != 1 || !list[0].Permanent || !list[1].Until.Equal(later) {
		t.Fatalf("ListRetained order/state wrong: %+v", list)
	}
	vs, err := s.Versions(ctx, "o")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if !vs[0].Retained || !vs[0].PermanentHold || !vs[1].Retained {
		t.Fatalf("VersionInfo retention flags wrong: %+v %+v", vs[0], vs[1])
	}
	t.Logf("result: hold state queryable through Retention/ListRetained/VersionInfo")

	// Error classification.
	if _, err := s.Retention(ctx, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown object: got %v want ErrNotFound", err)
	}
	if _, err := s.Retention(ctx, "o", 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown version: got %v want ErrNotFound", err)
	}
	if _, err := s.ListRetained(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ListRetained unknown object: got %v want ErrNotFound", err)
	}
}

// TestRetainSurvivesRestart asserts hold state is durable: a permanent
// hold placed before closing still protects data after reopening.
func TestRetainSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	v1 := putBytes(t, s, "a", []byte("audit-data"))
	putBytes(t, s, "a", []byte("newer-data"))
	expiry := time.Now().UTC().Add(7 * 24 * time.Hour)
	if _, err := s.Retain(ctx, "a", v1.Version, expiry); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if _, err := s.PurgeVersion(ctx, "a", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("before restart: purge held version got %v want ErrRetained", err)
	}

	// Restart the process (close + reopen the same directory).
	s = reopen(t, s, dir, opts)
	ri, err := s.Retention(ctx, "a", v1.Version)
	if err != nil {
		t.Fatalf("retention after restart: %v", err)
	}
	if !ri.Retained || ri.Permanent || !ri.Until.Equal(expiry) {
		t.Fatalf("hold lost after restart: %+v", ri)
	}
	if _, err := s.PurgeVersion(ctx, "a", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("after restart: purge held version got %v want ErrRetained", err)
	}
	// And the bytes are still physically present and readable.
	if string(getVersionBody(t, s, "a", v1.Version)) != "audit-data" {
		t.Fatalf("retained bytes damaged across restart")
	}
	t.Logf("result: timed hold durable across restart, protection intact, bytes readable")
}

// TestReleaseAndExpiry covers explicit early release and natural expiry,
// after which destructive operations succeed.
func TestReleaseAndExpiry(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "o", []byte("v1-bytes"))
	putBytes(t, s, "o", []byte("v2-bytes"))
	putBytes(t, s, "o", []byte("v3-bytes")) // v1 non-current so it is purgable
	expiry := clk.now().Add(time.Hour)

	// Early release of a permanent hold.
	if _, err := s.Retain(ctx, "o", v1.Version, time.Time{}); err != nil {
		t.Fatalf("retain: %v", err)
	}
	ri, err := s.ReleaseRetention(ctx, "o", v1.Version)
	if err != nil || ri.Retained {
		t.Fatalf("release permanent: %+v err=%v", ri, err)
	}
	// Releasing again is a no-op success.
	if ri, err := s.ReleaseRetention(ctx, "o", v1.Version); err != nil || ri.Retained {
		t.Fatalf("repeat release: %+v err=%v", ri, err)
	}
	// Now purge is allowed.
	if _, err := s.PurgeVersion(ctx, "o", v1.Version); err != nil {
		t.Fatalf("purge after release: %v", err)
	}
	t.Logf("result: explicit early release lifts protection, purge succeeds, release idempotent")

	// Timed hold on v2: blocked while active, allowed after expiry.
	if _, err := s.Retain(ctx, "o", 2, expiry); err != nil {
		t.Fatalf("retain v2: %v", err)
	}
	if _, err := s.PurgeVersion(ctx, "o", 2); !errors.Is(err, ErrRetained) {
		t.Fatalf("purge unexpired: got %v want ErrRetained", err)
	}
	clk.set(expiry) // exactly at expiry: not active
	ri, err = s.Retention(ctx, "o", 2)
	if err != nil || ri.Retained {
		t.Fatalf("expiry instant should be inactive: %+v err=%v", ri, err)
	}
	list, _ := s.ListRetained(ctx, "o")
	if len(list) != 0 {
		t.Fatalf("expired holds must not appear in ListRetained: %+v", list)
	}
	if _, err := s.PurgeVersion(ctx, "o", 2); err != nil {
		t.Fatalf("purge after expiry: %v", err)
	}
	if _, _, gerr := s.GetVersion(ctx, "o", 2); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("expired-then-purged read: got %v want ErrCollected", gerr)
	}
	t.Logf("result: expired hold no longer protects; purge yields ErrCollected on later read")
}

// TestRetainBlocksDelete verifies soft deletes (object-level and version)
// are refused while a hold is active and allowed after release; reads
// remain unaffected by holds.
func TestRetainBlocksDelete(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "o", []byte("first"))
	v2 := putBytes(t, s, "o", []byte("second"))
	// Hold the CURRENT version: object-level Delete must be refused.
	if _, err := s.Retain(ctx, "o", v2.Version, clk.now().Add(time.Hour)); err != nil {
		t.Fatalf("retain current: %v", err)
	}
	if _, err := s.Delete(ctx, "o"); !errors.Is(err, ErrRetained) {
		t.Fatalf("delete held current: got %v want ErrRetained", err)
	}
	// Reads still work and report the hold via VersionInfo.
	if string(getBody(t, s, "o")) != "second" {
		t.Fatalf("hold must not affect reads")
	}
	if _, _, gerr := s.GetVersion(ctx, "o", v2.Version); gerr != nil {
		t.Fatalf("held version must still be readable: %v", gerr)
	}
	// Hold the old non-current version: DeleteVersion must be refused.
	if _, err := s.Retain(ctx, "o", v1.Version, time.Time{}); err != nil {
		t.Fatalf("retain v1: %v", err)
	}
	if _, err := s.DeleteVersion(ctx, "o", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("delete held version: got %v want ErrRetained", err)
	}
	// Release the current hold; object Delete now works, and the held old
	// version stays protected and restorable.
	if _, err := s.ReleaseRetention(ctx, "o", v2.Version); err != nil {
		t.Fatalf("release current: %v", err)
	}
	if _, err := s.Delete(ctx, "o"); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
	// Releasing the old hold and restoring the object works end to end.
	if _, err := s.ReleaseRetention(ctx, "o", v1.Version); err != nil {
		t.Fatalf("release old: %v", err)
	}
	if _, err := s.Restore(ctx, "o"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Logf("result: holds block Delete/DeleteVersion with ErrRetained, never block reads")
}

// TestRetainOnDeletedVersion verifies a hold can be put on a
// soft-deleted version (audit snapshot), and even aggressive Sweep cannot
// reclaim it until release/expiry.
func TestRetainOnDeletedVersion(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SweepDeletedVersions: true}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	v1 := putBytes(t, s, "g", []byte("deleted-and-held"))
	putBytes(t, s, "alive", []byte("keep"))
	if _, err := s.Delete(ctx, "g"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Retain(ctx, "g", v1.Version, clk.now().Add(30*24*time.Hour)); err != nil {
		t.Fatalf("retain deleted version: %v", err)
	}
	st, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep with hold: %v", err)
	}
	if blobFileCount(t, dir) != 2 {
		t.Fatalf("held deleted blob must survive aggressive sweep, blobs=%d", blobFileCount(t, dir))
	}
	if st.RetainedVersions != 1 {
		t.Fatalf("stats retained count: got %d want 1", st.RetainedVersions)
	}
	// After expiry, the same sweep collects it.
	clk.advance(31 * 24 * time.Hour)
	st, err = s.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep after expiry: %v", err)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("expired held blob should be collected, blobs=%d", blobFileCount(t, dir))
	}
	if _, _, gerr := s.GetVersion(ctx, "g", v1.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("collected read: got %v want ErrCollected", gerr)
	}
	t.Logf("result: held soft-deleted data spared by aggressive Sweep, collected after expiry")
}
