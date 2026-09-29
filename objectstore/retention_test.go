package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeClock installs a controllable clock on s and returns a pointer the
// test advances to simulate retention expiry without sleeping.
func fakeClock(t *testing.T, s *Store, start time.Time) *time.Time {
	t.Helper()
	clk := start.UTC()
	s.now = func() time.Time { return clk }
	return &clk
}

// TestRetentionSetQueryIdempotent covers setting a time-bounded hold,
// observing it via per-version and per-object queries, restart durability,
// and that repeating the same/weaker set is side-effect free.
func TestRetentionSetQueryIdempotent(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	v1 := putBytes(t, s, "audited", []byte("audit-me"))
	putBytes(t, s, "audited", []byte("newer")) // v2 is current; v1 is old

	deadline := now.Add(30 * 24 * time.Hour)
	st, err := s.PutRetention(ctx, "audited", v1.Version, RetainUntil(deadline))
	if err != nil {
		t.Fatalf("PutRetention: %v", err)
	}
	if !st.Retained || st.Permanent || !st.Until.Equal(deadline) {
		t.Fatalf("unexpected status: %+v", st)
	}
	t.Logf("set hold on v1 until %s -> retained=%v", st.Until, st.Retained)

	// Per-version query.
	got, err := s.RetentionOf(ctx, "audited", v1.Version)
	if err != nil || !got.Retained || !got.Until.Equal(deadline) {
		t.Fatalf("RetentionOf: %+v err=%v", got, err)
	}
	// Per-object query lists both versions; only v1 is held.
	all, err := s.Retentions(ctx, "audited")
	if err != nil || len(all) != 2 || !all[0].Retained || all[1].Retained {
		t.Fatalf("Retentions: %+v err=%v", all, err)
	}
	// VersionInfo exposes the active hold too.
	_, vi, err := s.GetVersion(ctx, "audited", v1.Version)
	if err != nil {
		t.Fatalf("get old active version: %v", err)
	}
	if !vi.Retained || vi.Permanent || !vi.RetainUntil.Equal(deadline) {
		t.Fatalf("VersionInfo retention fields wrong: %+v", vi)
	}

	// Re-apply the same deadline and a strictly weaker one: no change.
	if _, err := s.PutRetention(ctx, "audited", v1.Version, RetainUntil(deadline)); err != nil {
		t.Fatalf("re-set same: %v", err)
	}
	if _, err := s.PutRetention(ctx, "audited", v1.Version, RetainUntil(deadline.Add(-time.Hour))); err != nil {
		t.Fatalf("set weaker: %v", err)
	}
	again, _ := s.RetentionOf(ctx, "audited", v1.Version)
	if !again.Until.Equal(deadline) || !again.Retained {
		t.Fatalf("weaker re-set must not shorten hold: %+v", again)
	}
	t.Logf("idempotent re-set preserved deadline %s", again.Until)

	// Stronger re-set extends.
	later := deadline.Add(7 * 24 * time.Hour)
	if _, err := s.PutRetention(ctx, "audited", v1.Version, RetainUntil(later)); err != nil {
		t.Fatalf("extend via set: %v", err)
	}
	after, _ := s.RetentionOf(ctx, "audited", v1.Version)
	if !after.Until.Equal(later) {
		t.Fatalf("stronger set should extend: %+v", after)
	}

	// Unknown / collected targets fail distinctly.
	if _, err := s.PutRetention(ctx, "ghost", 1, RetainUntil(later)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("hold unknown object: %v", err)
	}
	if _, err := s.PutRetention(ctx, "audited", 99, RetainUntil(later)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("hold unknown version: %v", err)
	}

	// Durability: protection survives a process restart.
	s2 := reopen(t, s, dir, Options{})
	s2.now = func() time.Time { return *clk }
	persisted, err := s2.RetentionOf(ctx, "audited", v1.Version)
	if err != nil || !persisted.Retained || !persisted.Until.Equal(later) {
		t.Fatalf("retention lost across restart: %+v err=%v", persisted, err)
	}
	if string(getVersionBody(t, s2, "audited", v1.Version)) != "audit-me" {
		t.Fatalf("retained old version no longer readable")
	}
	t.Logf("result: hold survived restart until %s; bytes readable", persisted.Until)
}

// TestRetentionPermanentAndExtend verifies permanent holds never expire,
// Extend only lengthens, and the zero policy is rejected.
func TestRetentionPermanentAndExtend(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	v := putBytes(t, s, "p", []byte("forever"))

	// Empty policy is rejected.
	if _, err := s.PutRetention(ctx, "p", v.Version, Retention{}); err == nil {
		t.Fatalf("empty retention must be rejected")
	}

	if _, err := s.PutRetention(ctx, "p", v.Version, RetainPermanent()); err != nil {
		t.Fatalf("set permanent: %v", err)
	}
	// Advance a very long time: still retained.
	*clk = now.Add(100 * 365 * 24 * time.Hour)
	st, _ := s.RetentionOf(ctx, "p", v.Version)
	if !st.Retained || !st.Permanent {
		t.Fatalf("permanent hold expired: %+v", st)
	}
	// Finite extension of permanent is a no-op (cannot weaken).
	if _, err := s.ExtendRetention(ctx, "p", v.Version, RetainUntil(now.Add(time.Hour))); err != nil {
		t.Fatalf("extend permanent with finite: %v", err)
	}
	st2, _ := s.RetentionOf(ctx, "p", v.Version)
	if !st2.Permanent {
		t.Fatalf("permanent hold weakened by extend: %+v", st2)
	}
	// Extending a released/absent hold is an error.
	f := putBytes(t, s, "f", []byte("no-hold"))
	if _, err := s.ExtendRetention(ctx, "f", f.Version, RetainUntil(now.Add(time.Hour))); !errors.Is(err, ErrRetained) {
		t.Fatalf("extend without hold: got %v want ErrRetained", err)
	}

	// Finite hold: shorter extend is a no-op, longer extend applies.
	base := *clk
	v2 := putBytes(t, s, "x", []byte("extendable"))
	if _, err := s.PutRetention(ctx, "x", v2.Version, RetainUntil(base.Add(time.Hour))); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := s.ExtendRetention(ctx, "x", v2.Version, RetainUntil(base.Add(30*time.Minute))); err != nil {
		t.Fatalf("shorter extend: %v", err)
	}
	st3, _ := s.RetentionOf(ctx, "x", v2.Version)
	if !st3.Until.Equal(base.Add(time.Hour)) {
		t.Fatalf("shorter extend shortened hold: %+v", st3)
	}
	if _, err := s.ExtendRetention(ctx, "x", v2.Version, RetainUntil(base.Add(2*time.Hour))); err != nil {
		t.Fatalf("longer extend: %v", err)
	}
	st4, _ := s.RetentionOf(ctx, "x", v2.Version)
	if !st4.Until.Equal(base.Add(2 * time.Hour)) {
		t.Fatalf("longer extend did not apply: %+v", st4)
	}
	t.Logf("result: permanent never expires; extend is monotonic")
}

// TestRetentionReleaseAndExpiry verifies the two ways a hold stops
// protecting bytes: explicit release (durable, reclaimable first) and the
// deadline passing. Both allow reclamation afterwards; re-arming works.
func TestRetentionReleaseAndExpiry(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	rel := putBytes(t, s, "rel", []byte("will-release"))
	exp := putBytes(t, s, "exp", []byte("will-expire"))
	relDeadline := now.Add(7 * 24 * time.Hour)
	expDeadline := now.Add(24 * time.Hour)
	if _, err := s.PutRetention(ctx, "rel", rel.Version, RetainUntil(relDeadline)); err != nil {
		t.Fatalf("hold rel: %v", err)
	}
	if _, err := s.PutRetention(ctx, "exp", exp.Version, RetainUntil(expDeadline)); err != nil {
		t.Fatalf("hold exp: %v", err)
	}

	// Release rel early.
	rs, err := s.ReleaseRetention(ctx, "rel", rel.Version)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if rs.Retained || rs.ReleasedAt.IsZero() {
		t.Fatalf("release did not clear hold: %+v", rs)
	}
	// Idempotent: releasing again is a no-op.
	if _, err := s.ReleaseRetention(ctx, "rel", rel.Version); err != nil {
		t.Fatalf("repeat release: %v", err)
	}
	// Re-arm after release works.
	if _, err := s.PutRetention(ctx, "rel", rel.Version, RetainUntil(relDeadline.Add(24*time.Hour))); err != nil {
		t.Fatalf("re-arm after release: %v", err)
	}
	if st, _ := s.RetentionOf(ctx, "rel", rel.Version); !st.Retained || !st.ReleasedAt.IsZero() {
		t.Fatalf("re-arm failed: %+v", st)
	}
	// Release again for the expiry/reclaim part of the test.
	if _, err := s.ReleaseRetention(ctx, "rel", rel.Version); err != nil {
		t.Fatalf("release 2: %v", err)
	}

	// Before exp's deadline it is still held; after it, it is expired.
	*clk = expDeadline.Add(-time.Second)
	if st, _ := s.RetentionOf(ctx, "exp", exp.Version); !st.Retained {
		t.Fatalf("exp should still be held one second before deadline")
	}
	*clk = expDeadline
	if st, _ := s.RetentionOf(ctx, "exp", exp.Version); st.Retained {
		t.Fatalf("exp should be expired at deadline: %+v", st)
	}

	// Both are now reclaimable. Make them soft-deleted and purge: rel was
	// released (class 1), exp expired (class 2) — purge must succeed for both.
	if _, err := s.Delete(ctx, "rel"); err != nil {
		t.Fatalf("delete rel: %v", err)
	}
	if _, err := s.Delete(ctx, "exp"); err != nil {
		t.Fatalf("delete exp: %v", err)
	}
	// PurgeVersion must allow the released version.
	if _, err := s.PurgeVersion(ctx, "rel", rel.Version); err != nil {
		t.Fatalf("purge released: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "rel", rel.Version); !errors.Is(err, ErrCollected) {
		t.Fatalf("released version should be collected: %v", err)
	}
	// The expired version survives restart as expired (not forgotten), and
	// is purgeable there too.
	s2 := reopen(t, s, dir, Options{})
	s2.now = func() time.Time { return *clk }
	st, _ := s2.RetentionOf(ctx, "exp", exp.Version)
	if st.Retained {
		t.Fatalf("expiry must survive restart (still expired): %+v", st)
	}
	relSt, _ := s2.RetentionOf(ctx, "rel", rel.Version)
	if relSt.Retained {
		t.Fatalf("release must survive restart: %+v", relSt)
	}
	if _, err := s2.PurgeVersion(ctx, "exp", exp.Version); err != nil {
		t.Fatalf("purge expired after restart: %v", err)
	}
	t.Logf("result: release and expiry both lift protection durably; purge allowed")
}

// TestRetentionBlocksPurgeAndSoftDeletedRead verifies:
//   - PurgeVersion on an actively held version returns ErrRetained;
//   - a soft-deleted but retained version reads as ErrDeleted AND ErrRetained
//     (distinct from a plain deleted version), and the bytes stay on disk;
//   - aggressive Sweep does not collect it, but does after release.
func TestRetentionBlocksPurgeAndSoftDeletedRead(t *testing.T) {
	s, dir := newTestStore(t, Options{SweepDeletedVersions: true})
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	clk := fakeClock(t, s, now)

	plain := putBytes(t, s, "plain", []byte("ordinary-delete"))
	held := putBytes(t, s, "held", []byte("held-delete"))
	if _, err := s.PutRetention(ctx, "held", held.Version, RetainUntil(now.Add(24*time.Hour))); err != nil {
		t.Fatalf("hold: %v", err)
	}

	// Purge an active held version is refused (it is non-current after a
	// newer put).
	putBytes(t, s, "held", []byte("held-current"))
	if _, err := s.PurgeVersion(ctx, "held", held.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("purge held: got %v want ErrRetained", err)
	}

	// Soft delete two objects; bytes kept on disk for both.
	if _, err := s.Delete(ctx, "plain"); err != nil {
		t.Fatalf("delete plain: %v", err)
	}
	// held's current v2: delete the object (tombstone) — retention on v1
	// stays attached to v1; also explicitly soft-delete v1 path is via
	// DeleteVersion only for non-current, so instead hold+delete a fresh obj.
	hold2 := putBytes(t, s, "held2", []byte("held-deleted-bytes"))
	if _, err := s.PutRetention(ctx, "held2", hold2.Version, RetainUntil(now.Add(24*time.Hour))); err != nil {
		t.Fatalf("hold2: %v", err)
	}
	if _, err := s.Delete(ctx, "held2"); err != nil {
		t.Fatalf("delete held2: %v", err)
	}

	// Read classification: plain -> ErrDeleted only; held2 -> ErrDeleted
	// and ErrRetained.
	_, _, perr := s.Get(ctx, "plain")
	if !errors.Is(perr, ErrDeleted) || errors.Is(perr, ErrRetained) {
		t.Fatalf("plain deleted classification wrong: %v", perr)
	}
	_, _, herr := s.Get(ctx, "held2")
	if !errors.Is(herr, ErrDeleted) {
		t.Fatalf("held read should be ErrDeleted: %v", herr)
	}
	if !errors.Is(herr, ErrRetained) {
		t.Fatalf("held read should also be ErrRetained: %v", herr)
	}
	if errors.Is(herr, ErrNotFound) {
		t.Fatalf("held read must not look like never-existed")
	}
	t.Logf("judgment: held soft-deleted read = ErrDeleted+ErrRetained, never ErrNotFound")

	// Aggressive Sweep must collect plain but never held2.
	if _, err := s.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "plain", plain.Version); !errors.Is(err, ErrCollected) {
		t.Fatalf("plain deleted should be collected under aggressive sweep: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "held2", hold2.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("held2 must be protected from sweep: %v", err)
	}
	if blobFileCount(t, dir) < 2 {
		t.Fatalf("held blob must remain on disk, blobs=%d", blobFileCount(t, dir))
	}
	// Restore still works and bytes are intact.
	if _, err := s.Restore(ctx, "held2"); err != nil {
		t.Fatalf("restore held2: %v", err)
	}
	if string(getBody(t, s, "held2")) != "held-deleted-bytes" {
		t.Fatalf("held2 bytes harmed while retained")
	}
	// After release + delete again, aggressive sweep can collect it.
	if _, err := s.ReleaseRetention(ctx, "held2", hold2.Version); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.Delete(ctx, "held2"); err != nil {
		t.Fatalf("re-delete: %v", err)
	}
	if _, err := s.Sweep(ctx); err != nil {
		t.Fatalf("sweep after release: %v", err)
	}
	if _, _, err := s.GetVersion(ctx, "held2", hold2.Version); !errors.Is(err, ErrCollected) {
		t.Fatalf("released held should be collectable: %v", err)
	}
	_ = clk
	t.Logf("result: retention blocks purge/sweep; read reason stays ErrDeleted(+ErrRetained)")
}
