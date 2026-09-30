package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestDeletedRetainedReadReasonDistinct covers the audit-archive case: a
// version is soft-deleted first and placed under retention afterwards.
// Reading it must let the caller distinguish *all three* facts at once:
//
//   - errors.Is(err, ErrDeleted)           : data present, restorable;
//   - errors.Is(err, ErrRetained)          : still inside its hold;
//   - errors.Is(err, ErrDeletedRetained)   : exactly this combination.
//
// A plain soft-deleted version (hold released / expired / never held)
// must NOT report ErrRetained. The distinction must survive a restart
// because it is derived purely from persisted metadata.
func TestDeletedRetainedReadReasonDistinct(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SweepDeletedVersions: true, WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	// Case A: deleted first, then retained (permanent + timed variants).
	permV := putBytes(t, s, "perm", []byte("deleted-then-permanently-held"))
	timedV := putBytes(t, s, "timed", []byte("deleted-then-timed-held!!!!"))
	plainV := putBytes(t, s, "plain", []byte("deleted-never-held-bytes!!"))
	if _, err := s.Delete(ctx, "perm"); err != nil {
		t.Fatalf("delete perm: %v", err)
	}
	if _, err := s.Delete(ctx, "timed"); err != nil {
		t.Fatalf("delete timed: %v", err)
	}
	if _, err := s.Delete(ctx, "plain"); err != nil {
		t.Fatalf("delete plain: %v", err)
	}
	if _, err := s.Retain(ctx, "perm", permV.Version, time.Time{}); err != nil {
		t.Fatalf("retain perm: %v", err)
	}
	timedExpiry := clk.now().Add(7 * 24 * time.Hour)
	if _, err := s.Retain(ctx, "timed", timedV.Version, timedExpiry); err != nil {
		t.Fatalf("retain timed: %v", err)
	}

	wantHeld := func(t *testing.T, label string, gerr error) {
		t.Helper()
		if !errors.Is(gerr, ErrDeletedRetained) {
			t.Fatalf("%s: got %v want ErrDeletedRetained", label, gerr)
		}
		if !errors.Is(gerr, ErrDeleted) {
			t.Fatalf("%s: held+deleted read must still match ErrDeleted: %v", label, gerr)
		}
		if !errors.Is(gerr, ErrRetained) {
			t.Fatalf("%s: held+deleted read must match ErrRetained so the hold is visible: %v", label, gerr)
		}
		if errors.Is(gerr, ErrCollected) || errors.Is(gerr, ErrNotFound) {
			t.Fatalf("%s: hold must not demote the error class: %v", label, gerr)
		}
	}

	// GetVersion path.
	_, _, gerr := s.GetVersion(ctx, "perm", permV.Version)
	wantHeld(t, "perm GetVersion", gerr)
	_, _, gerr = s.GetVersion(ctx, "timed", timedV.Version)
	wantHeld(t, "timed GetVersion", gerr)
	// Get (object-level tombstone) path.
	_, _, gerr = s.Get(ctx, "perm")
	wantHeld(t, "perm Get", gerr)
	_, _, gerr = s.Get(ctx, "timed")
	wantHeld(t, "timed Get", gerr)
	t.Logf("judgment: deleted+held reads report ErrDeletedRetained (matches ErrDeleted and ErrRetained)")

	// The VersionInfo delivered with the rejected read still carries the
	// hold state explicitly.
	_, info, err := s.GetVersion(ctx, "timed", timedV.Version)
	if err == nil {
		t.Fatalf("expected rejection")
	}
	if !info.Retained || info.RetainUntil.IsZero() {
		t.Fatalf("rejected read info must expose hold: %+v", info)
	}

	// Plain deleted: ErrDeleted but never ErrRetained/ErrDeletedRetained.
	if _, _, gerr := s.GetVersion(ctx, "plain", plainV.Version); !errors.Is(gerr, ErrDeleted) ||
		errors.Is(gerr, ErrRetained) {
		t.Fatalf("plain deleted read: got %v want ErrDeleted without ErrRetained", gerr)
	}
	t.Logf("judgment: plain deleted read is ErrDeleted only (hold reason absent)")

	// Release the timed hold: still deleted, but the hold reason disappears.
	if _, err := s.ReleaseRetention(ctx, "timed", timedV.Version); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, _, gerr := s.GetVersion(ctx, "timed", timedV.Version); !errors.Is(gerr, ErrDeleted) ||
		errors.Is(gerr, ErrRetained) {
		t.Fatalf("after release: got %v want plain ErrDeleted", gerr)
	}
	// Re-retain: the combination is back.
	if _, err := s.Retain(ctx, "timed", timedV.Version, timedExpiry); err != nil {
		t.Fatalf("re-retain: %v", err)
	}
	if _, _, gerr := s.GetVersion(ctx, "timed", timedV.Version); !errors.Is(gerr, ErrDeletedRetained) {
		t.Fatalf("after re-retain: got %v want ErrDeletedRetained", gerr)
	}
	t.Logf("judgment: release clears the hold reason; re-retaining restores it")

	// Expiry makes the hold reason lapse while the version stays deleted.
	clk.set(timedExpiry) // exactly at expiry: hold inactive
	if _, _, gerr := s.GetVersion(ctx, "timed", timedV.Version); !errors.Is(gerr, ErrDeleted) ||
		errors.Is(gerr, ErrRetained) {
		t.Fatalf("after expiry: got %v want plain ErrDeleted", gerr)
	}
	// Re-establish the hold before the pressure sweep below, so the durable
	// state at restart shows the deleted+held combination again.
	clk.set(base)
	if _, err := s.Retain(ctx, "timed", timedV.Version, timedExpiry); err != nil {
		t.Fatalf("re-retain after expiry probe: %v", err)
	}

	// Under watermark pressure the held version is neither collected nor
	// downgraded to ErrCollected.
	if _, werr := s.Sweep(ctx); !errors.Is(werr, ErrWatermarkExceeded) {
		t.Fatalf("sweep: got %v want ErrWatermarkExceeded", werr)
	}
	if _, _, gerr := s.GetVersion(ctx, "perm", permV.Version); !errors.Is(gerr, ErrDeletedRetained) {
		t.Fatalf("held version changed class after pressure sweep: %v", gerr)
	}

	// Restart: the same classification must come from durable metadata.
	s2 := reopen(t, s, dir, opts)
	withFakeClock(s2, base) // before expiry again, for the timed variant
	_, _, gerr = s2.GetVersion(ctx, "perm", permV.Version)
	wantHeld(t, "perm after restart", gerr)
	_, _, gerr = s2.Get(ctx, "timed")
	wantHeld(t, "timed after restart", gerr)
	if _, _, gerr := s2.GetVersion(ctx, "plain", plainV.Version); errors.Is(gerr, ErrRetained) {
		t.Fatalf("plain deleted after restart must not look held: %v", gerr)
	}
	t.Logf("result: deleted+held read reason distinct, survives restart; released/expired reads are plain ErrDeleted")
}
