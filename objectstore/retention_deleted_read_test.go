package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// assertDeletedAndRetained checks that a read error simultaneously
// classifies as ErrDeleted (soft-deleted, data still present/restorable)
// and ErrRetained (an effective hold is additionally protecting it).
func assertDeletedAndRetained(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("%s: error must match ErrDeleted, got %v", label, err)
	}
	if !errors.Is(err, ErrRetained) {
		t.Fatalf("%s: error must additionally match ErrRetained so callers can "+
			"tell the deleted version is still hold-protected, got %v", label, err)
	}
}

// TestReadDeletedRetainedVersionDistinguishesHold covers the audit-archive
// scenario: a version is soft-deleted first and put under retention
// afterwards. Reading it must fail, but the rejection reason has to expose
// BOTH "deleted" and "still under retention" independently, both through
// GetVersion and through object-level Get, and after a process restart
// (the distinction comes from durable manifest fields, not memory).
func TestReadDeletedRetainedVersionDistinguishesHold(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := withFakeClock(s, base)

	// Non-current version: soft-deleted via DeleteVersion, then held.
	v1 := putBytes(t, s, "o", []byte("first-snapshot"))
	putBytes(t, s, "o", []byte("second-snapshot")) // keeps v1 non-current
	if _, err := s.DeleteVersion(ctx, "o", v1.Version); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	expiry := clk.now().Add(30 * 24 * time.Hour)
	if _, err := s.Retain(ctx, "o", v1.Version, expiry); err != nil {
		t.Fatalf("retain deleted v1: %v", err)
	}
	_, _, gerr := s.GetVersion(ctx, "o", v1.Version)
	assertDeletedAndRetained(t, "GetVersion deleted+timed-held v1", gerr)
	t.Logf("judgment: GetVersion error matches both ErrDeleted and ErrRetained: %v", gerr)

	// VersionInfo must also still report the hold state.
	ri, err := s.Retention(ctx, "o", v1.Version)
	if err != nil || !ri.Retained || !ri.Until.Equal(expiry) {
		t.Fatalf("retention query on deleted held version: %+v err=%v", ri, err)
	}

	// Object-level tombstone: Delete first, then retain the hidden current.
	hd := putBytes(t, s, "tomb", []byte("tombstone-current-data"))
	if _, err := s.Delete(ctx, "tomb"); err != nil {
		t.Fatalf("delete tomb: %v", err)
	}
	if _, err := s.Retain(ctx, "tomb", hd.Version, time.Time{}); err != nil {
		t.Fatalf("retain deleted current: %v", err)
	}
	_, _, gerr = s.Get(ctx, "tomb")
	assertDeletedAndRetained(t, "Get tombstoned permanent-held object", gerr)
	_, _, gerr = s.GetVersion(ctx, "tomb", hd.Version)
	assertDeletedAndRetained(t, "GetVersion tombstoned permanent-held version", gerr)
	t.Logf("judgment: tombstoned object read error matches both sentinels: %v", gerr)

	// Control cases:
	//  - a plain soft-deleted version with no hold matches ErrDeleted only;
	//    it must NOT look retained.
	plain := putBytes(t, s, "p", []byte("plain-deleted-data"))
	if _, err := s.Delete(ctx, "p"); err != nil {
		t.Fatalf("delete p: %v", err)
	}
	_, _, gerr = s.GetVersion(ctx, "p", plain.Version)
	if !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("plain deleted must match ErrDeleted: %v", gerr)
	}
	if errors.Is(gerr, ErrRetained) {
		t.Fatalf("plain deleted must not match ErrRetained: %v", gerr)
	}
	//  - after the hold expires, only ErrDeleted remains.
	clk.set(expiry)
	_, _, gerr = s.GetVersion(ctx, "o", v1.Version)
	if !errors.Is(gerr, ErrDeleted) || errors.Is(gerr, ErrRetained) {
		t.Fatalf("expired hold: want ErrDeleted only, got %v", gerr)
	}
	//  - an explicitly released hold likewise drops the retained reason.
	if _, err := s.ReleaseRetention(ctx, "tomb", hd.Version); err != nil {
		t.Fatalf("release tomb: %v", err)
	}
	_, _, gerr = s.Get(ctx, "tomb")
	if !errors.Is(gerr, ErrDeleted) || errors.Is(gerr, ErrRetained) {
		t.Fatalf("released hold: want ErrDeleted only, got %v", gerr)
	}
	t.Logf("judgment: plain/expired/released deleted versions match ErrDeleted only")

	// Re-arm holds, then prove the distinction survives a process restart.
	clk.set(base)
	if _, err := s.Retain(ctx, "o", v1.Version, expiry); err != nil {
		t.Fatalf("re-retain v1: %v", err)
	}
	if _, err := s.Retain(ctx, "tomb", hd.Version, time.Time{}); err != nil {
		t.Fatalf("re-retain tomb permanent: %v", err)
	}
	s2 := reopen(t, s, dir, opts)
	clk2 := withFakeClock(s2, base)
	_, _, gerr = s2.GetVersion(ctx, "o", v1.Version)
	assertDeletedAndRetained(t, "after restart GetVersion timed-held", gerr)
	_, _, gerr = s2.Get(ctx, "tomb")
	assertDeletedAndRetained(t, "after restart Get permanent-held", gerr)
	// The bytes are still physically present and become readable again
	// after restoring; the hold keeps blocking reclaim meanwhile.
	if _, err := s2.PurgeVersion(ctx, "tomb", hd.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("purge held deleted version after restart: got %v want ErrRetained", err)
	}
	if _, err := s2.RestoreVersion(ctx, "tomb", hd.Version); err != nil {
		t.Fatalf("restore held deleted version: %v", err)
	}
	if body := getVersionBody(t, s2, "tomb", hd.Version); string(body) != "tombstone-current-data" {
		t.Fatalf("restored bytes wrong: %q", body)
	}
	// After time passes past expiry and another restart, the retained
	// reason must be gone while ErrDeleted persists.
	clk2.set(expiry)
	s3 := reopen(t, s2, dir, opts)
	withFakeClock(s3, expiry)
	_, _, gerr = s3.GetVersion(ctx, "o", v1.Version)
	if !errors.Is(gerr, ErrDeleted) || errors.Is(gerr, ErrRetained) {
		t.Fatalf("after restart past expiry: want ErrDeleted only, got %v", gerr)
	}
	t.Logf("result: deleted+retained rejection reason is distinct and durable across restarts")
}
