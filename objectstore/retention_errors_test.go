package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestReadRejectionReasonsStayDistinct verifies that adding retention and
// watermark reclaim does not scramble the existing rejection taxonomy:
//
//   - never existed            -> ErrNotFound
//   - soft-deleted             -> ErrDeleted (data present, restorable)
//   - physically reclaimed     -> ErrCollected
//   - destructive op on held   -> ErrRetained
//   - purge current            -> ErrCurrent
//
// This is checked both before and after a restart to prove the
// classification comes from durable metadata, not memory.
func TestReadRejectionReasonsStayDistinct(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SweepDeletedVersions: true, WatermarkBytes: 1}
	s, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	v1 := putBytes(t, s, "a", []byte("a-v1"))
	putBytes(t, s, "a", []byte("a-v2")) // v1 old, v2 current
	hd := putBytes(t, s, "h", []byte("held-version!!"))

	// 1. Never existed.
	if _, _, err := s.GetVersion(ctx, "a", 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown version: got %v want ErrNotFound", err)
	}
	if _, _, err := s.Get(ctx, "never-existed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown object: got %v want ErrNotFound", err)
	}
	t.Logf("judgment: unknown object/version -> ErrNotFound")

	// 2. Retained old version: reads fine, destructive ops -> ErrRetained.
	if _, err := s.Retain(ctx, "a", v1.Version, time.Time{}); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if string(getVersionBody(t, s, "a", v1.Version)) != "a-v1" {
		t.Fatalf("held version must remain readable")
	}
	if _, err := s.PurgeVersion(ctx, "a", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("purge held: got %v want ErrRetained", err)
	}
	if _, err := s.DeleteVersion(ctx, "a", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("delete held: got %v want ErrRetained", err)
	}
	t.Logf("judgment: destructive op on held version -> ErrRetained; reads unaffected")

	// 3. purge current -> ErrCurrent even without any hold.
	if _, err := s.PurgeVersion(ctx, "a", 2); !errors.Is(err, ErrCurrent) {
		t.Fatalf("purge current: got %v want ErrCurrent", err)
	}
	t.Logf("judgment: purge current -> ErrCurrent")

	// 4. Soft-deleted held object under watermark pressure: Sweep must not
	// collect it; read stays ErrDeleted (not ErrCollected, not NotFound).
	if _, err := s.Delete(ctx, "h"); err != nil {
		t.Fatalf("delete h: %v", err)
	}
	if _, err := s.Retain(ctx, "h", hd.Version, time.Time{}); err != nil {
		t.Fatalf("retain deleted: %v", err)
	}
	if _, werr := s.Sweep(ctx); !errors.Is(werr, ErrWatermarkExceeded) {
		t.Fatalf("sweep pressure: got %v want ErrWatermarkExceeded", werr)
	}
	if _, _, gerr := s.GetVersion(ctx, "h", hd.Version); !errors.Is(gerr, ErrDeleted) {
		t.Fatalf("held deleted read must stay ErrDeleted: got %v", gerr)
	}
	t.Logf("judgment: held+deleted survives sweep; read -> ErrDeleted (not Collected/NotFound)")

	// 5. Release + sweep collects it; read now explicitly ErrCollected.
	if _, err := s.ReleaseRetention(ctx, "h", hd.Version); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, werr := s.Sweep(ctx); !errors.Is(werr, ErrWatermarkExceeded) {
		t.Fatalf("sweep after release: got %v want ErrWatermarkExceeded", werr)
	}
	if _, _, gerr := s.GetVersion(ctx, "h", hd.Version); !errors.Is(gerr, ErrCollected) {
		t.Fatalf("reclaimed read: got %v want ErrCollected", gerr)
	}
	t.Logf("judgment: after release+sweep, read -> ErrCollected (never demoted to ErrNotFound)")

	// 6. Same taxonomy after a process restart.
	s2 := reopen(t, s, dir, opts)
	var cases = []struct {
		name    string
		version int64
		want    error
	}{
		{"a", v1.Version, nil}, // still held, readable (restart kept hold)
		{"h", hd.Version, ErrCollected},
		{"a", 999, ErrNotFound},
		{"never-existed", 1, ErrNotFound},
	}
	for _, c := range cases {
		_, _, gerr := s2.GetVersion(ctx, c.name, c.version)
		if c.want == nil {
			if gerr != nil {
				t.Fatalf("%s v%d after restart: got %v want readable", c.name, c.version, gerr)
			}
			continue
		}
		if !errors.Is(gerr, c.want) {
			t.Fatalf("%s v%d after restart: got %v want %v", c.name, c.version, gerr, c.want)
		}
	}
	if _, err := s2.PurgeVersion(ctx, "a", v1.Version); !errors.Is(err, ErrRetained) {
		t.Fatalf("hold after restart: got %v want ErrRetained", err)
	}
	t.Logf("result: all rejection reasons stable across restart; hold protection persisted")
}
