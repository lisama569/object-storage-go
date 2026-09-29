package objectstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func isNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

// TestEdgeWatermarkEmptyStore verifies watermark operations on an empty
// store are a successful no-op.
func TestEdgeWatermarkEmptyStore(t *testing.T) {
	s, _ := newTestStore(t, Options{MaxPhysicalBytes: 100})
	rep, st, err := s.SweepToWatermark(context.Background())
	if err != nil {
		t.Fatalf("empty watermark sweep: %v", err)
	}
	if !rep.BelowWatermark || st.OnDiskBytes != 0 || rep.BytesReclaimed != 0 {
		t.Fatalf("empty report wrong: %+v %+v", rep, st)
	}
}

// TestEdgeRetentionNotFoundCases verifies every retention entry point
// classifies unknown objects/versions as ErrNotFound.
func TestEdgeRetentionNotFoundCases(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{"RetentionOf", func() error { _, e := s.RetentionOf(ctx, "nope", 1); return e }},
		{"Retentions", func() error { _, e := s.Retentions(ctx, "nope"); return e }},
		{"Release", func() error { _, e := s.ReleaseRetention(ctx, "nope", 1); return e }},
		{"Extend", func() error { _, e := s.ExtendRetention(ctx, "nope", 1, RetainPermanent()); return e }},
		{"Put", func() error { _, e := s.PutRetention(ctx, "nope", 1, RetainPermanent()); return e }},
	}
	for _, c := range checks {
		if err := c.call(); !isNotFound(err) {
			t.Fatalf("%s unknown: got %v want ErrNotFound", c.name, err)
		}
	}
}

// TestManifestRetentionFieldsRoundTrip ensures retention metadata is stored
// in the manifest and survives a literal on-disk reload (new fields are
// persisted), and that an old-style manifest without the fields still loads
// with the version treated as unprotected (no migration required).
func TestManifestRetentionFieldsRoundTrip(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	ctx := context.Background()
	v := putBytes(t, s, "o", []byte("round-trip"))
	if _, err := s.PutRetention(ctx, "o", v.Version, RetainPermanent()); err != nil {
		t.Fatalf("hold: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(raw), "retain_until") {
		t.Fatalf("manifest missing persisted retention field:\n%s", raw)
	}

	// Old-style manifest (no retention fields) must load cleanly. The digest
	// is syntactically valid so loading does not fail; the blob need not
	// exist (recoverLocked only reports missing blobs).
	oldStyle := `{
      "objects": {
        "leg": {
          "current": 1,
          "next": 2,
          "versions": {
            "1": {"version":1,"size":3,"digest":"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","created":"","state":0}
          }
        }
      }
    }`
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte(oldStyle), 0o600); err != nil {
		t.Fatalf("write old manifest: %v", err)
	}
	s2, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatalf("reopen with old-style manifest: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	st, err := s2.RetentionOf(context.Background(), "leg", 1)
	if err != nil || st.Retained {
		t.Fatalf("old manifest version must load as unprotected: %+v err=%v", st, err)
	}
}
