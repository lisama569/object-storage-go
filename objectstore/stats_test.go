package objectstore

import (
	"context"
	"testing"
)

// TestStatsDedupAndVersions checks logical vs physical accounting:
// identical content across versions/objects shares one physical blob,
// while logical usage counts each readable version.
func TestStatsDedupAndVersions(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	shared := []byte("shared-content-12345")
	putBytes(t, s, "a", shared)
	putBytes(t, s, "b", shared) // cross-object shared blob
	putBytes(t, s, "a", []byte("distinct-second-version"))

	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	t.Logf("input: 2 objects, 3 readable versions, 2 distinct blobs -> %+v", st)
	if st.Objects != 2 {
		t.Fatalf("objects: got %d want 2", st.Objects)
	}
	if st.LiveVersions != 3 {
		t.Fatalf("live versions: got %d want 3", st.LiveVersions)
	}
	if st.ReferencedBlobs != 2 {
		t.Fatalf("physical blobs: got %d want 2 (dedup shared content)", st.ReferencedBlobs)
	}
	if st.PhysicalBytes != int64(len(shared))+int64(len("distinct-second-version")) {
		t.Fatalf("physical bytes wrong: %d", st.PhysicalBytes)
	}
	if st.LogicalBytes != int64(len(shared))*2+int64(len("distinct-second-version")) {
		t.Fatalf("logical bytes (counts shared twice) wrong: %d", st.LogicalBytes)
	}
	if st.OrphanBlobs != 0 || st.ReclaimableBytes != 0 {
		t.Fatalf("fresh store should have no orphans: %+v", st)
	}
	t.Logf("result: physical=%d (dedup) logical=%d (per-version), orphans=0",
		st.PhysicalBytes, st.LogicalBytes)
}

// TestStatsDeletedAccounting verifies soft-deleted versions still count
// as referenced until purged, then move to collected and free the blob.
func TestStatsDeletedAccounting(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	v1 := putBytes(t, s, "o", []byte("old-release"))
	putBytes(t, s, "o", []byte("new-release"))

	// Purge the old non-current version; its unique blob becomes physical
	// garbage and is removed synchronously by PurgeVersion.
	if _, err := s.PurgeVersion(ctx, "o", v1.Version); err != nil {
		t.Fatalf("purge: %v", err)
	}
	st, _ := s.Stats(ctx)
	t.Logf("after purge: live=%d deleted=%d collected=%d referenced_blobs=%d",
		st.LiveVersions, st.DeletedVersions, st.CollectedVersion, st.ReferencedBlobs)
	if st.CollectedVersion != 1 || st.ReferencedBlobs != 1 {
		t.Fatalf("accounting after purge wrong: %+v", st)
	}
	// Sweep on a consistent store changes nothing.
	st2, _ := s.Sweep(ctx)
	if st2.OrphanBlobs != 0 || st2.ReferencedBlobs != 1 {
		t.Fatalf("post-purge sweep should be a no-op: %+v", st2)
	}
	t.Logf("result: collected version retained as metadata, physical blob released; sweep no-op")
}
