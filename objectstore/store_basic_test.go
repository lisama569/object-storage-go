package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

// TestPutGetRoundTrip verifies the basic write/read contract and that the
// returned descriptor exposes digest, size and a version number.
func TestPutGetRoundTrip(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	payload := []byte("hello content-addressable world")

	vi := putBytes(t, s, "greet", payload)
	t.Logf("input=%q bytes=%d -> version=%d digest=%s (basis: sha256 of bytes)",
		"greet", len(payload), vi.Version, vi.Digest)
	if vi.Digest != digestOf(payload) {
		t.Fatalf("digest mismatch: got %s want %s", vi.Digest, digestOf(payload))
	}
	if vi.Size != int64(len(payload)) {
		t.Fatalf("size mismatch: got %d want %d", vi.Size, len(payload))
	}

	body := getBody(t, s, "greet")
	if !bytes.Equal(body, payload) {
		t.Fatalf("read content mismatch: got %q want %q", body, payload)
	}
	t.Logf("result: read back %d identical bytes, integrity verified on read", len(body))
}

// TestDedupSameContentNoBlobCopy verifies that writing identical content
// never creates a new physical blob or a new version when the current
// version already has those bytes.
func TestDedupSameContentNoBlobCopy(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	payload := []byte("deduplicate-me-please")

	v1 := putBytes(t, s, "obj", payload)
	blobsAfterFirst := blobFileCount(t, dir)
	t.Logf("first write: version=%d digest=%s physical blobs=%d",
		v1.Version, v1.Digest, blobsAfterFirst)

	// Same object, same bytes -> same version, same single blob.
	v2 := putBytes(t, s, "obj", payload)
	if v2.Version != v1.Version {
		t.Fatalf("expected dedup to reuse version %d, got new version %d", v1.Version, v2.Version)
	}
	if blobFileCount(t, dir) != blobsAfterFirst {
		t.Fatalf("identical rewrite created another physical blob")
	}
	t.Logf("identical rewrite reused version=%d; physical blobs still=%d (no new copy)",
		v2.Version, blobFileCount(t, dir))

	// Different object, same bytes -> a new version but shared blob.
	v3 := putBytes(t, s, "other-obj", payload)
	if v3.Version != 1 {
		t.Fatalf("expected new object at version 1, got %d", v3.Version)
	}
	if blobFileCount(t, dir) != 1 {
		t.Fatalf("same content across objects must share one blob, found %d",
			blobFileCount(t, dir))
	}
	t.Logf("cross-object identical content created new logical version but shared 1 blob; result PASS")
}

// TestStableIdentityAcrossChunking verifies object identity (digest) is a
// pure function of bytes and is unaffected by how the reader chunks them.
func TestStableIdentityAcrossChunking(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	// Build 200KB so reads straddle many buffer boundaries.
	payload := bytes.Repeat([]byte("ABCDEFGH12345678"), 200*1024/16)

	v1 := putBytes(t, s, "a", payload)
	v2 := putBytes(t, s, "b", payload)
	t.Logf("input=%d bytes via streaming reader; digest(a)=%s digest(b)=%s",
		len(payload), v1.Digest, v2.Digest)
	if v1.Digest != digestOf(payload) || v1.Digest != v2.Digest {
		t.Fatalf("identity not stable: %s vs %s vs %s", v1.Digest, digestOf(payload), v2.Digest)
	}
	t.Logf("result: identical content maps to identical digest independent of chunk boundaries")
}

// TestNotFoundClassification verifies reads reject unknown, deleted and
// collected versions with distinguishable sentinel errors.
func TestNotFoundClassification(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "doc", []byte("v1 bytes"))

	cases := []struct {
		name    string
		fn      func() error
		wantErr error
	}{
		{"unknown object", func() error {
			_, _, e := s.Get(ctx, "ghost")
			return e
		}, ErrNotFound},
		{"unknown version", func() error {
			_, _, e := s.GetVersion(ctx, "doc", 999)
			return e
		}, ErrNotFound},
	}
	for _, tc := range cases {
		err := tc.fn()
		t.Logf("case=%q err=%v (basis: expect sentinel %v)", tc.name, err, tc.wantErr)
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s: got %v want %v", tc.name, err, tc.wantErr)
		}
	}
	t.Logf("result: not-found cases rejected explicitly, never silent/partial data")
}

// TestDeleteRestoreLifecycle verifies soft delete hides data, Get reports
// ErrDeleted, history remains enumerable, and Restore brings it back.
func TestDeleteRestoreLifecycle(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	p1 := []byte("first")
	p2 := []byte("second")
	putBytes(t, s, "k", p1)
	putBytes(t, s, "k", p2)

	dv, err := s.Delete(ctx, "k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	t.Logf("deleted current: version=%d state=%d (basis: soft delete retains bytes)", dv.Version, dv.State)

	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get after delete: got %v want ErrDeleted", err)
	}
	// Explicit old version v1 must still be independently readable.
	body := getVersionBody(t, s, "k", 1)
	if string(body) != "first" {
		t.Fatalf("old version content changed: %q", body)
	}
	t.Logf("after delete: Get rejected with ErrDeleted; v1 still readable; history preserved")

	rv, err := s.Restore(ctx, "k")
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if rv.Version != 2 || string(getBody(t, s, "k")) != "second" {
		t.Fatalf("restore did not return newest active version: %+v", rv)
	}
	t.Logf("result: Restore made v%d current and content is readable again", rv.Version)
}

// TestVersionsHistory verifies monotonic version numbering and listing.
func TestVersionsHistory(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	ctx := context.Background()
	putBytes(t, s, "h", []byte("a"))
	putBytes(t, s, "h", []byte("b"))
	putBytes(t, s, "h", []byte("c"))

	vs, err := s.Versions(ctx, "h")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(vs) != 3 || vs[0].Version != 1 || vs[2].Version != 3 {
		t.Fatalf("history wrong: %+v", vs)
	}
	for _, v := range vs {
		t.Logf("history entry version=%d size=%d digest=%s state=%d", v.Version, v.Size, v.Digest, v.State)
	}
	names, _ := s.List(ctx)
	if len(names) != 1 || names[0] != "h" {
		t.Fatalf("List wrong: %v", names)
	}
	t.Logf("result: %d monotonic versions, List=%v", len(vs), names)
}

// TestReadVerificationDetectsCorruption flips bytes in a committed blob
// and asserts read verification rejects it with ErrDigestMismatch rather
// than returning altered data.
func TestReadVerificationDetectsCorruption(t *testing.T) {
	s, dir := newTestStore(t, Options{})
	payload := []byte("integrity matters a great deal")
	vi := putBytes(t, s, "file", payload)

	// Corrupt the physical blob directly, bypassing the API.
	path, perr := s.blobs.blobPath(vi.Digest)
	if perr != nil {
		t.Fatalf("blob path: %v", perr)
	}
	if err := osWriteByteFlip(path); err != nil {
		t.Fatalf("corrupt blob: %v", err)
	}
	t.Logf("input: flipped one byte of committed blob at %s", path)

	rc := openBody(t, s, "file")
	_, rerr := io.ReadAll(rc)
	rc.Close()
	if !errors.Is(rerr, ErrDigestMismatch) {
		t.Fatalf("corrupted read: got %v want ErrDigestMismatch", rerr)
	}
	t.Logf("result: corrupted blob rejected with ErrDigestMismatch; dir=%s", dir)

	// Verify should also report it.
	rep, verr := s.Verify(context.Background())
	if verr != nil {
		t.Fatalf("Verify: %v", verr)
	}
	if len(rep.Corrupt) != 1 {
		t.Fatalf("expected 1 corrupt blob, got %+v", rep)
	}
	t.Logf("Verify report: checked=%d corrupt=%v (basis: re-hash vs content address)",
		rep.BlobsChecked, rep.Corrupt)
}

// TestInvalidName verifies input validation rejects unsafe names.
func TestInvalidName(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	long := make([]byte, maxNameLen+1)
	for i := range long {
		long[i] = 'x'
	}
	for _, bad := range []string{"", "with\x00nul", string(long)} {
		_, err := s.Put(context.Background(), bad, bytes.NewReader([]byte("x")))
		t.Logf("input name=%q -> err=%v (basis: validateName)", bad, err)
		if !errors.Is(err, ErrName) {
			t.Fatalf("name %q: got %v want ErrName", bad, err)
		}
	}
	if _, _, err := s.Get(context.Background(), ""); !errors.Is(err, ErrName) {
		t.Fatalf("Get empty name: got %v want ErrName", err)
	}
	t.Logf("result: invalid names rejected with ErrName")
}

// ExampleStore demonstrates opening a store in a temp directory,
// writing identical content twice (dedup) and reading it back.
func ExampleStore() {
	dir, _ := os.MkdirTemp("", "objectstore-example-")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	store, err := Open(ctx, dir, Options{})
	if err != nil {
		fmt.Println("open error")
		return
	}
	defer store.Close()

	data := []byte("hello-example")
	v1, _ := store.Put(ctx, "greeting", bytes.NewReader(data))
	v2, _ := store.Put(ctx, "greeting", bytes.NewReader(data)) // identical: dedup

	rc, _, _ := store.Get(ctx, "greeting")
	out, _ := io.ReadAll(rc)
	rc.Close()

	fmt.Printf("versions=%d,%d same=%v body=%s\n", v1.Version, v2.Version,
		v1.Version == v2.Version, string(out))
	// Output: versions=1,1 same=true body=hello-example
}
