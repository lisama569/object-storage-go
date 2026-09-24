package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T, cfg Config) (*Store, *RecoveryReport) {
	t.Helper()
	if cfg.Dir == "" {
		cfg = DefaultConfig(t.TempDir())
	}
	ctx := context.Background()
	s, rep, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, rep
}

func putBytes(t *testing.T, s *Store, name string, data []byte) *VersionInfo {
	t.Helper()
	info, err := s.Put(context.Background(), name, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put(%q,%d bytes) failed: %v", name, len(data), err)
	}
	return info
}

func getBytes(t *testing.T, s *Store, name string, vid int64) ([]byte, *VersionInfo) {
	t.Helper()
	rc, info, err := s.Get(context.Background(), name, vid)
	if err != nil {
		t.Fatalf("Get(%q,#%d) failed: %v", name, vid, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q,#%d) failed: %v", name, vid, err)
	}
	return data, info
}

// TestPutGetDedup 输入：3 次相同内容 + 1 次不同内容。
// 判定：内容标识稳定、重复写入不产生副本、版本各自独立、物理字节去重。
func TestPutGetDedup(t *testing.T) {
	s, rep := newTestStore(t, DefaultConfig(t.TempDir()))
	t.Logf("recovery on empty dir: stagingRemoved=%d orphans=%d dangling=%d",
		len(rep.StagingFilesRemoved), len(rep.OrphanBlobsFound), len(rep.DanglingRefsFound))

	contentA := []byte("hello-content-addressed-world")
	contentB := []byte("different-content")

	wantID, n, err := HashContent(bytes.NewReader(contentA))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input A: %q (%d bytes), expected content id=%s", contentA, n, wantID)

	v1 := putBytes(t, s, "obj", contentA)
	v2 := putBytes(t, s, "obj", contentA) // 相同内容
	v3 := putBytes(t, s, "other", contentA)
	v4 := putBytes(t, s, "obj", contentB)

	if v1.ContentID != wantID || v2.ContentID != wantID || v3.ContentID != wantID {
		t.Fatalf("content id must be stable: v1=%s v2=%s v3=%s want=%s",
			v1.ContentID, v2.ContentID, v3.ContentID, wantID)
	}
	if v1.ID == v2.ID || v2.ID == v4.ID {
		t.Fatalf("each Put must create a new version: %d %d %d", v1.ID, v2.ID, v4.ID)
	}
	if v2.ContentID == v4.ContentID {
		t.Fatalf("different content must have different ids")
	}

	got, info := getBytes(t, s, "obj", 0)
	if !bytes.Equal(got, contentB) {
		t.Fatalf("latest live version mismatch: got %q want %q", got, contentB)
	}
	t.Logf("judgement: latest live of obj is v%d id=%s (contentB), as written last", info.ID, info.ContentID)

	got1, _ := getBytes(t, s, "obj", v1.ID)
	if !bytes.Equal(got1, contentA) {
		t.Fatalf("old version v1 content changed")
	}

	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stats: objects=%d liveVersions=%d blobs=%d blobBytes=%d logicalBytes=%d",
		st.Objects, st.LiveVersions, st.Blobs, st.BlobBytes, st.LogicalBytes)
	if st.Blobs != 2 {
		t.Fatalf("dedup: want 2 physical blobs, got %d", st.Blobs)
	}
	if st.BlobBytes != int64(len(contentA)+len(contentB)) {
		t.Fatalf("blobBytes want %d got %d", len(contentA)+len(contentB), st.BlobBytes)
	}
	if st.LiveVersions != 4 || st.LogicalBytes != int64(3*len(contentA)+len(contentB)) {
		t.Fatalf("logical accounting wrong: %+v", st)
	}
}

// TestChunkStableID 判定：同一内容用不同分块边界拷贝，标识一致。
func TestChunkStableID(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestStore(t, DefaultConfig(dir))
	payload := bytes.Repeat([]byte("abcdefghij"), 1000) // 10000 字节
	info := putBytes(t, s, "x", payload)

	// 用多个不同尺寸的拷贝块流式重算，作为分块无关性的判定依据。
	for _, chunk := range []int{1, 7, 4096, 65537} {
		r := &chunkedReader{data: payload, chunk: chunk}
		id, n, err := HashContent(r)
		if err != nil {
			t.Fatal(err)
		}
		if id != info.ContentID || n != int64(len(payload)) {
			t.Fatalf("chunk=%d: id=%s n=%d, want id=%s n=%d", chunk, id, n, info.ContentID, len(payload))
		}
	}
	t.Logf("judgement: content id %s stable across chunk sizes {1,7,4096,65537}", info.ContentID)
}

type chunkedReader struct {
	data  []byte
	chunk int
	pos   int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.chunk
	if n > len(p) {
		n = len(p)
	}
	if c.pos+n > len(c.data) {
		n = len(c.data) - c.pos
	}
	n = copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

// TestDeleteRestoreReadability 判定：删除不可读但可恢复；恢复后可读；
// 不存在/已删除/已回收三类错误可区分。
func TestDeleteRestoreReadability(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestStore(t, DefaultConfig(dir))
	v1 := putBytes(t, s, "k", []byte("v1"))
	v2 := putBytes(t, s, "k", []byte("v2-longer"))

	// 不存在
	if _, _, err := s.Get(context.Background(), "nope", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, _, err := s.Get(context.Background(), "k", 9999); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("want ErrVersionNotFound, got %v", err)
	}
	t.Log("judgement: missing object -> ErrNotFound; missing version -> ErrVersionNotFound")

	// 删除最新存活版本 v2，当前读取应被拒绝（ErrDeleted），但 v1 仍可读。
	d, err := s.Delete(context.Background(), "k", 0)
	if err != nil || d.ID != v2.ID {
		t.Fatalf("delete latest want v%d, got %+v err=%v", v2.ID, d, err)
	}
	if _, _, err := s.Get(context.Background(), "k", 0); !errors.Is(err, ErrDeleted) {
		t.Fatalf("deleted current must reject with ErrDeleted, got %v", err)
	}
	if _, _, err := s.Get(context.Background(), "k", v2.ID); !errors.Is(err, ErrDeleted) {
		t.Fatalf("deleted version must reject with ErrDeleted, got %v", err)
	}
	if got, _ := getBytes(t, s, "k", v1.ID); string(got) != "v1" {
		t.Fatalf("older live version must stay readable")
	}
	t.Log("judgement: after deleting v2, current -> ErrDeleted while v1 stays readable")

	// 恢复 v2：重新成为当前存活版本。
	r, err := s.Restore(context.Background(), "k", v2.ID)
	if err != nil || r.State != VersionLive {
		t.Fatalf("restore failed: %+v %v", r, err)
	}
	if got, info := getBytes(t, s, "k", 0); string(got) != "v2-longer" || info.ID != v2.ID {
		t.Fatalf("restored v2 must become current, got v%d %q", info.ID, got)
	}
	t.Log("judgement: restored v2 becomes the single definite current version again")

	// 持久化后重开，状态保持。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, rep := newTestStoreAt(t, dir)
	t.Logf("reopen recovery: staging=%d dangling=%d", len(rep.StagingFilesRemoved), len(rep.DanglingRefsFound))
	if got, _ := getBytes(t, s2, "k", 0); string(got) != "v2-longer" {
		t.Fatalf("state must survive restart, got %q", got)
	}
}

func newTestStoreAt(t *testing.T, dir string) (*Store, *RecoveryReport) {
	t.Helper()
	cfg := DefaultConfig(dir)
	s, rep, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, rep
}

// TestReopenPersistence 输入：写两个版本后正常关闭。
// 判定：重开后版本列表、内容、统计完全一致。
func TestReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestStore(t, DefaultConfig(dir))
	putBytes(t, s, "a", []byte("aa"))
	putBytes(t, s, "a", []byte("aaa"))
	putBytes(t, s, "b", []byte("aa")) // 与 a 的 v1 去重
	st1, _ := s.Stats()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, _ := newTestStoreAt(t, dir)
	vs, err := s2.Versions(context.Background(), "a")
	if err != nil || len(vs) != 2 {
		t.Fatalf("versions after reopen: %+v err=%v", vs, err)
	}
	st2, _ := s2.Stats()
	if st1 != st2 {
		t.Fatalf("stats changed across reopen: before=%+v after=%+v", st1, st2)
	}
	t.Logf("judgement: versions=%d stats stable after reopen: %+v", len(vs), st2)
}

// TestDirLock 判定：同一目录不能被两个句柄/进程同时打开。
func TestDirLock(t *testing.T) {
	dir := t.TempDir()
	s1, _, err := Open(context.Background(), DefaultConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	_, _, err = Open(context.Background(), DefaultConfig(dir))
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("second Open must fail with ErrBusy, got %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s3, _, err := Open(context.Background(), DefaultConfig(dir))
	if err != nil {
		t.Fatalf("Open after close must succeed, got %v", err)
	}
	t.Log("judgement: concurrent open -> ErrBusy; open after close succeeds")
	_ = s3.Close()
}

// TestGCReclaimAndErrors 判定：GC 回收过期删除版本与孤儿，区分 ErrReclaimed，
// 重复执行结论稳定。
func TestGCReclaimAndErrors(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = time.Millisecond // 宽限期极短，便于稳定复现
	s, _, _ := Open(context.Background(), cfg)
	t.Cleanup(func() { _ = s.Close() })

	v1 := putBytes(t, s, "g", []byte("gone"))
	vKeep := putBytes(t, s, "keep", []byte("alive-content"))
	if _, err := s.Delete(context.Background(), "g", v1.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	rep, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC#1 input: grace=%s; deleted=%d blobs deleted=%d bytes=%d",
		rep.GracePeriod, rep.DeletedVersions, len(rep.DeletableBlobs), rep.DeletableBytes)
	if len(rep.DeletableBlobs) != 1 {
		t.Fatalf("want exactly 1 blob reclaimed, got %d (%v)", len(rep.DeletableBlobs), rep.DeletableBlobs)
	}

	if _, _, err := s.Get(context.Background(), "g", v1.ID); !errors.Is(err, ErrReclaimed) {
		t.Fatalf("reclaimed version must return ErrReclaimed, got %v", err)
	}
	if _, err := s.Restore(context.Background(), "g", v1.ID); !errors.Is(err, ErrReclaimed) {
		t.Fatalf("restore of reclaimed version must fail with ErrReclaimed, got %v", err)
	}
	if got, _ := getBytes(t, s, "keep", vKeep.ID); string(got) != "alive-content" {
		t.Fatalf("referenced data must survive GC")
	}
	t.Log("judgement: g#1 -> ErrReclaimed (distinct from ErrDeleted); keep data intact")

	// 重复 GC：不应再删除任何东西。
	rep2, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.DeletableBlobs) != 0 {
		t.Fatalf("second GC must delete nothing, got %v", rep2.DeletableBlobs)
	}
	t.Logf("GC#2 result stable: deleted=%d reclaimedVersions=%d",
		len(rep2.DeletableBlobs), len(rep2.VersionsReclaimed))
}

func ExampleStore() {
	dir, _ := os.MkdirTemp("", "objstore-example-*")
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = 0
	s, rep, err := Open(context.Background(), cfg)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer s.Close()
	info, _ := s.Put(context.Background(), "greeting", strings.NewReader("hello"))
	rc, _, _ := s.Get(context.Background(), "greeting", 0)
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	fmt.Println(string(b), len(rep.StagingFilesRemoved), info.ContentID != "")
	// Output: hello 0 true
}

var _ = filepath.Join
