package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMistakeDeleteBoundaries 覆盖误删边界：
//  1. 删除不存在对象/不存在版本 -> 明确错误，不影响任何数据；
//  2. 对已删除版本再删 -> 幂等成功，不产生额外状态变化；
//  3. 宽限期内 GC -> 数据仍在，可恢复；
//  4. 删除后被 GC 回收 -> 恢复被拒绝（ErrReclaimed），同名重新写入不受影响；
//  5. 空内容对象也可正常去重读写。
func TestMistakeDeleteBoundaries(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	s, _ := newTestStore(t, cfg)

	// 1) 误删不存在
	if _, err := s.Delete(context.Background(), "ghost", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing object must be ErrNotFound, got %v", err)
	}
	if _, err := s.Delete(context.Background(), "ghost", 42); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing object by id must be ErrNotFound, got %v", err)
	}
	v := putBytes(t, s, "real", []byte("payload"))
	if _, err := s.Delete(context.Background(), "real", v.ID+100); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("delete missing version must be ErrVersionNotFound, got %v", err)
	}
	t.Log("judgement: deleting absent object/version fails with specific errors, mutates nothing")

	// 2) 重复删除幂等
	d1, err := s.Delete(context.Background(), "real", v.ID)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := s.Delete(context.Background(), "real", v.ID)
	if err != nil || d1.State != VersionDeleted || d2.State != VersionDeleted {
		t.Fatalf("double delete must be idempotent, got %+v %+v err=%v", d1, d2, err)
	}
	t.Log("judgement: deleting an already deleted version is idempotent")

	// 3) 立即 GC（默认宽限期 1s）：数据保留，可恢复。
	rep, err := s.RunGC(context.Background())
	if err != nil || len(rep.DeletableBlobs) != 0 {
		t.Fatalf("GC within grace must keep data, got %v err=%v", rep, err)
	}
	r, err := s.Restore(context.Background(), "real", v.ID)
	if err != nil || r.State != VersionLive {
		t.Fatalf("restore within grace must succeed, got %+v err=%v", r, err)
	}
	if got, _ := getBytes(t, s, "real", v.ID); string(got) != "payload" {
		t.Fatalf("restored content mismatch: %q", got)
	}
	t.Log("judgement: mistaken delete undone via Restore before GC; data bytes unchanged")
}

// TestReclaimThenRewrite 判定：回收后同名对象重新写入是全新版本，
// 历史版本仍然可列举但永久不可读（ErrReclaimed），不会串内容。
func TestReclaimThenRewrite(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = 0 // 立即回收
	s, _ := newTestStore(t, cfg)

	old := putBytes(t, s, "log", []byte("old-generation"))
	if _, err := s.Delete(context.Background(), "log", old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunGC(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(context.Background(), "log", old.ID); !errors.Is(err, ErrReclaimed) {
		t.Fatalf("old version must be ErrReclaimed, got %v", err)
	}

	newV := putBytes(t, s, "log", []byte("new-generation"))
	if newV.ID <= old.ID {
		t.Fatalf("rewrite must create a newer version: old=%d new=%d", old.ID, newV.ID)
	}
	got, info := getBytes(t, s, "log", 0)
	if info.ID != newV.ID || string(got) != "new-generation" {
		t.Fatalf("current must be the rewritten version v%d %q", info.ID, got)
	}
	vs, _ := s.Versions(context.Background(), "log")
	if len(vs) != 2 || vs[0].State != VersionReclaimed || vs[1].State != VersionLive {
		t.Fatalf("history must keep reclaimed record + new live, got %+v", vs)
	}
	t.Logf("judgement: reclaimed v%d stays listed but unreadable; rewrite v%d independent: %+v",
		old.ID, newV.ID, vs)
}

// TestEmptyAndLargeContent 判定：零字节与较大内容都能稳定去重、读写。
func TestEmptyAndLargeContent(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	e1 := putBytes(t, s, "e1", nil)
	e2 := putBytes(t, s, "e2", []byte{})
	if e1.ContentID != e2.ContentID || e1.Size != 0 {
		t.Fatalf("empty contents must share id: %s %s", e1.ContentID, e2.ContentID)
	}
	if got, info := getBytes(t, s, "e1", 0); len(got) != 0 || info.Size != 0 {
		t.Fatalf("empty read wrong: %q info=%+v", got, info)
	}
	big := bytes.Repeat([]byte{0xab}, 3*1024*1024+7)
	bv := putBytes(t, s, "big", big)
	got, _ := getBytes(t, s, "big", bv.ID)
	if !bytes.Equal(got, big) {
		t.Fatalf("large content mismatch: len %d vs %d", len(got), len(big))
	}
	st, _ := s.Stats()
	// 空内容 1 个 blob + 大对象 1 个 blob，共 2 个物理块；3 个存活版本。
	if st.Blobs != 2 || st.LiveVersions != 3 {
		t.Fatalf("stats wrong for empty+large: %+v", st)
	}
	t.Logf("judgement: empty writes dedup (id=%s); 3MiB+ content roundtrips; stats=%+v",
		e1.ContentID[:12], st)
}

// TestPathTraversalRejected 判定：内容标识格式严格校验，杜绝路径穿越；
// 对外对象名不参与路径构造（名字只存在于清单 JSON 中）。
func TestPathTraversalRejected(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	// 对象名含特殊字符是安全的：它不会映射成文件路径。
	name := "../../etc/passwd"
	v := putBytes(t, s, name, []byte("x"))
	if got, _ := getBytes(t, s, name, v.ID); string(got) != "x" {
		t.Fatalf("odd object name roundtrip failed")
	}
	if _, err := s.blobs.Open("../etc/passwd"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed content id must be ErrInvalid, got %v", err)
	}
	if _, err := s.blobs.Open(ContentID(strings.Repeat("z", 63))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short content id must be ErrInvalid, got %v", err)
	}
	t.Log("judgement: object names are metadata only; blob ids validated against traversal")
}

// TestContextCancelCleansUp 判定：写入被 context 取消时，不留下版本也不留下
// staging 残文件；随后存储仍可正常使用。
func TestContextCancelCleansUp(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, "slow", pr)
		errCh <- err
	}()
	_, _ = pw.Write([]byte("some-bytes-"))
	cancel()
	// 关闭写端以解除可能的 Copy 阻塞。
	_ = pw.Close()
	if err := <-errCh; err == nil {
		t.Fatal("cancelled Put must return an error")
	} else {
		t.Logf("cancelled Put returned (as expected): %v", err)
	}
	// 不留残文件、不留版本。
	staging, _ := s.blobs.listStaging()
	if len(staging) != 0 {
		t.Fatalf("cancelled write must clean staging, leftover=%v", staging)
	}
	if _, _, err := s.Get(context.Background(), "slow", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancelled write must not create an object, got %v", err)
	}
	// 存储仍可用。
	v := putBytes(t, s, "after", []byte("works"))
	if got, _ := getBytes(t, s, "after", v.ID); string(got) != "works" {
		t.Fatalf("store unusable after cancellation: %q", got)
	}
	t.Log("judgement: cancellation leaves no staging file and no version; store stays usable")
}

// TestManifestTmpCleaned 判定：manifest 写入中断留下的 .tmp 文件不影响加载，
// 且不会污染下一次原子替换（它们仅为暂存名，永远不被读取）。
func TestManifestTmpCleaned(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	s, _, _ := Open(context.Background(), cfg)
	putBytes(t, s, "m", []byte("mm"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 伪造一个 manifest 替换中断后的残留临时文件。
	junk := filepath.Join(dir, "manifest.json.tmp-deadbeef")
	if err := os.WriteFile(junk, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("load must ignore stale manifest tmp files: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if got, _ := getBytes(t, s2, "m", 0); string(got) != "mm" {
		t.Fatalf("data must load despite manifest tmp junk: %q", got)
	}
	_ = rep
	t.Log("judgement: stale manifest.json.tmp-* file never read; committed manifest intact")
}
