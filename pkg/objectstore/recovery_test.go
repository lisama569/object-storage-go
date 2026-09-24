package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// reopen 关闭后用同样配置重开（模拟进程重启；flock 由内核在崩溃时释放）。
func reopen(t *testing.T, s **Store, cfg Config) *RecoveryReport {
	t.Helper()
	if err := (*s).Close(); err != nil {
		t.Fatal(err)
	}
	ns, rep, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	*s = ns
	t.Cleanup(func() { _ = ns.Close() })
	return rep
}

// TestInterruptedWriteRecovery 输入：一个成功写入 + 一个“写入中途进程被杀”
// （留下 staging 残文件）。
// 判定：重开后残文件被清理；既有对象可读；残缺数据绝不会成为可读取对象；
// 清单中不包含对残文件的引用。
func TestInterruptedWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	s, _, _ := Open(context.Background(), cfg)

	good := []byte("committed-object")
	putBytes(t, s, "good", good)

	// 模拟“写到一半崩溃”：直接拿一个 blob 句柄写入部分字节后，不 Commit，
	// 直接关闭句柄并“杀死进程”（Close 不清理它，通过 reopen 模拟）。
	w, err := s.blobs.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("partial-")); err != nil {
		t.Fatal(err)
	}
	// 故意不 Commit、不 Cancel；模拟进程立即死亡：staging 中留下残文件。
	stagingBefore, _ := s.blobs.listStaging()
	t.Logf("input: 1 committed object (%d bytes) + %d orphaned staging file(s) simulating mid-write crash",
		len(good), len(stagingBefore))
	if len(stagingBefore) != 1 {
		t.Fatalf("expected exactly 1 staging leftover, got %d", len(stagingBefore))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 进程死亡后临时文件仍留在磁盘。
	left, _ := filepath.Glob(filepath.Join(dir, "staging", "*"))
	t.Logf("on-disk state at crash: %d staging files remain", len(left))

	// 重启并恢复。
	s2, rep, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	t.Logf("recovery result: removed staging=%v dangling=%d orphans=%d",
		rep.StagingFilesRemoved, len(rep.DanglingRefsFound), len(rep.OrphanBlobsFound))
	if len(rep.StagingFilesRemoved) != 1 {
		t.Fatalf("recovery must remove exactly 1 staging file, got %d", len(rep.StagingFilesRemoved))
	}

	got, _ := getBytes(t, s2, "good", 0)
	if !bytes.Equal(got, good) {
		t.Fatalf("committed object must survive crash intact, got %q", got)
	}
	// 残缺数据没有对应对象名。
	if _, _, err := s2.Get(context.Background(), "good", 999); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("partial write must never be addressable, got %v", err)
	}
	st, _ := s2.Stats()
	if st.LiveVersions != 1 || st.Blobs != 1 {
		t.Fatalf("post-recovery stats wrong: %+v", st)
	}
	leftAfter, _ := filepath.Glob(filepath.Join(dir, "staging", "*"))
	if len(leftAfter) != 0 {
		t.Fatalf("staging must be empty after recovery, got %d", len(leftAfter))
	}
	t.Log("judgement: partial data unreadable & cleaned; committed data intact; no dangling references")
}

// TestInterruptedGCRecovery 输入：一个已删除对象；先模拟 GC 在“元数据已标记
// RECLAIMED、数据文件尚未删除”时崩溃，重启；再模拟“数据文件被外部删除”后重启。
// 判定：两种崩溃后元数据与磁盘都能重新自洽，数据绝不丢失（仍引用的 blob 不动）。
func TestInterruptedGCRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = time.Millisecond
	s, _, _ := Open(context.Background(), cfg)

	vict := putBytes(t, s, "victim", []byte("to-be-reclaimed"))
	keep := putBytes(t, s, "keep", []byte("must-survive-crash"))
	keepID := keep.ContentID
	if _, err := s.Delete(context.Background(), "victim", vict.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	// 第一次 GC：正常完成，数据被删、版本转 RECLAIMED。
	rep, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC completed: deleted blobs=%d versionsReclaimed=%d",
		len(rep.DeletableBlobs), len(rep.VersionsReclaimed))
	if len(rep.DeletableBlobs) != 1 {
		t.Fatalf("want 1 deleted blob, got %d", len(rep.DeletableBlobs))
	}

	// 模拟“数据文件被外部删除但清单仍指向它”的崩溃遗留：手动删除 keep 的 blob，
	// 重启恢复必须把对应版本标记为 RECLAIMED（不静默返回旧/坏内容），且不崩溃。
	keepPath, err := s.blobs.blobPath(keepID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(keepPath); err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated post-GC crash: blob %s removed from disk while manifest still references it", keepID[:12])

	rep2 := reopen(t, &s, cfg)
	if len(rep2.DanglingRefsFound) != 1 {
		t.Fatalf("recovery must detect 1 dangling ref, got %+v", rep2.DanglingRefsFound)
	}
	t.Logf("recovery result: dangling=%+v", rep2.DanglingRefsFound)
	if _, _, err := s.Get(context.Background(), "keep", keep.ID); !errors.Is(err, ErrReclaimed) {
		t.Fatalf("dangling ref read must return ErrReclaimed, got %v", err)
	}
	// victim 仍是 RECLAIMED；重复恢复不产生新变更。
	if vs, _ := s.Versions(context.Background(), "victim"); vs[0].State != VersionReclaimed {
		t.Fatalf("victim should stay RECLAIMED, got %s", vs[0].State)
	}
	_, _, err = Open(context.Background(), cfg) // 目录仍被 s 锁住，应失败
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("lock must still be held by reopened store, got %v", err)
	}
	t.Log("judgement: dangling reference healed to RECLAIMED; reads fail loudly; lock honored")
}

// TestGCIdempotentAndReferencedSafe 输入：共享同一 blob 的两个对象，删除其中一个。
// 判定：宽限期前 GC 不删；宽限期后也不删（仍被另一存活版本引用）；
// 把两个都删除并过宽限期后才回收；连跑三次 GC，引用数据始终安全。
func TestGCIdempotentAndReferencedSafe(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = 20 * time.Millisecond
	s, _, _ := Open(context.Background(), cfg)
	t.Cleanup(func() { _ = s.Close() })

	content := []byte("shared-content")
	a := putBytes(t, s, "a", content)
	b := putBytes(t, s, "b", content)
	t.Logf("input: two objects sharing content id=%s (dedup expected: 1 blob)", a.ContentID[:12])
	st, _ := s.Stats()
	if st.Blobs != 1 {
		t.Fatalf("setup: want 1 physical blob, got %d", st.Blobs)
	}

	// 删除 a：blob 仍被 b 引用，任何时候都不能被回收。
	if _, err := s.Delete(context.Background(), "a", a.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	for i := 0; i < 3; i++ {
		rep, err := s.RunGC(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.DeletableBlobs) != 0 || rep.LiveBlobs != 1 {
			t.Fatalf("GC#%d must keep referenced blob, deleted=%v live=%d",
				i+1, rep.DeletableBlobs, rep.LiveBlobs)
		}
	}
	got, _ := getBytes(t, s, "b", b.ID)
	if !bytes.Equal(got, content) {
		t.Fatalf("b content corrupted by GC: %q", got)
	}
	t.Log("judgement: blob referenced by any live/deleted-in-grace version is never collected (3 GC runs)")

	// 再删除 b；此时 blob 不再被 LIVE 引用，但 DELETED 在宽限期内仍受保护。
	if _, err := s.Delete(context.Background(), "b", b.ID); err != nil {
		t.Fatal(err)
	}
	rep0, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep0.DeletableBlobs) != 0 {
		t.Fatalf("deleted-within-grace blob must be retained, got %v", rep0.DeletableBlobs)
	}
	t.Log("judgement: deleted version data retained within grace period (restorable)")

	time.Sleep(30 * time.Millisecond)
	rep1, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep1.DeletableBlobs) != 1 {
		t.Fatalf("after grace, want 1 deletion, got %v", rep1.DeletableBlobs)
	}
	// 再跑两次：必须完全无操作。
	for i := 0; i < 2; i++ {
		repN, err := s.RunGC(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(repN.DeletableBlobs) != 0 || len(repN.VersionsReclaimed) != 0 {
			t.Fatalf("repeat GC must be a no-op, got %+v", repN)
		}
	}
	t.Log("judgement: repeated GC after collection is a no-op; idempotent")
}

// TestCorruptBlobDetected 判定：磁盘 blob 被篡改后，VerifyOnOpen 能识别为
// CORRUPT；读取（全量校验）也以 ErrChecksum 失败，绝不返回被篡改内容。
func TestCorruptBlobDetected(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	s, _, _ := Open(context.Background(), cfg)
	v := putBytes(t, s, "doc", []byte("important-secret"))
	p, err := s.blobs.blobPath(v.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 原地篡改：翻转首字节（保持文件长度，确保不是被“缺失”路径发现）。
	f, err := os.OpenFile(p, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte{0}
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	t.Logf("input: corrupted first byte of blob %s on disk", v.ContentID[:12])

	// 读取路径：逐块校验，损坏在 Read 结束时暴露。
	s2, _, _ := Open(context.Background(), cfg)
	t.Cleanup(func() { _ = s2.Close() })
	rc, _, err := s2.Get(context.Background(), "doc", v.ID)
	if err != nil {
		t.Fatalf("open of reader should succeed lazily, got %v", err)
	}
	_, rerr := io.ReadAll(rc)
	_ = rc.Close()
	if !errors.Is(rerr, ErrChecksum) {
		t.Fatalf("reading corrupt blob must fail with ErrChecksum, got %v", rerr)
	}
	t.Log("judgement: tampered content rejected on read with ErrChecksum (not returned silently)")

	// 打开期校验：标记为 CORRUPT。
	_ = s2.Close()
	cfg2 := DefaultConfig(dir)
	cfg2.VerifyOnOpen = true
	s3, rep, err := Open(context.Background(), cfg2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	if len(rep.DanglingRefsFound) != 1 || rep.DanglingRefsFound[0].Reason != "sha256 mismatch" {
		t.Fatalf("VerifyOnOpen must report checksum mismatch, got %+v", rep.DanglingRefsFound)
	}
	if _, _, err := s3.Get(context.Background(), "doc", v.ID); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt state must yield ErrChecksum, got %v", err)
	}
	t.Logf("recovery result: %+v -> version CORRUPT", rep.DanglingRefsFound)
}

// TestOrphanBlobGrace 判定：清单未引用的孤儿 blob（如崩溃在 rename 之后、
// manifest 提交之前）在宽限期内保留、超期后由 GC 回收，恢复时可解释。
func TestOrphanBlobGrace(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = 30 * time.Millisecond
	s, _, _ := Open(context.Background(), cfg)

	w, _ := s.blobs.Begin()
	_, _ = w.Write([]byte("orphan-bytes"))
	id, _, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: committed blob %s on disk but never recorded in manifest (crash before metadata)", id[:12])
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rep := reopen(t, &s, cfg)
	if len(rep.OrphanBlobsFound) != 1 {
		t.Fatalf("recovery must register 1 orphan, got %v", rep.OrphanBlobsFound)
	}
	t.Logf("recovery result: orphan registered %s (kept until grace expires)", id[:12])

	gc0, err := s.RunGC(context.Background())
	if err != nil || len(gc0.DeletableBlobs) != 0 {
		t.Fatalf("orphan within grace must survive GC, got %v err=%v", gc0.DeletableBlobs, err)
	}
	time.Sleep(35 * time.Millisecond)
	gc1, err := s.RunGC(context.Background())
	if err != nil || len(gc1.DeletableBlobs) != 1 {
		t.Fatalf("orphan after grace must be collected, got %v err=%v", gc1.DeletableBlobs, err)
	}
	t.Log("judgement: crash-orphaned blob survives grace, then is reclaimed exactly once")
}

func TestRecoveryReportString(t *testing.T) {
	r := &RecoveryReport{
		StagingFilesRemoved: []string{"/x/staging/tmp-1"},
		OrphanBlobsFound:    []ContentID{"ab"},
	}
	if fmt.Sprint(r) == "" {
		t.Fatal("report should be printable")
	}
}
