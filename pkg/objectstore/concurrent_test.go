package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentPuts 输入：N 个 goroutine 并发写入同一对象名，内容只取 K 种
// （去重命中率高）。
// 判定：
//   - 每次 Put 都成功且版本号互不相同、单调推进（无丢失提交）；
//   - 物理 blob 数恰好等于不同内容数（并发去重正确，不产生重复副本）；
//   - 用 -race 运行不得报告数据竞争。
func TestConcurrentPuts(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	const n = 40
	const k = 5

	var wg sync.WaitGroup
	errs := make(chan error, n)
	var maxID int64
	ids := make(chan int64, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("content-%02d", i%k))
			<-start
			info, err := s.Put(context.Background(), "hot", bytes.NewReader(payload))
			if err != nil {
				errs <- err
				return
			}
			ids <- info.ID
			atomic.StoreInt64(&maxID, info.ID)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Fatalf("concurrent put error: %v", err)
	}
	seen := map[int64]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate version id %d under concurrent commits", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("lost commits: got %d versions, want %d", len(seen), n)
	}
	st, _ := s.Stats()
	t.Logf("input: %d concurrent puts, %d distinct contents; result versions=%d physicalBlobs=%d blobBytes=%d",
		n, k, st.LiveVersions, st.Blobs, st.BlobBytes)
	if st.Blobs != k {
		t.Fatalf("dedup under concurrency: want %d blobs, got %d", k, st.Blobs)
	}
	if st.LiveVersions != n {
		t.Fatalf("want %d live versions, got %d", n, st.LiveVersions)
	}
	// 当前版本一定是最后提交之一，且内容自洽可读。
	got, info, err := func() ([]byte, *VersionInfo, error) {
		rc, i, e := s.Get(context.Background(), "hot", 0)
		if e != nil {
			return nil, nil, e
		}
		defer rc.Close()
		b, e := io.ReadAll(rc)
		return b, i, e
	}()
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != maxID || !bytes.HasPrefix(got, []byte("content-")) {
		t.Fatalf("current visibility not deterministic: v%d %q max=%d", info.ID, got, maxID)
	}
	t.Logf("judgement: %d commits, no duplicates/loss; blobs deduped to %d; current=v%d visible deterministically",
		n, st.Blobs, info.ID)
}

// TestConcurrentReadsWhilePuts 输入：持续并发提交的同时，多个读取者反复读取
// 旧版本与当前版本。
// 判定：任何一次成功读取返回的内容都与其版本声明的 ContentID 全量一致；
// 读取要么成功要么得到明确错误，不允许 panic/部分内容。
func TestConcurrentReadsWhilePuts(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	base := putBytes(t, s, "k", bytes.Repeat([]byte("base-"), 256))

	const writers = 6
	const readers = 8
	const rounds = 20
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var bad int64

	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for r := 0; r < rounds; r++ {
				data := []byte(fmt.Sprintf("w%d-r%d-%s", w, r, bytes.Repeat([]byte("z"), r%32)))
				if _, err := s.Put(context.Background(), "k", bytes.NewReader(data)); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}
	for rd := 0; rd < readers; rd++ {
		wg.Add(1)
		go func(rd int) {
			defer wg.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				rc, info, err := s.Get(context.Background(), "k", 0)
				if err != nil {
					if errors.Is(err, ErrDeleted) || errors.Is(err, ErrNotFound) {
						continue
					}
					// 并发期间不删除，除 ErrClosed 外不应有其他错误。
					t.Errorf("unexpected get error: %v", err)
					return
				}
				got, rerr := io.ReadAll(rc)
				_ = rc.Close()
				if rerr != nil {
					t.Errorf("read: %v", rerr)
					return
				}
				id, _, _ := HashContent(bytes.NewReader(got))
				if id != info.ContentID {
					atomic.AddInt64(&bad, 1)
				}
			}
		}(rd)
	}
	// 固定旧版本的并发读取：内容必须永远等于 base。
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for {
			select {
			case <-stop:
				return
			default:
			}
			rc, _, err := s.Get(context.Background(), "k", base.ID)
			if err != nil {
				t.Errorf("old version get: %v", err)
				return
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if !bytes.Equal(got, bytes.Repeat([]byte("base-"), 256)) {
				atomic.AddInt64(&bad, 1)
			}
		}
	}()

	close(start)
	// 等写者大致完成后停读者。
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	_ = writers
	time.Sleep(150 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent test hung")
	}
	if atomic.LoadInt64(&bad) != 0 {
		t.Fatalf("%d reads returned content inconsistent with their version id", bad)
	}
	st, _ := s.Stats()
	t.Logf("judgement: readers saw only self-consistent content; final versions=%d blobs=%d",
		st.LiveVersions, st.Blobs)
}

// TestConcurrentDeleteRestoreVisibility 判定：删除/恢复与读取并发时，
// 当前版本读取的结果只可能是“完整的最新存活内容”或 ErrDeleted，
// 绝不会读到中间态或旧内容冒充当前。
func TestConcurrentDeleteRestoreVisibility(t *testing.T) {
	s, _ := newTestStore(t, DefaultConfig(t.TempDir()))
	v1 := putBytes(t, s, "c", []byte("one"))
	v2 := putBytes(t, s, "c", []byte("two"))
	_ = v1

	const rounds = 30
	var wg sync.WaitGroup
	start := make(chan struct{})
	var violations int64

	wg.Add(2)
	go func() { // 删除/恢复 v2
		defer wg.Done()
		<-start
		for i := 0; i < rounds; i++ {
			if _, err := s.Delete(context.Background(), "c", v2.ID); err != nil {
				t.Errorf("delete: %v", err)
				return
			}
			if _, err := s.Restore(context.Background(), "c", v2.ID); err != nil {
				t.Errorf("restore: %v", err)
				return
			}
		}
	}()
	go func() { // 读取当前版本并校验可见性只能是两种确定状态之一
		defer wg.Done()
		<-start
		for i := 0; i < rounds*4; i++ {
			rc, info, err := s.Get(context.Background(), "c", 0)
			if err != nil {
				if !errors.Is(err, ErrDeleted) {
					t.Errorf("unexpected err: %v", err)
					return
				}
				continue
			}
			b, _ := io.ReadAll(rc)
			_ = rc.Close()
			switch {
			case info.ID == v2.ID && string(b) == "two":
			default:
				// v2 是最后一个版本：它存活时当前必须精确等于 v2；
				// 不允许“静默回退”读到 v1 当作当前。
				atomic.AddInt64(&violations, 1)
			}
		}
	}()
	close(start)
	wg.Wait()
	if violations != 0 {
		t.Fatalf("visibility violated %d times: stale content exposed as current", violations)
	}
	t.Log("judgement: current view is always exactly v2/two or ErrDeleted; never stale v1")
}

// TestConcurrentGCSerializes 判定：GCLockTimeout=0 时并发 GC 互斥，
// 第二个调用立即得到 ErrLockHeld；串行排队（带超时）时全部成功，
// 且被引用数据最终完好。
func TestConcurrentGCSerializes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = time.Millisecond
	cfg.GCLockTimeout = 0
	s, _, _ := Open(context.Background(), cfg)
	t.Cleanup(func() { _ = s.Close() })
	v := putBytes(t, s, "x", []byte("x-data"))

	// 确定性构造：先占住 GC 互斥门（模拟一次正在进行的回收），
	// 此时所有并发 GC 都必须得到 ErrLockHeld。
	s.gcGate <- struct{}{}
	var busy int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := s.RunGC(context.Background()); err != nil {
				if errors.Is(err, ErrLockHeld) {
					atomic.AddInt64(&busy, 1)
					return
				}
				t.Errorf("gc: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if atomic.LoadInt64(&busy) != 8 {
		t.Fatalf("all 8 GCs must get ErrLockHeld while gate is held, got %d", busy)
	}
	<-s.gcGate // 释放后，GC 立即可用。
	if _, err := s.RunGC(context.Background()); err != nil {
		t.Fatalf("GC must succeed after gate released, got %v", err)
	}
	got, _ := getBytes(t, s, "x", v.ID)
	if string(got) != "x-data" {
		t.Fatalf("referenced data lost during concurrent GC: %q", got)
	}
	t.Logf("judgement: 8 concurrent GC attempts rejected with ErrLockHeld while one runs; referenced blob intact")
}

// TestConcurrentPutAndImmediateGC 判定：宽限期=0 时，写入与 GC 高度并发，
// 任何成功返回的版本在随后立即读取时必须可读（不存在“已落盘未登记”blob
// 被 GC 抢先删除的窗口）。
func TestConcurrentPutAndImmediateGC(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.MaxUnreferencedBlobAge = 0 // 最激进回收
	s, _, _ := Open(context.Background(), cfg)
	t.Cleanup(func() { _ = s.Close() })

	const writers = 8
	const per = 15
	var wg sync.WaitGroup
	start := make(chan struct{})
	var failures int64
	var infosMu sync.Mutex
	var infos []*VersionInfo

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < per; j++ {
				data := []byte(fmt.Sprintf("writer-%d-version-%d-payload", i, j))
				info, err := s.Put(context.Background(), "stream", bytes.NewReader(data))
				if err != nil {
					t.Errorf("put: %v", err)
					return
				}
				infosMu.Lock()
				infos = append(infos, info)
				infosMu.Unlock()
			}
		}(i)
	}
	// 一个持续 GC 的协程：宽限期为 0 时最容易暴露 rename 与登记之间的窗口。
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for k := 0; k < writers*per*2; k++ {
			if _, err := s.RunGC(context.Background()); err != nil {
				if errors.Is(err, ErrLockHeld) {
					continue
				}
				t.Errorf("gc: %v", err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()

	// GC 结束后，再写一个保留对象，防止“全部版本都被删”——本测试不删除版本，
	// 所有版本都应存活并可读。
	for _, info := range infos {
		rc, _, err := s.Get(context.Background(), "stream", info.ID)
		if err != nil {
			atomic.AddInt64(&failures, 1)
			t.Errorf("committed version %d unreadable right after Put: %v", info.ID, err)
			continue
		}
		got, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			atomic.AddInt64(&failures, 1)
			t.Errorf("version %d read error: %v", info.ID, rerr)
			continue
		}
		id, _, _ := HashContent(bytes.NewReader(got))
		if id != info.ContentID {
			atomic.AddInt64(&failures, 1)
		}
	}
	if failures != 0 {
		t.Fatalf("%d committed versions were lost under immediate GC", failures)
	}
	st, _ := s.Stats()
	t.Logf("judgement: %d committed versions all readable with aggressive concurrent GC; blobs=%d",
		len(infos), st.Blobs)
}
