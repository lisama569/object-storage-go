package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// VersionInfo 是对外暴露的版本信息。
type VersionInfo struct {
	ID        int64
	Object    string
	ContentID ContentID
	Size      int64
	CreatedAt time.Time
	State     VersionState
}

func toInfo(r VersionRecord) VersionInfo {
	return VersionInfo{
		ID:        r.ID,
		Object:    r.Object,
		ContentID: r.ContentID,
		Size:      r.Size,
		CreatedAt: r.CreatedAt,
		State:     r.State,
	}
}

// Store 是本地内容寻址对象存储。
//
// 进程内用 RWMutex 串行化元数据变更；进程间用 flock 保证同一数据目录
// 同一时刻只有一个存活实例（见 lock.go）。
type Store struct {
	cfg      Config
	mu       sync.RWMutex
	closed   bool
	manifest *manifestStore
	blobs    *blobStore
	data     *Manifest
	lock     *dirLock

	// gcGate 容量为 1 的通道，充当进程内回收互斥锁，支持超时获取。
	gcGate chan struct{}
}

// Open 打开（必要时初始化）位于 cfg.Dir 的本地存储，并执行崩溃恢复。
// 返回的 RecoveryReport 记录恢复的判定依据与结果。同一目录在任何时刻
// 只能被一个进程/句柄打开，否则返回 ErrBusy。
func Open(ctx context.Context, cfg Config) (*Store, *RecoveryReport, error) {
	if err := cfg.normalize(); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, nil, err
	}
	lk, err := acquireDirLock(cfg.Dir)
	if err != nil {
		return nil, nil, err
	}

	bs := newBlobStore(cfg.Dir, cfg.WriteFsync)
	if err := bs.ensureDirs(); err != nil {
		_ = lk.release()
		return nil, nil, err
	}
	mf := newManifestStore(cfg.Dir, cfg.WriteFsync)
	data, err := mf.load()
	if err != nil {
		_ = lk.release()
		return nil, nil, err
	}

	s := &Store{
		cfg:      cfg,
		manifest: mf,
		blobs:    bs,
		data:     data,
		lock:     lk,
		gcGate:   make(chan struct{}, 1),
	}

	report, err := s.recoverStorage(ctx)
	if err != nil {
		_ = lk.release()
		return nil, nil, err
	}
	if report.changed() {
		if err := mf.save(data); err != nil {
			_ = lk.release()
			return nil, nil, err
		}
	}
	return s, report, nil
}

// Put 写入一个对象版本。
//
// 流程：数据先全部进入 staging 临时文件并同步计算 SHA-256，校验后原子
// 重命名为内容块，最后才提交清单。因此：
//   - 相同内容（相同字节）永远映射到同一个内容块，重复写入不产生副本；
//   - 内容标识只取决于字节，与分块边界、写入顺序无关；
//   - 写入中途崩溃只会残留 staging 文件，恢复时被清理，绝不会出现
//     “能读到半个对象”或清单指向残缺数据的情况。
func (s *Store) Put(ctx context.Context, name string, r io.Reader) (*VersionInfo, error) {
	if name == "" {
		return nil, errors.Join(ErrInvalid, errors.New("object name is empty"))
	}
	if r == nil {
		return nil, errors.Join(ErrInvalid, errors.New("reader is nil"))
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	s.mu.RUnlock()

	// 流式写 staging 期间不持有元数据锁：多个大对象写入可真正并行。
	// 每个写入使用独立临时文件，提交靠原子 rename 去重，天然无冲突。
	w, err := s.blobs.Begin()
	if err != nil {
		return nil, err
	}
	cancelled := false
	defer func() {
		if !cancelled {
			w.Cancel() // 已 Commit 后 Cancel 为空操作
		}
	}()

	copyDone := make(chan error, 1)
	go func() {
		buf := make([]byte, hashBlockSize)
		_, e := io.CopyBuffer(w, r, buf)
		copyDone <- e
	}()
	select {
	case err := <-copyDone:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		// 先让进行中的 Write 尽快失败，再等待复制协程完全退出，
		// 最后才能安全清理临时文件（避免与复制协程并发写同一文件）。
		// 因此 ctx 取消要求 reader 在上下文取消后返回（关闭管道/返回错误）。
		w.abort()
		<-copyDone
		w.Cancel()
		cancelled = true
		return nil, ctx.Err()
	}

	// rename 与元数据登记同处一把锁的临界区：GC 必须持有同一把锁才能扫描
	// 磁盘并删除 blob，因此即便宽限期为 0，也不存在“blob 已落盘但尚无版本
	// 引用”的窗口被 GC 误删（避免 Put 成功却首次读取即 ErrReclaimed）。
	s.mu.Lock()
	defer s.mu.Unlock()
	id, size, err := w.commitLocked()
	if err != nil {
		return nil, err
	}

	if s.closed {
		// 存储已关闭：内容块可能成为孤儿，由下次打开后的 GC 回收。
		return nil, ErrClosed
	}

	now := time.Now().UTC()
	_, contentExisted := s.data.Content[id]
	if !contentExisted {
		s.data.Content[id] = BlobMeta{Size: size, CreatedAt: now}
	}
	rec := VersionRecord{
		ID:        s.data.Counter,
		Object:    name,
		ContentID: id,
		Size:      size,
		CreatedAt: now,
		State:     VersionLive,
	}
	s.data.Counter++
	s.data.Objects[name] = append(s.data.Objects[name], rec)

	if err := s.manifest.save(s.data); err != nil {
		// 清单提交失败：对象视为未写入。回滚内存中的追加，保持句柄状态
		// 与磁盘上仍为旧清单一致。内容块可能成为孤儿，由 GC 回收。
		s.rollbackLastVersion(name, rec.ID)
		if !contentExisted {
			delete(s.data.Content, id)
		}
		s.data.Counter--
		return nil, err
	}
	info := toInfo(rec)
	return &info, nil
}

// rollbackLastVersion 移除内存清单中刚追加的版本（保存失败时使用）。
func (s *Store) rollbackLastVersion(name string, id int64) {
	recs := s.data.Objects[name]
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].ID == id {
			s.data.Objects[name] = append(recs[:i], recs[i+1:]...)
			break
		}
	}
	if len(s.data.Objects[name]) == 0 {
		delete(s.data.Objects, name)
	}
}

// Get 读取指定版本；versionID<=0 表示读取当前（最新的存活）版本。
// 返回的 ReadCloser 在整个读取过程中持有读锁，Close 后释放。
//
// 错误可通过 errors.Is 区分：
//   - ErrNotFound / ErrVersionNotFound：名字或版本不存在；
//   - ErrDeleted：版本存在但已被删除（仍可 Restore）；
//   - ErrReclaimed：版本的数据已被回收或丢失，永久不可读；
//   - ErrChecksum：数据存在但内容校验失败。
func (s *Store) Get(ctx context.Context, name string, versionID int64) (io.ReadCloser, *VersionInfo, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, nil, ErrClosed
	}

	recs, ok := s.data.Objects[name]
	if !ok || len(recs) == 0 {
		s.mu.RUnlock()
		return nil, nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	var rec VersionRecord
	if versionID > 0 {
		found := false
		for i := range recs {
			if recs[i].ID == versionID {
				rec = recs[i] // 拷贝：解锁后只使用本地副本，避免与原地修改竞争
				found = true
				break
			}
		}
		if !found {
			s.mu.RUnlock()
			return nil, nil, fmt.Errorf("%w: %s#%d", ErrVersionNotFound, name, versionID)
		}
	} else {
		// “当前版本”即版本序列的最后一个版本：它存活则可读，否则按其
		// 实际状态给出明确拒绝原因（删除/回收/损坏），绝不静默回退到旧版本。
		rec = recs[len(recs)-1] // 拷贝到局部变量
		if rec.State != VersionLive {
			s.mu.RUnlock()
			switch rec.State {
			case VersionDeleted:
				return nil, nil, fmt.Errorf("%w: %s (current version %d)", ErrDeleted, name, rec.ID)
			case VersionReclaimed:
				return nil, nil, fmt.Errorf("%w: %s (current version %d)", ErrReclaimed, name, rec.ID)
			case VersionCorrupt:
				return nil, nil, fmt.Errorf("%w: %s (current version %d)", ErrChecksum, name, rec.ID)
			default:
				return nil, nil, fmt.Errorf("%w: %s", ErrNotFound, name)
			}
		}
	}

	switch rec.State {
	case VersionDeleted:
		s.mu.RUnlock()
		return nil, nil, fmt.Errorf("%w: %s#%d", ErrDeleted, name, rec.ID)
	case VersionReclaimed:
		s.mu.RUnlock()
		return nil, nil, fmt.Errorf("%w: %s#%d", ErrReclaimed, name, rec.ID)
	case VersionCorrupt:
		s.mu.RUnlock()
		return nil, nil, fmt.Errorf("%w: %s#%d", ErrChecksum, name, rec.ID)
	}

	rc, err := s.blobs.Open(rec.ContentID)
	if err != nil {
		s.mu.RUnlock()
		return nil, nil, err
	}
	info := toInfo(rec)
	// 读锁随流的 Close 释放；因此 GC/删除不会在读取途中移除底层数据。
	return &lockedReadCloser{rc: rc, unlock: s.mu.RUnlock}, &info, nil
}

// lockedReadCloser 将读锁的释放绑定到流关闭动作上。
type lockedReadCloser struct {
	rc     io.ReadCloser
	unlock func()
	once   sync.Once
}

func (l *lockedReadCloser) Read(p []byte) (int, error) { return l.rc.Read(p) }
func (l *lockedReadCloser) Close() error {
	var err error
	l.once.Do(func() { err = l.rc.Close(); l.unlock() })
	return err
}

var _ io.ReadCloser = (*lockedReadCloser)(nil)

// Versions 列出对象的全部版本（版本号升序），包含已删除/已回收版本。
func (s *Store) Versions(ctx context.Context, name string) ([]VersionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	recs, ok := s.data.Objects[name]
	if !ok || len(recs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	out := make([]VersionInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, toInfo(r))
	}
	return out, nil
}

// Delete 将指定版本（versionID<=0 时为最新存活版本）标记为删除。
// 删除是逻辑删除：在 GC 回收数据之前可通过 Restore 恢复。
func (s *Store) Delete(ctx context.Context, name string, versionID int64) (*VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	recs, ok := s.data.Objects[name]
	if !ok || len(recs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	idx := -1
	if versionID > 0 {
		for i := range recs {
			if recs[i].ID == versionID {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("%w: %s#%d", ErrVersionNotFound, name, versionID)
		}
	} else {
		// 当前版本 = 版本序列的最后一个版本；仅当其存活时可删。
		last := len(recs) - 1
		if recs[last].State == VersionLive {
			idx = last
		} else {
			return nil, fmt.Errorf("%w: %s current version %d is %s",
				ErrInvalid, name, recs[last].ID, recs[last].State)
		}
	}
	if recs[idx].State == VersionDeleted {
		info := toInfo(recs[idx])
		return &info, nil // 删除幂等
	}
	if recs[idx].State != VersionLive {
		return nil, fmt.Errorf("%w: %s#%d state=%s", ErrInvalid, name, recs[idx].ID, recs[idx].State)
	}
	recs[idx].State = VersionDeleted
	recs[idx].DeletedAt = time.Now().UTC()
	if err := s.manifest.save(s.data); err != nil {
		return nil, err
	}
	info := toInfo(recs[idx])
	return &info, nil
}

// Restore 将一个已删除版本恢复为存活。恢复后它即成为该对象的最新存活版本，
// 因此“当前版本”读取的可见性在任何时刻都唯一确定。
func (s *Store) Restore(ctx context.Context, name string, versionID int64) (*VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	recs, ok := s.data.Objects[name]
	if !ok || len(recs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if versionID <= 0 {
		return nil, errors.Join(ErrInvalid, errors.New("versionID is required for restore"))
	}
	for i := range recs {
		if recs[i].ID != versionID {
			continue
		}
		switch recs[i].State {
		case VersionDeleted:
			recs[i].State = VersionLive
			recs[i].DeletedAt = time.Time{}
			if err := s.manifest.save(s.data); err != nil {
				return nil, err
			}
			info := toInfo(recs[i])
			return &info, nil
		case VersionLive:
			info := toInfo(recs[i])
			return &info, nil
		case VersionReclaimed:
			return nil, fmt.Errorf("%w: %s#%d cannot restore reclaimed version", ErrReclaimed, name, versionID)
		case VersionCorrupt:
			return nil, fmt.Errorf("%w: %s#%d cannot restore corrupt version", ErrChecksum, name, versionID)
		}
	}
	return nil, fmt.Errorf("%w: %s#%d", ErrVersionNotFound, name, versionID)
}

// Close 关闭存储并释放目录锁。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	return s.lock.release()
}
