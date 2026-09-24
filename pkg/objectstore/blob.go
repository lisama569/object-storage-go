package objectstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// blobStore 负责数据块（blob）的内容寻址存储。
//
// 磁盘布局：
//
//	<root>/blobs/<id前2字符>/<id>   只读的不可变内容块
//	<root>/staging/tmp-<随机>        写入中的临时文件（崩溃后由恢复流程清理）
//
// blob 一旦以某 id 落位即不可变；提交是“写临时文件 + fsync + rename”的原子过程，
// 因此写入中途崩溃只会留下 staging 残文件，绝不会产生被误读的半成品 blob。
type blobStore struct {
	root    string // blobs 目录
	staging string // staging 目录
	fsync   bool
}

func newBlobStore(dir string, fsync bool) *blobStore {
	return &blobStore{
		root:    filepath.Join(dir, "blobs"),
		staging: filepath.Join(dir, "staging"),
		fsync:   fsync,
	}
}

// ensureDirs 创建 blobs/staging 目录。
func (b *blobStore) ensureDirs() error {
	if err := os.MkdirAll(b.root, 0o755); err != nil {
		return err
	}
	return os.MkdirAll(b.staging, 0o755)
}

// validateID 校验内容标识格式，杜绝路径穿越。
func validateID(id ContentID) error {
	s := string(id)
	if len(s) != 64 {
		return fmt.Errorf("%w: bad content id length", ErrInvalid)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("%w: content id not hex", ErrInvalid)
	}
	return nil
}

// blobPath 返回内容块的落盘路径。
func (b *blobStore) blobPath(id ContentID) (string, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	s := string(id)
	return filepath.Join(b.root, s[:2], s), nil
}

// Begin 在 staging 中创建一个匿名临时写入句柄；内容标识在 Commit 时
// 根据实际字节计算，调用方无需预先知道哈希。
func (b *blobStore) Begin() (*BlobWriter, error) {
	if err := os.MkdirAll(b.staging, 0o755); err != nil {
		return nil, err
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, err
	}
	tmp := filepath.Join(b.staging, "tmp-"+hex.EncodeToString(buf[:]))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &BlobWriter{
		store: b,
		f:     f,
		tmp:   tmp,
		h:     sha256.New(),
	}, nil
}

// Open 打开一个已存在的内容块；返回的 reader 在读取全程做 SHA-256 校验，
// 一旦内容与 id 不符，Read 返回 ErrChecksum，绝不静默返回损坏数据。
func (b *blobStore) Open(id ContentID) (io.ReadCloser, error) {
	p, err := b.blobPath(id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: blob %s missing", ErrReclaimed, id)
		}
		return nil, err
	}
	return &verifyingReader{f: f, want: id, h: sha256.New()}, nil
}

// Stat 返回内容块字节大小；不存在时 ok=false。
func (b *blobStore) Stat(id ContentID) (size int64, ok bool, err error) {
	p, err := b.blobPath(id)
	if err != nil {
		return 0, false, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return fi.Size(), true, nil
}

// Exists 判断内容块是否已存在。
func (b *blobStore) Exists(id ContentID) (bool, error) {
	_, ok, err := b.Stat(id)
	return ok, err
}

// statInfo 返回内容块的 FileInfo（恢复时取 mtime 用）。
func (b *blobStore) statInfo(id ContentID) (os.FileInfo, error) {
	p, err := b.blobPath(id)
	if err != nil {
		return nil, err
	}
	return os.Stat(p)
}

// Remove 删除一个内容块（回收用）；不存在不报错（回收可安全重复执行）。
func (b *blobStore) Remove(id ContentID) error {
	p, err := b.blobPath(id)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// listStaging 返回残留的临时文件路径（恢复用）。
func (b *blobStore) listStaging() ([]string, error) {
	entries, err := os.ReadDir(b.staging)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, filepath.Join(b.staging, e.Name()))
	}
	return out, nil
}

// listBlobs 返回磁盘上全部内容块的 id 与大小（恢复/统计用）。
func (b *blobStore) listBlobs() (map[ContentID]int64, error) {
	out := map[ContentID]int64{}
	shards, err := os.ReadDir(b.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}
	for _, sh := range shards {
		if !sh.IsDir() || len(sh.Name()) != 2 {
			continue
		}
		files, err := os.ReadDir(filepath.Join(b.root, sh.Name()))
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			id := ContentID(f.Name())
			if err := validateID(id); err != nil {
				continue // 忽略非 blob 杂物
			}
			info, err := f.Info()
			if err != nil {
				return nil, err
			}
			out[id] = info.Size()
		}
	}
	return out, nil
}

// BlobWriter 是 blob 的原子写入句柄：边写边哈希，提交时才算出内容标识。
type BlobWriter struct {
	store   *blobStore
	f       *os.File
	tmp     string
	h       hash.Hash // sha256 hasher
	n       int64
	mu      sync.Mutex
	done    bool
	aborted bool
}

// abort 请求中止：此后所有 Write 立即返回错误，供 ctx 取消时尽快打断复制。
func (w *BlobWriter) abort() {
	w.mu.Lock()
	w.aborted = true
	w.mu.Unlock()
}

// Write 将 p 写入临时文件并计入哈希。
func (w *BlobWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done || w.aborted {
		return 0, ErrClosed
	}
	n, err := w.f.Write(p)
	if n > 0 {
		_, _ = w.h.Write(p[:n])
		w.n += int64(n)
	}
	return n, err
}

// Commit 计算全部写入字节的 SHA-256 作为内容标识，原子重命名为正式 blob。
// 若同名 blob 已存在（去重命中或上次崩溃在 rename 后），复用已有内容块，
// 绝不产生第二份副本。返回内容标识与字节数。
func (w *BlobWriter) Commit() (ContentID, int64, error) {
	w.mu.Lock()
	if w.done || w.aborted {
		w.mu.Unlock()
		return "", 0, ErrClosed
	}
	w.done = true
	w.mu.Unlock()
	return w.commitLocked()
}

// commitLocked 假定调用方已处理 done/aborted 检查；执行 fsync 与原子 rename。
func (w *BlobWriter) commitLocked() (ContentID, int64, error) {
	id := ContentID(hex.EncodeToString(w.h.Sum(nil)))

	if w.store.fsync {
		if err := w.f.Sync(); err != nil {
			_ = w.cleanup()
			return "", 0, err
		}
	}
	if err := w.f.Close(); err != nil {
		_ = w.cleanup()
		return "", 0, err
	}

	dst, err := w.store.blobPath(id)
	if err != nil {
		_ = w.cleanup()
		return "", 0, err
	}

	// 去重命中：相同内容已落盘，不得再产生副本。
	if ok, _ := w.store.Exists(id); ok {
		_ = os.Remove(w.tmp)
		return id, w.n, nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		_ = w.cleanup()
		return "", 0, err
	}
	if err := os.Rename(w.tmp, dst); err != nil {
		_ = w.cleanup()
		return "", 0, err
	}
	if w.store.fsync {
		_ = fsyncDir(filepath.Dir(dst))
	}
	return id, w.n, nil
}

// Cancel 放弃写入并清理临时文件。必须在复制协程停止后调用。
func (w *BlobWriter) Cancel() {
	w.mu.Lock()
	if w.done {
		w.mu.Unlock()
		return
	}
	w.done = true
	w.mu.Unlock()
	_ = w.cleanup()
}

func (w *BlobWriter) cleanup() error {
	if w.f != nil {
		_ = w.f.Close()
	}
	return os.Remove(w.tmp)
}

// verifyingReader 边读边校验，内容与期望 id 不符时在最后一次 Read 返回 ErrChecksum。
type verifyingReader struct {
	f       *os.File
	want    ContentID
	h       hash.Hash
	checked bool
}

func (r *verifyingReader) Read(p []byte) (int, error) {
	n, err := r.f.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		if !r.checked {
			r.checked = true
			got := ContentID(hex.EncodeToString(r.h.Sum(nil)))
			if got != r.want {
				return n, fmt.Errorf("%w: blob %s corrupt (got %s)", ErrChecksum, r.want, got)
			}
		}
	}
	return n, err
}

func (r *verifyingReader) Close() error { return r.f.Close() }

var _ io.ReadCloser = (*verifyingReader)(nil)

// fsyncDir 对目录执行 fsync，保证其中的 rename/create 落盘。
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

var _ io.Writer = (*BlobWriter)(nil)
