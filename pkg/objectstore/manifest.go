package objectstore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// manifestVersion 是清单格式版本。
const manifestVersion = 1

// VersionState 版本生命周期状态。
type VersionState int

const (
	// VersionLive 存活，可读。
	VersionLive VersionState = iota
	// VersionDeleted 已删除（可恢复），底层数据尚未回收。
	VersionDeleted
	// VersionReclaimed 数据已被回收（或数据丢失），永久不可读、不可恢复。
	VersionReclaimed
	// VersionCorrupt 引用的数据校验失败，不可读。
	VersionCorrupt
)

// String 便于日志输出。
func (v VersionState) String() string {
	switch v {
	case VersionLive:
		return "LIVE"
	case VersionDeleted:
		return "DELETED"
	case VersionReclaimed:
		return "RECLAIMED"
	case VersionCorrupt:
		return "CORRUPT"
	default:
		return "UNKNOWN"
	}
}

// VersionRecord 描述对象的一个版本。
type VersionRecord struct {
	ID        int64        `json:"id"`
	Object    string       `json:"object"`
	ContentID ContentID    `json:"content_id"`
	Size      int64        `json:"size"`
	CreatedAt time.Time    `json:"created_at"`
	DeletedAt time.Time    `json:"deleted_at,omitempty"`
	State     VersionState `json:"state"`
}

// BlobMeta 记录内容块大小与首次登记时间（用量统计/回收宽限判定用）。
type BlobMeta struct {
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

// Manifest 是元数据根结构，以单一 JSON 文件原子替换方式持久化。
type Manifest struct {
	FormatVersion int                        `json:"format_version"`
	Counter       int64                      `json:"counter"` // 下一个可用版本号
	Objects       map[string][]VersionRecord `json:"objects"`
	Content       map[ContentID]BlobMeta     `json:"content"`
}

func newManifest() *Manifest {
	return &Manifest{
		FormatVersion: manifestVersion,
		Counter:       1,
		Objects:       map[string][]VersionRecord{},
		Content:       map[ContentID]BlobMeta{},
	}
}

// manifestStore 负责 manifest.json 的读取与原子替换。
//
// 持久化顺序：写同级临时文件 -> fsync 文件 -> rename 原子替换 -> fsync 目录。
// 崩溃只可能发生在“旧清单”或“完整新清单”二者之一，绝不会读到半截 JSON。
type manifestStore struct {
	path  string
	dir   string
	fsync bool
}

func newManifestStore(dir string, fsync bool) *manifestStore {
	return &manifestStore{
		path:  filepath.Join(dir, "manifest.json"),
		dir:   dir,
		fsync: fsync,
	}
}

// load 读取清单；文件不存在时返回空清单（全新存储）。
func (m *manifestStore) load() (*Manifest, error) {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return newManifest(), nil
		}
		return nil, err
	}
	var mf Manifest
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, err
	}
	if mf.FormatVersion != manifestVersion {
		return nil, errors.New("objectstore: unsupported manifest format version")
	}
	if mf.Objects == nil {
		mf.Objects = map[string][]VersionRecord{}
	}
	if mf.Content == nil {
		mf.Content = map[ContentID]BlobMeta{}
	}
	if mf.Counter == 0 {
		mf.Counter = 1
	}
	return &mf, nil
}

// listTempFiles 返回崩溃后残留的 manifest 原子替换临时文件。
func (m *manifestStore) listTempFiles() ([]string, error) {
	entries, err := os.ReadDir(m.dir)
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
		if name, ok := strings.CutPrefix(e.Name(), "manifest.json.tmp-"); ok && name != "" {
			out = append(out, filepath.Join(m.dir, e.Name()))
		}
	}
	return out, nil
}

// save 原子替换清单文件。
func (m *manifestStore) save(mf *Manifest) error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	tmp := filepath.Join(m.dir, "manifest.json.tmp-"+hex.EncodeToString(buf[:]))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if m.fsync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if m.fsync {
		_ = fsyncDir(m.dir)
	}
	return nil
}
