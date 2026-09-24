package objectstore

import (
	"errors"
	"time"
)

// Config 是存储引擎的可调参数。
type Config struct {
	// Dir 数据根目录（必填）。
	Dir string
	// MaxUnreferencedBlobAge 未被任何存活/已删除版本引用的 blob 在可被回收前
	// 至少需要存在多久（宽限期，避免回收与进行中的写入竞争）。0 表示立即可回收。
	MaxUnreferencedBlobAge time.Duration
	// GCLockTimeout 等待回收锁的最长时间；0 表示不等待。
	GCLockTimeout time.Duration
	// VerifyOnOpen 打开时是否对引用中的 blob 做全量 SHA-256 校验。
	VerifyOnOpen bool
	// WriteFsync 是否在关键落盘点调用 fsync（崩溃安全所需，默认 true）。
	WriteFsync bool
}

// DefaultConfig 返回以 dir 为根目录的默认配置：
// 宽限期 1s、GC 不等待锁、打开时不全量校验、写入启用 fsync。
func DefaultConfig(dir string) Config {
	return Config{
		Dir:                    dir,
		MaxUnreferencedBlobAge: time.Second,
		GCLockTimeout:          0,
		VerifyOnOpen:           false,
		WriteFsync:             true,
	}
}

// normalize 补全默认值并校验参数。
func (c *Config) normalize() error {
	if c.Dir == "" {
		return errors.Join(ErrInvalid, errors.New("Dir is required"))
	}
	if c.MaxUnreferencedBlobAge < 0 {
		return errors.Join(ErrInvalid, errors.New("MaxUnreferencedBlobAge must be >= 0"))
	}
	if c.GCLockTimeout < 0 {
		return errors.Join(ErrInvalid, errors.New("GCLockTimeout must be >= 0"))
	}
	return nil
}
