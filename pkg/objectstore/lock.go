package objectstore

import (
	"errors"
	"os"
	"syscall"
)

// dirLock 基于锁文件 + flock(2) 的跨进程独占锁。
// 进程异常退出时内核自动释放 flock，不会留下永久持有的锁。
type dirLock struct {
	path string
	f    *os.File
}

// acquireDirLock 以非阻塞方式获取目录独占锁；被占用时返回 ErrBusy。
func acquireDirLock(dir string) (*dirLock, error) {
	path := dir + "/lock"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return &dirLock{path: path, f: f}, nil
}

// release 释放 flock 并关闭锁文件（文件本身保留，无妨）。
func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
