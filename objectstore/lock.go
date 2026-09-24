package objectstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// fileLock is an advisory, whole-store lock held for the lifetime of an
// open Store. It serializes multiple processes pointing at the same
// directory (goroutines within one process are serialized by the Store's
// internal mutex).
//
// It is implemented with flock(2) on the file "<dir>/.lock". flock is tied
// to the open file description and is automatically released if the
// process dies (even with SIGKILL/power loss once the OS reaps it), so a
// crashed writer never leaves a permanently stuck lock.
type fileLock struct {
	f *os.File
}

// lockFileName is the lock file name inside the store directory.
const lockFileName = ".lock"

// acquireLock takes a non-blocking exclusive flock on <dir>/.lock,
// creating the file if needed. It returns ErrLocked when another process
// already holds it.
func acquireLock(dir string) (*fileLock, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	// LOCK_NB: fail immediately instead of blocking if another holder.
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("acquire flock: %w", err)
	}
	return &fileLock{f: f}, nil
}

// release unlocks and closes the lock file, making the store available
// to other processes immediately.
func (l *fileLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	// LOCK_UN releases the advisory lock; closing the fd also releases it.
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
