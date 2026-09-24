package objectstore

import "errors"

var (
	// ErrNotFound 对象名不存在。
	ErrNotFound = errors.New("objectstore: object not found")
	// ErrVersionNotFound 指定版本不存在。
	ErrVersionNotFound = errors.New("objectstore: version not found")
	// ErrDeleted 版本存在但处于“已删除”状态，尚未被回收。
	ErrDeleted = errors.New("objectstore: version is deleted")
	// ErrReclaimed 版本存在过，但其数据已被回收。
	ErrReclaimed = errors.New("objectstore: version has been reclaimed")
	// ErrChecksum 数据校验失败（内容与标识不一致或损坏）。
	ErrChecksum = errors.New("objectstore: checksum mismatch")
	// ErrClosed Store 已关闭。
	ErrClosed = errors.New("objectstore: store closed")
	// ErrBusy 存储目录已被其他进程/句柄以独占方式打开。
	ErrBusy = errors.New("objectstore: store directory is locked by another process")
	// ErrLockHeld ...
	ErrLockHeld = errors.New("objectstore: lock already held")
	// ErrInvalid 参数非法。
	ErrInvalid = errors.New("objectstore: invalid argument")
)
