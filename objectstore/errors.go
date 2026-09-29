package objectstore

import "errors"

// Sentine errors classify the reason a read or mutation is rejected.
//
// Callers can distinguish "never existed" from "currently deleted" from
// "data physically reclaimed" with errors.Is.
var (
	// ErrNotFound means the object (or version) never existed.
	ErrNotFound = errors.New("objectstore: not found")

	// ErrDeleted means the version exists in metadata but is marked deleted
	// (soft delete). The data is still on disk and the version can be restored.
	ErrDeleted = errors.New("objectstore: version is deleted")

	// ErrCollected means the version was explicitly purged or reclaimed by
	// garbage collection. Its metadata entry is retained for history but the
	// data can never be read or restored again.
	ErrCollected = errors.New("objectstore: version has been collected")

	// ErrCurrent means an operation was refused because the target version
	// is the current live version (for example purging it).
	ErrCurrent = errors.New("objectstore: cannot purge current version")

	// ErrRetained means a destructive operation was refused because the
	// version is protected by an active retention hold (a timed hold that
	// has not expired, or a permanent hold). Reading is unaffected.
	ErrRetained = errors.New("objectstore: version is under retention")

	// ErrWatermarkExceeded means a watermark-driven reclaim pass removed
	// everything safely reclaimable (orphans, expired/unheld soft-deleted
	// data) but total disk usage is still above the configured mark. No
	// protected or active data was deleted to force the level down.
	ErrWatermarkExceeded = errors.New("objectstore: disk usage still exceeds watermark after reclaim")

	// ErrLocked means another process already has the store open.
	ErrLocked = errors.New("objectstore: store is locked by another process")

	// ErrDigestMismatch means data read from disk failed integrity
	// verification against its content address.
	ErrDigestMismatch = errors.New("objectstore: content digest mismatch")

	// ErrName means an object name is invalid.
	ErrName = errors.New("objectstore: invalid object name")
)
