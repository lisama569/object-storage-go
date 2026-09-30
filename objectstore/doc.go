// Package objectstore implements a local-disk, content-addressed,
// versioned object store with crash-safe writes, soft delete, explicit
// purging, retention (legal) holds, capacity-watermark-driven reclamation
// and idempotent garbage collection.
//
// Retention (see Retain, ReleaseRetention, Retention, ListRetained):
// a version can be protected by either a permanent hold or a timed hold
// with an explicit expiry. Holds only ever strengthen (retain extends;
// never shortens): repeating an identical or weaker request is a true
// no-op that does not even rewrite the manifest. They are persisted in
// the atomic manifest so a process restart never loses protection, and
// forbid soft delete, purge and GC collection while effective. Reads are
// never blocked by a hold; reading a soft-deleted version that is still
// held returns an error matching both ErrDeleted and ErrRetained.
//
// Watermark (see Options.WatermarkBytes and Sweep): when the total size
// of committed blob files exceeds the configured mark, Sweep first removes
// orphan blobs and then collects soft-deleted versions whose retention is
// absent or expired, in a fixed priority: versions whose hold was
// explicitly released early first, then versions whose timed hold expired
// naturally, then versions that never had a hold; within each class the
// oldest goes first. Active versions and retained versions are never
// sacrificed; if the level still cannot be reached, Sweep returns
// ErrWatermarkExceeded explicitly.
//
// See README.md for full semantics and recovery guarantees.
package objectstore
