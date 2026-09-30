// Package objectstore implements a local-disk, content-addressed,
// versioned object store with crash-safe writes, soft delete, explicit
// purging, retention (legal) holds, capacity-watermark-driven reclamation
// and idempotent garbage collection.
//
// Retention (see Retain, ReleaseRetention, Retention, ListRetained):
// a version can be protected by either a permanent hold or a timed hold
// with an explicit expiry. Holds only ever strengthen (retain extends;
// never shortens), are persisted in the atomic manifest so a process
// restart never loses protection, and forbid soft delete, purge and GC
// collection while effective. Repeating an identical request or issuing
// a weaker one is a true side-effect-free no-op: no manifest commit is
// attempted. Reads are never blocked by a hold; a read of a version that
// is soft-deleted while still held returns ErrDeletedRetained, which
// matches both ErrDeleted and ErrRetained via errors.Is.
//
// Watermark (see Options.WatermarkBytes and Sweep): when the total size
// of committed blob files exceeds the configured mark, Sweep first removes
// orphan blobs and then collects soft-deleted versions without an active
// hold in a fixed tier order — early-released holds first, naturally
// expired holds second, never-held versions last; oldest Created and
// digest break ties within a tier — just until usage is back at the mark.
// Active versions and retained versions are never sacrificed; if the level
// still cannot be reached, Sweep returns ErrWatermarkExceeded explicitly.
//
// See README.md for full semantics and recovery guarantees.
package objectstore
