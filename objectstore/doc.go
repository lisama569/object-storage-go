// Package objectstore implements a local-disk, content-addressed,
// versioned object store with crash-safe writes, soft delete, explicit
// purging, idempotent garbage collection, per-version retention ("legal
// hold") and capacity-watermark reclamation.
//
// # Retention
//
// PutRetention attaches a durable, restart-surviving retention to one
// version: either a deadline (RetainUntil) or a permanent hold
// (RetainPermanent). While a retention is active the version's bytes can
// never be physically reclaimed, by PurgeVersion, by Sweep, or by
// capacity-watermark eviction. A retention can be extended
// (ExtendRetention, strictly monotonic), released early
// (ReleaseRetention, durable and idempotent), or allowed to lapse when its
// deadline passes. RetentionOf / Retentions report the current state.
// Soft-deleted versions under retention read with ErrDeleted that also
// matches ErrRetained via errors.Is; they never look like ErrNotFound.
//
// # Capacity watermark
//
// Options.MaxPhysicalBytes sets a high-water mark on bytes present under
// blobs/ (Stats.OnDiskBytes). SweepToWatermark (and, when the option is
// set, ordinary Sweep) first removes unreferenced orphan blobs, then
// reclaims soft-deleted versions in a fixed priority — explicitly
// released, retention-expired, ordinary — until usage is at or below the
// mark. Active versions and retained versions are never reclaimed; if the
// mark still cannot be met after every eligible version is reclaimed,
// ErrWatermark is returned without touching protected data.
//
// See README.md for full semantics, crash-safety ordering and recovery
// guarantees.
package objectstore
