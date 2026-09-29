package objectstore

import (
	"time"
)

// VersionState is the lifecycle state of a single object version.
type VersionState int

const (
	// StateActive is a normal, readable version.
	StateActive VersionState = iota
	// StateDeleted is a soft-deleted version (data retained, restorable).
	StateDeleted
	// StateCollected is a version whose data has been physically reclaimed.
	// Only the metadata tombstone remains.
	StateCollected
)

// VersionInfo is the immutable descriptor of one object version.
type VersionInfo struct {
	Object   string       `json:"object"`
	Version  int64        `json:"version"`
	Size     int64        `json:"size"`
	Digest   string       `json:"digest"`
	Created  time.Time    `json:"created"`
	Deleted  bool         `json:"deleted"`
	State    VersionState `json:"state"`
	PurgedAt time.Time    `json:"purged_at,omitempty"`
	// Retained reports whether the version currently holds an active
	// retention (permanent or not yet expired). It is purely informational;
	// use RetentionStatus for the full policy.
	Retained bool `json:"retained"`
	// Permanent reports whether the active retention is permanent.
	Permanent bool `json:"permanent,omitempty"`
	// RetainUntil is the retention deadline. It is the zero time for a
	// permanent retention or when no retention is set.
	RetainUntil time.Time `json:"retain_until,omitempty"`
}

// Retention describes a retention policy applied to one version.
//
// The zero value is not a valid policy: construct one with
// RetainUntil(deadline) or RetainPermanent().
type Retention struct {
	// Permanent keeps the version until an explicit ReleaseRetention,
	// regardless of elapsed time.
	Permanent bool `json:"permanent"`
	// Until is the deadline of a time-bounded retention. Protection ends
	// at this instant. Ignored when Permanent is true.
	Until time.Time `json:"until,omitempty"`
}

// RetainUntil returns a time-bounded retention expiring at until.
func RetainUntil(until time.Time) Retention {
	return Retention{Until: until.UTC()}
}

// RetainPermanent returns a retention that never expires on its own.
func RetainPermanent() Retention {
	return Retention{Permanent: true}
}

// RetentionStatus is the durable retention state of one version.
type RetentionStatus struct {
	Object    string    `json:"object"`
	Version   int64     `json:"version"`
	Retained  bool      `json:"retained"`
	Permanent bool      `json:"permanent,omitempty"`
	Until     time.Time `json:"until,omitempty"`
	// ReleasedAt is set when an active retention was explicitly released
	// ahead of its deadline. Such data is the first thing reclaimed under
	// capacity pressure.
	ReleasedAt time.Time `json:"released_at,omitempty"`
}

// Stats reports logical usage and physical disk usage.
type Stats struct {
	Objects          int   `json:"objects"`
	LiveVersions     int   `json:"live_versions"`
	DeletedVersions  int   `json:"deleted_versions"`
	CollectedVersion int   `json:"collected_versions"`
	ReferencedBlobs  int   `json:"referenced_blobs"`
	OrphanBlobs      int   `json:"orphan_blobs"`
	LogicalBytes     int64 `json:"logical_bytes"`
	PhysicalBytes    int64 `json:"physical_bytes"`
	ReclaimableBytes int64 `json:"reclaimable_bytes"`
	// OnDiskBytes is the total byte size of every committed blob file
	// currently present under blobs/ (referenced data, soft-deleted data
	// and unreferenced orphans alike). Capacity watermarks are evaluated
	// against this number.
	OnDiskBytes int64 `json:"on_disk_bytes"`
	// RetainedBytes counts, once per digest, blob bytes that some version
	// with an active retention depends on. Such data is never reclaimed.
	RetainedBytes int64 `json:"retained_bytes"`
}

// WatermarkReport describes the outcome of a capacity-driven reclaim pass.
type WatermarkReport struct {
	// Watermark is the configured high-water mark in bytes (0 = unlimited).
	Watermark int64 `json:"watermark"`
	// OnDiskBefore / OnDiskAfter bound the work the pass performed.
	OnDiskBefore int64 `json:"on_disk_before"`
	OnDiskAfter  int64 `json:"on_disk_after"`
	// OrphanBlobsRemoved is the number of unreferenced blob files deleted.
	OrphanBlobsRemoved int `json:"orphan_blobs_removed"`
	// VersionsCollected is the number of versions transitioned to the
	// irreversible Collected state.
	VersionsCollected int `json:"versions_collected"`
	// BytesReclaimed is how much on-disk usage dropped.
	BytesReclaimed int64 `json:"bytes_reclaimed"`
	// BelowWatermark reports whether OnDiskAfter <= Watermark.
	BelowWatermark bool `json:"below_watermark"`
}

// Options configures how a Store is opened. The zero value means safe
// defaults: read verification on, conservative (data-preserving) GC.
type Options struct {
	// DisableReadVerification turns off re-hashing bytes on read. By
	// default (zero value) every read is verified against the version's
	// content address and ErrDigestMismatch is returned on any discrepancy.
	DisableReadVerification bool
	// SweepDeletedVersions lets Sweep physically reclaim blob data
	// referenced only by soft-deleted versions and convert those versions
	// to the irreversible Collected state. Default false: deleted data is
	// retained and only true orphan blobs are removed.
	//
	// Retention still applies with this option: versions holding an active
	// retention are never collected; released and retention-expired
	// versions are eligible.
	SweepDeletedVersions bool
	// MaxPhysicalBytes is the capacity high-water mark measured in bytes
	// actually present under blobs/ (Stats.OnDiskBytes). Zero means no
	// watermark: Sweep never reclaims readable/soft-deleted data on its
	// own and SweepToWatermark is a no-op. When set, SweepToWatermark
	// reclaims orphans first and then soft-deleted versions in a fixed
	// priority (released, retention-expired, ordinary) until usage is at
	// or below the mark; if every eligible byte is reclaimed and usage is
	// still above the mark it returns ErrWatermark without touching
	// protected data.
	MaxPhysicalBytes int64
}
