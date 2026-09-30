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
	// Retained reports whether the version is currently under an active
	// retention hold (timed and unexpired, or permanent) at query time.
	Retained bool `json:"retained"`
	// PermanentHold is true when Retained is true and the hold never expires.
	PermanentHold bool `json:"permanent_hold,omitempty"`
	// RetainUntil is the expiry instant of a timed hold. It is zero for no
	// hold, an expired hold, or a permanent hold.
	RetainUntil time.Time `json:"retain_until,omitempty"`
}

// RetentionInfo describes the retention (legal-hold) state of one version.
//
// A hold is either permanent (Permanent true, Until zero) or timed (Until
// is the expiry instant). Retained is true only when the hold is currently
// in effect: Permanent, or Until strictly later than "now". An expired
// timed hold reports Retained == false.
type RetentionInfo struct {
	Object    string    `json:"object"`
	Version   int64     `json:"version"`
	Retained  bool      `json:"retained"`
	Permanent bool      `json:"permanent,omitempty"`
	Until     time.Time `json:"until,omitempty"`
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
	// DiskBytes is the total size of every committed blob file on disk,
	// including unreferenced blobs. This is the figure compared against a
	// configured watermark.
	DiskBytes int64 `json:"disk_bytes"`
	// RetainedVersions counts versions currently protected by an active
	// (timed-unexpired or permanent) retention hold.
	RetainedVersions int `json:"retained_versions"`
	// WatermarkBytes is the configured high-water mark; 0 disables it.
	WatermarkBytes int64 `json:"watermark_bytes,omitempty"`
	// OverWatermark is true when DiskBytes exceeds WatermarkBytes after the
	// operation finished (i.e. not enough reclaimable data existed).
	OverWatermark bool `json:"over_watermark,omitempty"`
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
	SweepDeletedVersions bool
	// WatermarkBytes, when > 0, sets a high-water mark on the total size
	// of committed blob files. Sweep then reclaims in deterministic
	// priority order (orphan blobs first, then soft-deleted data whose
	// hold was explicitly released early, then data whose hold expired
	// naturally, then data that never had a hold; oldest first within
	// each class) until DiskBytes is at or below the mark. Active
	// versions and retained versions are never reclaimed. If everything
	// safely reclaimable has been reclaimed and usage is still above the
	// mark, Sweep returns ErrWatermarkExceeded. Zero (the default)
	// disables watermark enforcement entirely.
	WatermarkBytes int64
}
