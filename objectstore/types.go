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
}
