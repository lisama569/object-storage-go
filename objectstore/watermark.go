package objectstore

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// This file implements capacity watermark-driven reclamation with a fixed,
// deterministic eviction order. Protected data can never be evicted:
//
//   - every Active (readable) version;
//   - every version holding an active retention (deadline not reached or
//     permanent), including soft-deleted ones.
//
// When usage must drop, candidates are reclaimed in exactly this order:
//
//  0. unreferenced blob files (orphans: crashed-write leftovers, blobs
//     released by PurgeVersion, blobs whose metadata commit survived a
//     previous interrupted GC while the file deletion did not);
//  1. soft-deleted versions whose retention was explicitly RELEASED;
//  2. soft-deleted versions whose retention has EXPIRED;
//  3. ordinary soft-deleted versions that never had a retention.
//
// Within one class, the oldest versions (by creation time) go first; the
// digest string is the final tie-break, so the outcome is deterministic and
// independent of map iteration order.
//
// A digest is considered only when it has no active-version reference and no
// retained reference at all. All soft-deleted versions sharing that digest
// transition to Collected together (they share one physical file).
//
// Crash safety is the same as PurgeVersion/Sweep: for each digest, metadata
// is committed (versions -> Collected) BEFORE the blob file is unlinked. A
// crash in between merely leaves an orphan blob that the next pass removes;
// a dangling metadata reference is impossible. Repeating the operation
// converges: already-Collected versions and missing blobs are skipped.

// evictionClass assigns a reclaim priority to one soft-deleted version.
// Smaller is reclaimed first. Protected versions return classProtected.
const classProtected = -1

func (s *Store) evictionClass(mv *metaVersion) int {
	if s.retentionActiveLocked(mv) {
		return classProtected
	}
	switch {
	case mv.RetainReleasedAt != "":
		return 1 // deliberately released: cheapest to reclaim
	case mv.RetainUntil != "":
		return 2 // retention expired on its own
	default:
		return 3 // ordinary soft delete, never held
	}
}

// deletedRef is one soft-deleted version participating in an eviction plan.
type deletedRef struct {
	name string
	mv   *metaVersion
}

// evictionCandidate groups all soft-deleted versions sharing one digest.
type evictionCandidate struct {
	digest string
	size   int64
	class  int
	oldest time.Time
	refs   []deletedRef
}

// planEvictionLocked builds the deterministic ordered list of reclaimable
// digests. It excludes every digest referenced by an Active version or by a
// retained version. Callers hold s.mu.
func (s *Store) planEvictionLocked(blobSize map[string]int64) []evictionCandidate {
	activeRef := map[string]bool{}
	group := map[string][]deletedRef{}
	for name, obj := range s.manifest.Objects {
		for _, mv := range obj.Versions {
			switch mv.State {
			case StateActive:
				activeRef[mv.Digest] = true
			case StateDeleted:
				group[mv.Digest] = append(group[mv.Digest], deletedRef{name: name, mv: mv})
			}
		}
	}

	var plan []evictionCandidate
	for digest, refs := range group {
		if activeRef[digest] {
			continue // live data sharing this blob keeps it safe
		}
		cand := evictionCandidate{digest: digest, size: blobSize[digest], class: 1 << 30}
		protected := false
		for _, r := range refs {
			c := s.evictionClass(r.mv)
			if c == classProtected {
				protected = true
				break
			}
			if c < cand.class {
				cand.class = c
			}
			if created := parseTime(r.mv.Created); !created.IsZero() {
				if cand.oldest.IsZero() || created.Before(cand.oldest) {
					cand.oldest = created
				}
			}
		}
		if protected {
			continue
		}
		cand.refs = refs
		plan = append(plan, cand)
	}
	sort.Slice(plan, func(i, j int) bool {
		if plan[i].class != plan[j].class {
			return plan[i].class < plan[j].class
		}
		if !plan[i].oldest.Equal(plan[j].oldest) {
			return plan[i].oldest.Before(plan[j].oldest)
		}
		return plan[i].digest < plan[j].digest
	})
	return plan
}

// onDiskSnapshotLocked returns size of every committed blob and total usage.
func (s *Store) onDiskSnapshotLocked() (map[string]int64, int64) {
	blobSize := map[string]int64{}
	var total int64
	_ = s.blobs.walkBlobs(func(digest string, size int64) error {
		blobSize[digest] = size
		total += size
		return nil
	})
	return blobSize, total
}

// removeOrphansLocked deletes every blob not referenced by a non-Collected
// version and returns how many files / bytes were removed. It is the same
// safe operation Sweep performs; retained/live digests are always in refs.
func (s *Store) removeOrphansLocked(ctx context.Context, blobSize map[string]int64) (int, int64, error) {
	refs := s.buildRefs()
	removed := 0
	var bytesFreed int64
	// Stable order for reproducible runs.
	digests := make([]string, 0, len(blobSize))
	for d := range blobSize {
		digests = append(digests, d)
	}
	sort.Strings(digests)
	for _, digest := range digests {
		if refs.referenced[digest] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return removed, bytesFreed, err
		}
		size := blobSize[digest]
		if s.blobs.crashBeforeBlobDelete != nil {
			if err := s.blobs.crashBeforeBlobDelete(digest); err != nil {
				return removed, bytesFreed, err
			}
		}
		if err := s.blobs.remove(digest); err != nil {
			return removed, bytesFreed, err
		}
		delete(blobSize, digest)
		bytesFreed += size
		removed++
	}
	return removed, bytesFreed, nil
}

// sweepToWatermarkLocked performs capacity-driven reclamation. It first
// removes all orphans (free space at zero cost), then walks the deterministic
// eviction plan until usage is at or below the watermark. It never touches
// protected data. The returned error is non-nil (ErrWatermark) only when the
// mark cannot be reached after exhausting every candidate.
//
// Callers hold s.mu.
func (s *Store) sweepToWatermarkLocked(ctx context.Context) (WatermarkReport, error) {
	rep := WatermarkReport{Watermark: s.opts.MaxPhysicalBytes}
	blobSize, onDisk := s.onDiskSnapshotLocked()
	rep.OnDiskBefore = onDisk

	if rep.Watermark <= 0 {
		// No capacity configured: nothing to drive eviction.
		rep.OnDiskAfter = onDisk
		rep.BelowWatermark = true
		return rep, nil
	}

	// Phase 0: always reclaim true orphans first.
	n, freed, err := s.removeOrphansLocked(ctx, blobSize)
	if err != nil {
		rep.OnDiskAfter = onDisk - freed
		rep.OrphanBlobsRemoved = n
		rep.BytesReclaimed = freed
		return rep, err
	}
	rep.OrphanBlobsRemoved = n
	onDisk -= freed

	// Phases 1-3: soft-deleted versions in fixed priority order.
	if onDisk > rep.Watermark {
		plan := s.planEvictionLocked(blobSize)
		for _, cand := range plan {
			if onDisk <= rep.Watermark {
				break
			}
			if err := ctx.Err(); err != nil {
				rep.OnDiskAfter = onDisk
				rep.BytesReclaimed = rep.OnDiskBefore - onDisk
				return rep, err
			}
			// Commit metadata first: every version sharing this digest
			// becomes Collected atomically (one manifest revision).
			type rollback struct {
				mv        *deletedRef
				prevState VersionState
				prevPurg  string
			}
			var undo []rollback
			for i := range cand.refs {
				r := &cand.refs[i]
				undo = append(undo, rollback{
					mv:        r,
					prevState: r.mv.State,
					prevPurg:  r.mv.PurgedAt,
				})
				r.mv.State = StateCollected
				if r.mv.PurgedAt == "" {
					r.mv.PurgedAt = nowUTC()
				}
			}
			if err := s.manifest.commit(s.root); err != nil {
				for _, u := range undo {
					u.mv.mv.State = u.prevState
					u.mv.mv.PurgedAt = u.prevPurg
				}
				rep.OnDiskAfter = onDisk
				rep.BytesReclaimed = rep.OnDiskBefore - onDisk
				return rep, err
			}
			if s.blobs.crashAfterSweepCommit != nil {
				if err := s.blobs.crashAfterSweepCommit(); err != nil {
					rep.OnDiskAfter = onDisk
					rep.VersionsCollected += len(cand.refs)
					rep.BytesReclaimed = rep.OnDiskBefore - onDisk
					return rep, err
				}
			}
			// Metadata is durable; now unlink the bytes.
			if s.blobs.crashBeforeBlobDelete != nil {
				if err := s.blobs.crashBeforeBlobDelete(cand.digest); err != nil {
					rep.OnDiskAfter = onDisk
					rep.VersionsCollected += len(cand.refs)
					rep.BytesReclaimed = rep.OnDiskBefore - onDisk
					return rep, err
				}
			}
			if err := s.blobs.remove(cand.digest); err != nil {
				rep.OnDiskAfter = onDisk
				rep.VersionsCollected += len(cand.refs)
				rep.BytesReclaimed = rep.OnDiskBefore - onDisk
				return rep, err
			}
			delete(blobSize, cand.digest)
			rep.VersionsCollected += len(cand.refs)
			onDisk -= cand.size
			freed += cand.size
		}
	}

	rep.BytesReclaimed = freed
	rep.OnDiskAfter = onDisk
	rep.BelowWatermark = onDisk <= rep.Watermark
	if !rep.BelowWatermark {
		return rep, fmt.Errorf("%w: on_disk=%d watermark=%d (reclaimed=%d bytes, %d versions)",
			ErrWatermark, onDisk, rep.Watermark, rep.BytesReclaimed, rep.VersionsCollected)
	}
	return rep, nil
}

// SweepToWatermark reclaims space until physical usage is at or below the
// configured Options.MaxPhysicalBytes. It always starts by removing
// unreferenced orphan blobs, then reclaims soft-deleted versions in the
// deterministic priority order (released, retention-expired, ordinary).
//
// Active versions and versions under active retention are never reclaimed.
// If every eligible version has been reclaimed and usage is still above the
// mark, ErrWatermark is returned together with the final report; the store
// is left consistent and no protected data was removed. With no watermark
// configured (MaxPhysicalBytes == 0) it is a no-op.
//
// The operation is idempotent and crash-resumable: rerunning it after an
// interruption (or a process restart) simply continues from durable state.
func (s *Store) SweepToWatermark(ctx context.Context) (WatermarkReport, Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return WatermarkReport{}, Stats{}, err
	}
	rep, err := s.sweepToWatermarkLocked(ctx)
	return rep, s.computeStatsLocked(), err
}
