package objectstore

import (
	"context"
	"fmt"
	"sort"
)

// reclaimGroup is one blob digest plus every soft-deleted, unretained
// version that references it. Collecting the group frees exactly one
// blob (dedup: one physical file may back several logical versions).
type reclaimGroup struct {
	digest  string
	size    int64
	oldest  string // smallest Created of the members: reclaim priority key
	members []*metaVersion
}

// enforceWatermarkLocked drives DiskBytes down to Options.WatermarkBytes
// using only data that is safe to sacrifice: orphan blobs are removed
// first (already handled by Sweep phase 2), then soft-deleted versions
// whose retention is absent or expired are collected in deterministic
// priority order (oldest first). Active versions and retained versions
// are never reclaimed. If every safe byte is reclaimed and usage is
// still above the mark, ErrWatermarkExceeded is returned; metadata and
// bytes stay self-consistent at every intermediate step. Callers hold
// s.mu.
func (s *Store) enforceWatermarkLocked(ctx context.Context) (Stats, error) {
	mark := s.opts.WatermarkBytes

	// Snapshot on-disk blob sizes.
	blobSize := map[string]int64{}
	if err := s.blobs.walkBlobs(func(digest string, size int64) error {
		blobSize[digest] = size
		return nil
	}); err != nil {
		return Stats{}, err
	}

	var diskBytes int64
	for _, sz := range blobSize {
		diskBytes += sz
	}
	if diskBytes <= mark {
		st := s.computeStatsLocked()
		return st, nil
	}
	needFree := diskBytes - mark
	now := s.now()

	// Build reclaim groups deterministically. Eligibility per version:
	//   - state == Deleted (active/current data is never sacrificed);
	//   - no currently-effective hold (permanent or unexpired timed).
	// A group is only collectible when the digest is referenced by no
	// active and no retained version at all; such digests are exactly
	// rs.onlyDeleted minus rs.held.
	rs := s.buildRefs()
	groups := map[string]*reclaimGroup{}
	names := make([]string, 0, len(s.manifest.Objects))
	for name := range s.manifest.Objects {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		obj := s.manifest.Objects[name]
		ids := make([]int64, 0, len(obj.Versions))
		for v := range obj.Versions {
			ids = append(ids, v)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, v := range ids {
			mv := obj.Versions[v]
			if mv.State != StateDeleted {
				continue
			}
			if active, _, _ := holdStateLocked(mv, now); active {
				continue
			}
			if !rs.onlyDeleted[mv.Digest] || rs.held[mv.Digest] {
				continue
			}
			g := groups[mv.Digest]
			if g == nil {
				g = &reclaimGroup{digest: mv.Digest, size: blobSize[mv.Digest], oldest: mv.Created}
				if g.size == 0 {
					g.size = mv.Size
				}
				groups[mv.Digest] = g
			}
			if mv.Created < g.oldest || g.oldest == "" {
				g.oldest = mv.Created
			}
			g.members = append(g.members, mv)
		}
	}

	ordered := make([]*reclaimGroup, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	// Deterministic priority: oldest soft-deleted data first; digest breaks
	// ties so repeated runs pick exactly the same victims.
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].oldest != ordered[j].oldest {
			return ordered[i].oldest < ordered[j].oldest
		}
		return ordered[i].digest < ordered[j].digest
	})

	// Collect one group at a time, committing metadata before deleting the
	// blob. A crash in between leaves only an orphan that the next Sweep
	// removes; it never leaves a dangling reference.
	for _, g := range ordered {
		if needFree <= 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return s.computeStatsLocked(), err
		}
		for _, mv := range g.members {
			mv.State = StateCollected
			if mv.PurgedAt == "" {
				mv.PurgedAt = nowUTC()
			}
			mv.RetainPermanent = false
			mv.RetainUntil = ""
		}
		if err := s.manifest.commit(s.root); err != nil {
			// Commit failed: durable state is unchanged, so restore the
			// in-memory snapshot to match and abort with nothing reclaimed.
			for _, mv := range g.members {
				mv.State = StateDeleted
				mv.PurgedAt = ""
				mv.RetainPermanent = false
				mv.RetainUntil = ""
			}
			return Stats{}, err
		}
		if err := s.blobs.remove(g.digest); err != nil {
			return s.computeStatsLocked(), err
		}
		needFree -= g.size
	}

	st := s.computeStatsLocked()
	if st.DiskBytes > mark {
		// Everything safely reclaimable is gone and the level is still too
		// high. Say so explicitly instead of touching protected data.
		return st, fmt.Errorf("%w: disk=%d watermark=%d", ErrWatermarkExceeded, st.DiskBytes, mark)
	}
	return st, nil
}
