package objectstore

import (
	"context"
	"fmt"
	"sort"
)

// reclaimTier orders eligible soft-deleted data. The lower the number,
// the earlier the group is sacrificed when a watermark needs space.
const (
	// tierReleased: the last effective hold was explicitly removed early
	// via ReleaseRetention. Its protection was deliberately lifted, so it
	// goes before data that merely aged out.
	tierReleased = 0
	// tierExpired: a timed hold ended naturally at its expiry instant;
	// nobody actively released it.
	tierExpired = 1
	// tierNeverHeld: data that never had any retention hold.
	tierNeverHeld = 2
)

// reclaimGroup is one blob digest plus every soft-deleted, unretained
// version that references it. Collecting the group frees exactly one
// blob (dedup: one physical file may back several logical versions).
type reclaimGroup struct {
	digest  string
	size    int64
	tier    int    // reclaim priority class (see reclaimTier constants)
	oldest  string // smallest Created of the members: tie break within a tier
	members []*metaVersion
}

// reclaimTierLocked classifies one currently-unprotected, soft-deleted
// version for watermark reclaim ordering. Callers hold s.mu.
func reclaimTierLocked(mv *metaVersion) int {
	switch {
	case mv.RetainReleasedAt != "":
		return tierReleased
	case mv.RetainUntil != "" || mv.RetainPermanent:
		return tierExpired
	default:
		return tierNeverHeld
	}
}

// enforceWatermarkLocked drives DiskBytes down to Options.WatermarkBytes
// using only data that is safe to sacrifice: orphan blobs are removed
// first (already handled by Sweep phase 2), then soft-deleted versions
// whose retention is absent or expired are collected in deterministic
// priority order:
//
//  1. versions whose hold was explicitly released early, then
//  2. versions whose timed hold expired naturally, then
//  3. versions that never had a hold,
//
// each class ordered oldest-first (digest breaks ties). Active versions
// and retained versions are never reclaimed. If every safe byte is
// reclaimed and usage is still above the mark, ErrWatermarkExceeded is
// returned; metadata and bytes stay self-consistent at every intermediate
// step. Callers hold s.mu.
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
			tier := reclaimTierLocked(mv)
			g := groups[mv.Digest]
			if g == nil {
				g = &reclaimGroup{digest: mv.Digest, size: blobSize[mv.Digest], tier: tier, oldest: mv.Created}
				if g.size == 0 {
					g.size = mv.Size
				}
				groups[mv.Digest] = g
			}
			// A shared group is sacrificed as early as its most
			// deliberately-releasable member.
			if tier < g.tier {
				g.tier = tier
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
	// Deterministic priority: explicitly released first, then naturally
	// expired, then never-held; within a class the oldest soft-deleted
	// data goes first and digest breaks ties, so repeated runs pick
	// exactly the same victims.
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].tier != ordered[j].tier {
			return ordered[i].tier < ordered[j].tier
		}
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
		// Snapshot the fields we overwrite so a failed commit restores the
		// in-memory manifest to exactly the durable state.
		type prev struct {
			mv                                     *metaVersion
			state                                  VersionState
			purgedAt, permUntil, until, releasedAt string
			permanent                              bool
		}
		snaps := make([]prev, 0, len(g.members))
		for _, mv := range g.members {
			snaps = append(snaps, prev{
				mv:         mv,
				state:      mv.State,
				purgedAt:   mv.PurgedAt,
				permanent:  mv.RetainPermanent,
				permUntil:  mv.RetainUntil,
				releasedAt: mv.RetainReleasedAt,
			})
			mv.State = StateCollected
			if mv.PurgedAt == "" {
				mv.PurgedAt = nowUTC()
			}
			mv.RetainPermanent = false
			mv.RetainUntil = ""
			mv.RetainReleasedAt = ""
		}
		if err := s.manifest.commit(s.root); err != nil {
			// Commit failed: durable state is unchanged, so restore the
			// in-memory snapshot to match and abort with nothing reclaimed.
			for _, sn := range snaps {
				sn.mv.State = sn.state
				sn.mv.PurgedAt = sn.purgedAt
				sn.mv.RetainPermanent = sn.permanent
				sn.mv.RetainUntil = sn.permUntil
				sn.mv.RetainReleasedAt = sn.releasedAt
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
