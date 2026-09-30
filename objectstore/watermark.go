package objectstore

import (
	"context"
	"fmt"
	"sort"
)

// reclaim tier orders watermark sacrifices deterministically. Lower
// numbers are reclaimed first. Only soft-deleted versions without an
// active hold ever belong to a tier:
//
//	tierReleased (1): the version had an active hold that was lifted
//	                  explicitly via ReleaseRetention ("already released");
//	tierExpired  (2): the version had a timed hold that lapsed on its own
//	                  ("naturally expired");
//	tierNeverHeld(3): the version was soft-deleted without ever being held.
//
// Within one tier the oldest Created wins and digest is the final
// tiebreak, so the chosen victims are identical on every run.
const (
	tierReleased  = 1
	tierExpired   = 2
	tierNeverHeld = 3
)

// reclaimTierLocked classifies a soft-deleted, currently-unheld version
// into the deterministic reclaim tier above. Callers hold s.mu.
func reclaimTierLocked(mv *metaVersion) int {
	switch {
	case mv.RetainReleased:
		return tierReleased
	case mv.RetainUntil != "":
		// A stored expiry with no active hold is a naturally expired timed
		// hold (an unparseable record would fail-closed as active and never
		// reach here).
		return tierExpired
	default:
		return tierNeverHeld
	}
}

// reclaimGroup is one blob digest plus every soft-deleted, unretained
// version that references it. Collecting the group frees exactly one
// blob (dedup: one physical file may back several logical versions). Its
// tier is the weakest (highest-numbered) tier of any member: a group is
// sacrificed only when every version sharing those bytes is at least as
// expendable, so a digest shared with e.g. an expired version cannot be
// deleted early on the strength of a released co-referer alone.
type reclaimGroup struct {
	digest  string
	size    int64
	tier    int
	oldest  string // smallest Created of the members: in-tier priority key
	members []*metaVersion
}

// enforceWatermarkLocked drives DiskBytes down to Options.WatermarkBytes
// using only data that is safe to sacrifice. Orphan blobs are always
// removed first (Sweep phase 2). Soft-deleted versions without an active
// hold are then collected in a fixed, reproducible priority:
//
//  1. versions whose active hold was explicitly lifted early
//     (ReleaseRetention);
//  2. versions whose timed hold expired naturally;
//  3. versions that never had a hold.
//
// Within one tier the oldest Created wins and digest is the final
// tiebreak; reclaiming stops the instant usage is back at the mark.
// Active versions and versions under an effective hold are never
// reclaimed, and a blob shared across tiers follows the weakest member.
// If every safe byte is reclaimed and usage is still above the mark,
// ErrWatermarkExceeded is returned; metadata and bytes stay
// self-consistent at every intermediate step. Callers hold s.mu.
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
				g = &reclaimGroup{
					digest: mv.Digest,
					size:   blobSize[mv.Digest],
					tier:   reclaimTierLocked(mv),
					oldest: mv.Created,
				}
				if g.size == 0 {
					g.size = mv.Size
				}
				groups[mv.Digest] = g
			}
			// The group inherits the weakest (highest-numbered) tier among
			// its members: never collect shared bytes while any sharing
			// version belongs to a later tier.
			if t := reclaimTierLocked(mv); t > g.tier {
				g.tier = t
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
	// Deterministic priority: released-before-expired-before-never-held;
	// within one tier the oldest soft-deleted data first; digest breaks any
	// remaining tie so repeated runs pick exactly the same victims.
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
		type memberSnap struct {
			mv             *metaVersion
			state          VersionState
			purgedAt       string
			perm, released bool
			until          string
		}
		snaps := make([]memberSnap, 0, len(g.members))
		for _, mv := range g.members {
			snaps = append(snaps, memberSnap{
				mv: mv, state: mv.State, purgedAt: mv.PurgedAt,
				perm: mv.RetainPermanent, until: mv.RetainUntil, released: mv.RetainReleased,
			})
			mv.State = StateCollected
			if mv.PurgedAt == "" {
				mv.PurgedAt = nowUTC()
			}
			// Collected data has no bytes and therefore no hold history to
			// keep; durable state stays self-consistent.
			mv.RetainPermanent = false
			mv.RetainUntil = ""
			mv.RetainReleased = false
		}
		if err := s.manifest.commit(s.root); err != nil {
			// Commit failed: durable state is unchanged, so restore the
			// in-memory snapshot to match and abort with nothing reclaimed.
			for _, sn := range snaps {
				sn.mv.State = sn.state
				sn.mv.PurgedAt = sn.purgedAt
				sn.mv.RetainPermanent = sn.perm
				sn.mv.RetainUntil = sn.until
				sn.mv.RetainReleased = sn.released
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
