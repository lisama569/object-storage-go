package objectstore

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Retain places or extends a retention hold on a specific version.
//
// A zero until (time.Time{}) requests a permanent hold; otherwise until is
// the expiry instant. Retain only ever strengthens a hold: a permanent
// hold stays permanent, and a timed hold is extended (never shortened) to
// the later of its current expiry and until. Repeating an identical or
// weaker request is a side-effect-free no-op. The hold protects data only
// once the atomic manifest commit has completed, so it survives process
// restarts and is never observed half-applied.
func (s *Store) Retain(ctx context.Context, name string, version int64, until time.Time) (RetentionInfo, error) {
	if err := validateName(name); err != nil {
		return RetentionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionInfo{}, err
	}
	obj := s.manifest.Objects[name]
	if obj == nil {
		return RetentionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}

	now := s.now()
	requestPermanent := until.IsZero()
	if !requestPermanent {
		u := until.UTC()
		if !u.After(now) {
			return RetentionInfo{}, fmt.Errorf("objectstore: retention expiry %s must be in the future", u.Format(time.RFC3339Nano))
		}
		until = u
	}

	active, permanent, oldUntil := holdStateLocked(mv, now)
	switch {
	case requestPermanent && permanent:
		// Identical permanent hold repeated: no-op, no commit.
	case !requestPermanent && permanent:
		// A weaker timed request can never shorten a permanent hold.
	case !requestPermanent && active && !oldUntil.Before(until):
		// An active timed (or fail-closed) hold whose expiry already equals
		// or outlives the request: identical repeat or weaker request,
		// nothing to write. An inactive record (expired/absent) never lands
		// here, so a valid future request over one places a fresh hold.
	case requestPermanent:
		// Strengthen an existing timed/absent/expired hold to permanent.
		prevPerm, prevUntil, prevReleased := mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased
		mv.RetainPermanent = true
		mv.RetainUntil = ""
		mv.RetainReleased = false
		if err := s.manifest.commit(s.root); err != nil {
			mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased = prevPerm, prevUntil, prevReleased
			return RetentionInfo{}, err
		}
	default:
		// A timed request that genuinely extends the current hold, or that
		// places the first hold (including over an expired record).
		prevPerm, prevUntil, prevReleased := mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased
		mv.RetainPermanent = false
		mv.RetainUntil = until.Format(time.RFC3339Nano)
		mv.RetainReleased = false
		if err := s.manifest.commit(s.root); err != nil {
			mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased = prevPerm, prevUntil, prevReleased
			return RetentionInfo{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return retentionInfo(name, mv, now), err
	}
	return retentionInfo(name, mv, now), nil
}

// ReleaseRetention explicitly removes an *active* retention hold, timed
// or permanent, including one that has not expired. Releasing a version
// that has no active hold (never held, or a timed hold that already
// expired) is a side-effect-free no-op that leaves the natural-expiry
// evidence untouched; only an explicit early release is marked as such
// (retain_released) so watermark reclaim can order released data ahead
// of naturally expired data. The release is durable only after the
// manifest commit, so a crash before that point leaves the protection
// fully in place.
func (s *Store) ReleaseRetention(ctx context.Context, name string, version int64) (RetentionInfo, error) {
	if err := validateName(name); err != nil {
		return RetentionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionInfo{}, err
	}
	obj := s.manifest.Objects[name]
	if obj == nil {
		return RetentionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}

	now := s.now()
	// Only an *active* hold counts as an explicit early release. Releasing a
	// hold that already expired (or that never existed) is a side-effect-free
	// no-op that preserves the natural-expiry evidence used by watermark
	// reclaim ordering.
	if active, _, _ := holdStateLocked(mv, now); active {
		prevPerm, prevUntil, prevReleased := mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased
		mv.RetainPermanent = false
		mv.RetainUntil = ""
		mv.RetainReleased = true
		if err := s.manifest.commit(s.root); err != nil {
			mv.RetainPermanent, mv.RetainUntil, mv.RetainReleased = prevPerm, prevUntil, prevReleased
			return RetentionInfo{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return retentionInfo(name, mv, now), err
	}
	return retentionInfo(name, mv, now), nil
}

// Retention reports the hold state of one version. Classification is as
// precise as for reads: unknown object/version -> ErrNotFound; a version
// whose data was collected -> ErrCollected.
func (s *Store) Retention(ctx context.Context, name string, version int64) (RetentionInfo, error) {
	if err := validateName(name); err != nil {
		return RetentionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionInfo{}, err
	}
	obj := s.manifest.Objects[name]
	if obj == nil {
		return RetentionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return RetentionInfo{}, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}
	return retentionInfo(name, mv, s.now()), nil
}

// ListRetained reports every currently-effective hold for an object,
// ordered by version number. An unknown object name is ErrNotFound (never
// confused with "no holds"); an object whose holds all expired returns an
// empty slice.
func (s *Store) ListRetained(ctx context.Context, name string) ([]RetentionInfo, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	obj := s.manifest.Objects[name]
	if obj == nil {
		return nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	now := s.now()
	ids := make([]int64, 0, len(obj.Versions))
	for v, mv := range obj.Versions {
		active, _, _ := holdStateLocked(mv, now)
		if active {
			ids = append(ids, v)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]RetentionInfo, 0, len(ids))
	for _, v := range ids {
		out = append(out, retentionInfo(name, obj.Versions[v], now))
	}
	return out, nil
}

// retentionInfo builds the public descriptor for one version. Callers
// hold s.mu.
func retentionInfo(name string, mv *metaVersion, now time.Time) RetentionInfo {
	active, permanent, until := holdStateLocked(mv, now)
	info := RetentionInfo{Object: name, Version: mv.Version, Retained: active, Permanent: permanent}
	if active && !permanent {
		info.Until = until
	}
	return info
}

// holdStateLocked classifies a version's hold against the given time.
// Returns:
//
//	active    : the version must not be reclaimed/soft-deleted right now;
//	permanent : true when active and the hold never expires;
//	until     : expiry instant for a timed hold (zero otherwise).
//
// Fail-closed: an unparseable stored expiry is treated as an active timed
// hold rather than letting a corrupt record silently lift protection.
func holdStateLocked(mv *metaVersion, now time.Time) (active, permanent bool, until time.Time) {
	if mv.RetainPermanent {
		return true, true, time.Time{}
	}
	if mv.RetainUntil == "" {
		return false, false, time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, mv.RetainUntil)
	if err != nil {
		// Protection must never disappear due to an unreadable record.
		return true, false, now.Add(time.Second)
	}
	if t.After(now) {
		return true, false, t
	}
	return false, false, t
}
