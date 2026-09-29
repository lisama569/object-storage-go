package objectstore

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// This file implements per-version retention ("legal hold"): a durable,
// restart-surviving policy that forbids physical reclamation of a version's
// bytes until a deadline passes or the retention is explicitly released.
//
// Durability: the policy lives inside the same atomically committed manifest
// as the version state. A process crash or restart therefore never "forgets"
// a hold that was acknowledged: if PutRetention returned successfully the
// protection is already on disk.
//
// State machine of one version's retention:
//
//	(no fields)            --PutRetention-->  active (deadline/permanent)
//	active                 --PutRetention-->  active (no weaker replacement)
//	active                 --ExtendRetention-> active (strictly longer)
//	active                 --ReleaseRetention-> released (ReleasedAt set)
//	active (deadline pass) --time----------->  expired (fields retained)
//	released/expired       --PutRetention-->  active again
//
// Expired and released retentions no longer protect bytes, but their fields
// are kept so reclaim priority and audit history stay explainable.

// nowFn returns the current time. Store.now, if set (tests), overrides the
// wall clock so expiry can be simulated deterministically without sleeping.
func (s *Store) nowFn() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

// encodeDeadline serializes a finite deadline.
func encodeDeadline(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// parseTime decodes an RFC3339Nano timestamp; the zero time is returned for
// an empty string. A parse error also yields the zero time.
func parseTime(v string) time.Time {
	if v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

// validateRetention rejects the zero value and non-UTC is normalized by the
// constructors; here we only ensure a usable policy was supplied.
func validateRetention(r Retention) error {
	if !r.Permanent && r.Until.IsZero() {
		return fmt.Errorf("objectstore: empty retention policy (use RetainUntil or RetainPermanent)")
	}
	return nil
}

// retentionActiveLocked reports whether the version is currently protected
// against reclamation according to s's clock. Callers hold s.mu.
func (s *Store) retentionActiveLocked(mv *metaVersion) bool {
	if mv == nil || mv.RetainUntil == "" || mv.RetainReleasedAt != "" {
		return false
	}
	if mv.RetainUntil == retainPermanentSentinel {
		return true
	}
	deadline, err := time.Parse(time.RFC3339Nano, mv.RetainUntil)
	if err != nil {
		// Unparseable deadlines fail safe: treat as protected rather than
		// silently deleting data that might still be legally held.
		return true
	}
	return s.nowFn().Before(deadline)
}

// statusLocked converts internal retention metadata into the public view.
// Callers hold s.mu.
func (s *Store) statusLocked(name string, mv *metaVersion) RetentionStatus {
	st := RetentionStatus{Object: name, Version: mv.Version}
	st.Retained = s.retentionActiveLocked(mv)
	st.ReleasedAt = parseTime(mv.RetainReleasedAt)
	if mv.RetainUntil == "" {
		return st
	}
	if mv.RetainUntil == retainPermanentSentinel {
		st.Permanent = true
		return st
	}
	st.Until = parseTime(mv.RetainUntil)
	return st
}

// lookupVersionForRetention resolves name/version and rejects targets that
// can never hold a retention: unknown (ErrNotFound) or already collected
// (ErrCollected). Soft-deleted versions may be retained (that is the audit
// use case: keep a deleted version's bytes). Callers hold s.mu.
func (s *Store) lookupVersionForRetention(name string, version int64) (*metaVersion, error) {
	obj := s.manifest.Objects[name]
	if obj == nil {
		return nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return nil, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return nil, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}
	return mv, nil
}

// commitPolicy mutates mv to hold the policy r after validating it. It does
// not commit the manifest. Re-applying a policy clears a prior release.
func applyPolicyLocked(mv *metaVersion, r Retention) {
	if r.Permanent {
		mv.RetainUntil = retainPermanentSentinel
	} else {
		mv.RetainUntil = encodeDeadline(r.Until)
	}
	mv.RetainReleasedAt = ""
}

// PutRetention applies (or renews) a retention on one version.
//
// It is idempotent and safe to repeat: setting the same requirement again
// never weakens an existing hold and produces no harmful side effects.
// Concretely, an existing retention is only replaced by a strictly stronger
// one (a longer deadline, or permanent); re-asserting a shorter/equal
// deadline leaves the existing hold untouched.
func (s *Store) PutRetention(ctx context.Context, name string, version int64, r Retention) (RetentionStatus, error) {
	if err := validateName(name); err != nil {
		return RetentionStatus{}, err
	}
	if err := validateRetention(r); err != nil {
		return RetentionStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionStatus{}, err
	}
	mv, err := s.lookupVersionForRetention(name, version)
	if err != nil {
		return RetentionStatus{}, err
	}

	prevUntil, prevReleased := mv.RetainUntil, mv.RetainReleasedAt
	// Never silently shorten an existing active hold.
	if s.retentionActiveLocked(mv) && !retentionStronger(r, prevUntil, s.nowFn()) {
		return s.statusLocked(name, mv), nil
	}
	applyPolicyLocked(mv, r)
	if err := s.manifest.commit(s.root); err != nil {
		mv.RetainUntil, mv.RetainReleasedAt = prevUntil, prevReleased
		return RetentionStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return s.statusLocked(name, mv), err
	}
	return s.statusLocked(name, mv), nil
}

// retentionStronger reports whether policy r grants a strictly longer
// protection than the currently encoded deadline cur (which is known to be
// active). Permanent is the strongest; finite deadlines compare instants.
func retentionStronger(r Retention, cur string, now time.Time) bool {
	if cur == retainPermanentSentinel {
		return false // nothing is longer than permanent
	}
	if r.Permanent {
		return true
	}
	curDeadline, err := time.Parse(time.RFC3339Nano, cur)
	if err != nil {
		return true
	}
	return r.Until.UTC().After(curDeadline)
}

// ExtendRetention strictly lengthens an existing retention. It is a no-op
// success when the current hold is already permanent or already reaches at
// least the requested deadline. Unlike PutRetention it never (re)arms a hold
// from a released/expired/absent state: use PutRetention for that.
func (s *Store) ExtendRetention(ctx context.Context, name string, version int64, r Retention) (RetentionStatus, error) {
	if err := validateName(name); err != nil {
		return RetentionStatus{}, err
	}
	if err := validateRetention(r); err != nil {
		return RetentionStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionStatus{}, err
	}
	mv, err := s.lookupVersionForRetention(name, version)
	if err != nil {
		return RetentionStatus{}, err
	}
	if !s.retentionActiveLocked(mv) {
		return RetentionStatus{}, fmt.Errorf("%w: %s v%d has no active retention to extend", ErrRetained, name, version)
	}
	prevUntil := mv.RetainUntil
	if !retentionStronger(r, prevUntil, s.nowFn()) {
		// Already at least as long (permanent or later deadline): idempotent.
		return s.statusLocked(name, mv), nil
	}
	if r.Permanent {
		mv.RetainUntil = retainPermanentSentinel
	} else {
		mv.RetainUntil = encodeDeadline(r.Until)
	}
	if err := s.manifest.commit(s.root); err != nil {
		mv.RetainUntil = prevUntil
		return RetentionStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return s.statusLocked(name, mv), err
	}
	return s.statusLocked(name, mv), nil
}

// ReleaseRetention removes an active retention ahead of its deadline. The
// release is itself durable: ReleasedAt is committed so that, after a
// restart, the bytes are reclaimable first (they were deliberately freed).
// Releasing a version that has no active retention is an idempotent no-op.
func (s *Store) ReleaseRetention(ctx context.Context, name string, version int64) (RetentionStatus, error) {
	if err := validateName(name); err != nil {
		return RetentionStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionStatus{}, err
	}
	mv, err := s.lookupVersionForRetention(name, version)
	if err != nil {
		return RetentionStatus{}, err
	}
	if !s.retentionActiveLocked(mv) {
		// Idempotent: expired, already released or never held.
		return s.statusLocked(name, mv), nil
	}
	prevUntil, prevReleased := mv.RetainUntil, mv.RetainReleasedAt
	mv.RetainReleasedAt = encodeDeadline(s.nowFn())
	if err := s.manifest.commit(s.root); err != nil {
		mv.RetainUntil, mv.RetainReleasedAt = prevUntil, prevReleased
		return RetentionStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return s.statusLocked(name, mv), err
	}
	return s.statusLocked(name, mv), nil
}

// RetentionOf reports the retention state of one specific version. The
// version itself must exist (ErrNotFound otherwise); a collected version is
// reported with its last recorded policy and Retained == false.
func (s *Store) RetentionOf(ctx context.Context, name string, version int64) (RetentionStatus, error) {
	if err := validateName(name); err != nil {
		return RetentionStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RetentionStatus{}, err
	}
	obj := s.manifest.Objects[name]
	if obj == nil {
		return RetentionStatus{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return RetentionStatus{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	return s.statusLocked(name, mv), nil
}

// Retentions lists the retention state of every version of one object,
// ordered from oldest to newest version. This answers "is anything under
// this object currently retained, and until when".
func (s *Store) Retentions(ctx context.Context, name string) ([]RetentionStatus, error) {
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
	ids := make([]int64, 0, len(obj.Versions))
	for v := range obj.Versions {
		ids = append(ids, v)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]RetentionStatus, 0, len(ids))
	for _, v := range ids {
		out = append(out, s.statusLocked(name, obj.Versions[v]))
	}
	return out, nil
}
