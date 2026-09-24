package objectstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Store is a local, content-addressed, versioned object store.
//
// A single Store instance is safe for concurrent use. At most one Store
// may be open on a directory at a time across processes; in-process
// concurrency is serialized by an internal mutex.
type Store struct {
	mu       sync.Mutex
	root     string
	opts     Options
	blobs    *blobStore
	manifest *manifest
	lock     *fileLock
}

const maxNameLen = 1024

// validateName rejects empty names and names that cannot be represented
// safely as a metadata key. Names are otherwise opaque byte strings; the
// store never interprets "/" or "." in them.
func validateName(name string) error {
	if name == "" || len(name) > maxNameLen || strings.ContainsRune(name, 0) {
		return fmt.Errorf("%w: %q", ErrName, name)
	}
	return nil
}

// Open opens (or creates) a store rooted at dir.
//
// It takes an exclusive process lock, loads the (atomically committed)
// manifest, ensures the directory layout exists, and performs an
// automatic recovery of staging files left by crashed writers. It never
// deletes committed blob data, so an interrupted process can simply
// reopen the same directory.
func Open(ctx context.Context, dir string, opts Options) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dir == "" {
		return nil, fmt.Errorf("objectstore: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	lock, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			lock.release()
		}
	}()

	blobs := newBlobStore(dir)
	if err := blobs.ensureDirs(); err != nil {
		return nil, err
	}
	m, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		root:     dir,
		opts:     opts,
		blobs:    blobs,
		manifest: m,
		lock:     lock,
	}
	// Reap any staging files left by a crash. This cannot touch data
	// reachable from the manifest.
	if _, err := s.recoverLocked(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return s, nil
}

// Close releases the store lock, making the directory available to
// another process.
func (s *Store) Close() error {
	s.mu.Lock()
	l := s.lock
	s.lock = nil
	s.mu.Unlock()
	if l == nil {
		return nil
	}
	return l.release()
}

// nowUTC returns the current wall time as RFC3339Nano UTC.
func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// toInfo converts internal metadata into the public VersionInfo.
func toInfo(name string, mv *metaVersion) VersionInfo {
	vi := VersionInfo{
		Object:  name,
		Version: mv.Version,
		Size:    mv.Size,
		Digest:  mv.Digest,
		State:   mv.State,
	}
	if mv.Created != "" {
		if t, err := time.Parse(time.RFC3339Nano, mv.Created); err == nil {
			vi.Created = t
		}
	}
	if mv.PurgedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, mv.PurgedAt); err == nil {
			vi.PurgedAt = t
		}
	}
	vi.Deleted = mv.State == StateDeleted
	return vi
}

// lookupVersion returns the object and version metadata, classifying
// every negative outcome distinctly:
//
//	unknown name / unknown version -> ErrNotFound
//	StateCollected                 -> ErrCollected
//	StateDeleted                   -> ErrDeleted
//
// Callers must hold s.mu.
func (s *Store) lookupVersion(name string, version int64) (*objectMeta, *metaVersion, error) {
	obj := s.manifest.Objects[name]
	if obj == nil {
		return nil, nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return obj, nil, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	switch mv.State {
	case StateCollected:
		return obj, mv, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	case StateDeleted:
		return obj, mv, fmt.Errorf("%w: %s v%d", ErrDeleted, name, version)
	}
	return obj, mv, nil
}

// Put writes data as a new version of name.
//
// Content addressing: the identity of the bytes is their SHA-256 digest
// (prefix "sha256:"), independent of write order and read chunk size.
// If the current live version already has exactly these bytes, no new
// version is allocated and no data is copied. Otherwise a fresh
// monotonically increasing version number is assigned.
//
// Crash safety: bytes are first staged and fsynced at their digest path;
// only afterwards is the manifest atomically committed. A crash at any
// point therefore leaves either the old state or a complete new state,
// never a reference to missing bytes.
func (s *Store) Put(ctx context.Context, name string, r io.Reader) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return VersionInfo{}, err
	}

	digest, size, err := s.blobs.put(r, "")
	if err != nil {
		return VersionInfo{}, err
	}
	// Test hook: simulate a crash after bytes are durable but before the
	// manifest commit. The error propagates as a failed Put; the blob is
	// intentionally left as an orphan exactly like a real crash.
	if s.blobs.crashAfterInstall != nil {
		if err := s.blobs.crashAfterInstall(digest); err != nil {
			return VersionInfo{}, err
		}
	}

	obj := s.manifest.Objects[name]
	if obj == nil {
		obj = &objectMeta{Current: 0, Next: 1, Versions: map[int64]*metaVersion{}}
		s.manifest.Objects[name] = obj
	}
	// Exact-content dedup against the current live version.
	if cur := obj.Versions[obj.Current]; cur != nil && cur.State == StateActive &&
		cur.Digest == digest {
		if err := ctx.Err(); err != nil {
			return VersionInfo{}, err
		}
		return toInfo(name, cur), nil
	}

	version := obj.Next
	obj.Next++
	mv := &metaVersion{
		Version: version,
		Size:    size,
		Digest:  digest,
		Created: nowUTC(),
		State:   StateActive,
	}
	prevCurrent := obj.Current
	obj.Versions[version] = mv
	obj.Current = version
	if err := s.manifest.commit(s.root); err != nil {
		// Manifest not committed: roll back the in-memory change so the
		// live instance matches durable state. The blob is an orphan that
		// a later Sweep safely removes.
		delete(obj.Versions, version)
		obj.Next--
		obj.Current = prevCurrent
		if len(obj.Versions) == 0 {
			delete(s.manifest.Objects, name)
		}
		return VersionInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return toInfo(name, mv), err
	}
	return toInfo(name, mv), nil
}

// openVerified opens the blob for a resolved version and, unless read
// verification is disabled, wraps it so the full content is re-hashed
// before EOF is returned to the caller.
func (s *Store) openVerified(mv *metaVersion) (io.ReadCloser, error) {
	f, err := s.blobs.open(mv.Digest)
	if err != nil {
		return nil, err
	}
	if s.opts.DisableReadVerification {
		return f, nil
	}
	return &verifiedReader{
		f:       f,
		h:       sha256.New(),
		want:    mv.Digest,
		wantLen: mv.Size,
		got:     0,
	}, nil
}

// Get reads the current live version of name. It returns ErrNotFound,
// ErrDeleted or ErrCollected to distinguish why the data is unavailable.
func (s *Store) Get(ctx context.Context, name string) (io.ReadCloser, VersionInfo, error) {
	if err := validateName(name); err != nil {
		return nil, VersionInfo{}, err
	}
	s.mu.Lock()
	_, mv, err := s.lookupCurrentLocked(name)
	info := VersionInfo{}
	if mv != nil {
		info = toInfo(name, mv)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, VersionInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, info, err
	}
	rc, err := s.openVerified(mv)
	return rc, info, err
}

// lookupCurrentLocked resolves the current version with explicit error
// classification. obj == nil means the object name is unknown. Callers
// hold s.mu.
func (s *Store) lookupCurrentLocked(name string) (*objectMeta, *metaVersion, error) {
	obj := s.manifest.Objects[name]
	if obj == nil {
		return nil, nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	if obj.Current == 0 {
		// Object-level tombstone: explain using the newest version's state.
		var newest int64
		for v := range obj.Versions {
			if v > newest {
				newest = v
			}
		}
		mv := obj.Versions[newest]
		switch {
		case mv == nil:
			return obj, nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
		case mv.State == StateCollected:
			return obj, mv, fmt.Errorf("%w: %s", ErrCollected, name)
		default:
			return obj, mv, fmt.Errorf("%w: %s", ErrDeleted, name)
		}
	}
	return s.lookupVersion(name, obj.Current)
}

// GetVersion reads a specific version by number.
func (s *Store) GetVersion(ctx context.Context, name string, version int64) (io.ReadCloser, VersionInfo, error) {
	if err := validateName(name); err != nil {
		return nil, VersionInfo{}, err
	}
	s.mu.Lock()
	_, mv, err := s.lookupVersion(name, version)
	info := VersionInfo{}
	if mv != nil {
		info = toInfo(name, mv)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, info, err
	}
	if err := ctx.Err(); err != nil {
		return nil, info, err
	}
	rc, err := s.openVerified(mv)
	return rc, info, err
}

// Delete marks the current version of name deleted (soft delete).
// History and bytes are retained; Get is refused with ErrDeleted and
// Restore can bring the data back.
func (s *Store) Delete(ctx context.Context, name string) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, mv, err := s.lookupCurrentLocked(name)
	if err != nil {
		return VersionInfo{}, err
	}
	// Object-level delete: hide the current pointer AND mark that exact
	// version soft-deleted. DeletedCurrent lets Restore undo precisely
	// this operation rather than guessing the nearest active version.
	prevCurrent := obj.Current
	mv.State = StateDeleted
	obj.DeletedCurrent = prevCurrent
	obj.Current = 0
	if err := s.manifest.commit(s.root); err != nil {
		mv.State = StateActive
		obj.Current = prevCurrent
		obj.DeletedCurrent = 0
		return VersionInfo{}, err
	}
	return toInfo(name, mv), nil
}

// DeleteVersion marks a specific non-current version deleted. The
// current live version cannot be hidden this way; use Delete for that.
// Deleting an already soft-deleted non-current version is a no-op.
func (s *Store) DeleteVersion(ctx context.Context, name string, version int64) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.manifest.Objects[name]
	if obj == nil {
		return VersionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	if obj.Current == version {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrCurrent, name, version)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}
	if mv.State == StateDeleted {
		return toInfo(name, mv), nil
	}
	mv.State = StateDeleted
	if err := s.manifest.commit(s.root); err != nil {
		mv.State = StateActive
		return VersionInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return toInfo(name, mv), err
	}
	return toInfo(name, mv), nil
}

// Restore reverses an object-level soft delete. The newest active
// version becomes current again; deleted non-current versions stay
// deleted and can be restored individually with RestoreVersion.
func (s *Store) Restore(ctx context.Context, name string) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreLocked(ctx, name)
}

func (s *Store) restoreLocked(ctx context.Context, name string) (VersionInfo, error) {
	obj := s.manifest.Objects[name]
	if obj == nil {
		return VersionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	if obj.Current != 0 {
		return toInfo(name, obj.Versions[obj.Current]), nil
	}
	// Prefer the exact version that Delete hid. It is currently in the
	// Deleted state and Restore clears that. Fall back to the newest
	// active version only if the recorded target vanished or was
	// physically collected (older metadata / purged history).
	target := obj.DeletedCurrent
	if mv := obj.Versions[target]; mv == nil || mv.State == StateCollected {
		var best int64
		for v, mv := range obj.Versions {
			if mv.State == StateActive && v > best {
				best = v
			}
		}
		target = best
	}
	if target == 0 {
		return VersionInfo{}, fmt.Errorf("%w: %s has no restorable active version", ErrCollected, name)
	}
	prev := obj.Current
	prevDel := obj.DeletedCurrent
	targetMV := obj.Versions[target]
	prevState := targetMV.State
	targetMV.State = StateActive
	obj.Current = target
	obj.DeletedCurrent = 0
	if err := s.manifest.commit(s.root); err != nil {
		targetMV.State = prevState
		obj.Current = prev
		obj.DeletedCurrent = prevDel
		return VersionInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return toInfo(name, targetMV), err
	}
	return toInfo(name, targetMV), nil
}

// RestoreVersion clears the soft-deleted state of one specific version.
// If the object currently has no live current version, the restored
// version also becomes current.
func (s *Store) RestoreVersion(ctx context.Context, name string, version int64) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.manifest.Objects[name]
	if obj == nil {
		return VersionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrCollected, name, version)
	}
	if mv.State == StateActive {
		return toInfo(name, mv), nil
	}
	prevCurrent := obj.Current
	mv.State = StateActive
	if obj.Current == 0 {
		obj.Current = version
	}
	if err := s.manifest.commit(s.root); err != nil {
		mv.State = StateDeleted
		obj.Current = prevCurrent
		return VersionInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return toInfo(name, mv), err
	}
	return toInfo(name, mv), nil
}

// PurgeVersion physically reclaims one version's data.
//
// The current live version can never be purged (ErrCurrent). A blob is
// deleted from disk only when this version was the last reference to it
// among all active and soft-deleted versions, so shared bytes stay safe.
// The version record is retained as StateCollected for history and any
// later read is rejected with ErrCollected.
//
// Ordering: manifest is committed to StateCollected first, then the
// unreferenced blob is removed. A crash in between leaves an orphan blob
// that Sweep cleans up; it never leaves a dangling reference.
func (s *Store) PurgeVersion(ctx context.Context, name string, version int64) (VersionInfo, error) {
	if err := validateName(name); err != nil {
		return VersionInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.manifest.Objects[name]
	if obj == nil {
		return VersionInfo{}, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	if obj.Current == version {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrCurrent, name, version)
	}
	mv := obj.Versions[version]
	if mv == nil {
		return VersionInfo{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	if mv.State == StateCollected {
		return toInfo(name, mv), nil
	}
	digest := mv.Digest
	prevState := mv.State
	mv.State = StateCollected
	mv.PurgedAt = nowUTC()
	if err := s.manifest.commit(s.root); err != nil {
		mv.State = prevState
		mv.PurgedAt = ""
		return VersionInfo{}, err
	}
	if !s.digestReferencedLocked(digest, func(v *metaVersion) bool {
		return v.State != StateCollected
	}) {
		if err := s.blobs.remove(digest); err != nil {
			return toInfo(name, mv), err
		}
	}
	if err := ctx.Err(); err != nil {
		return toInfo(name, mv), err
	}
	return toInfo(name, mv), nil
}

// digestReferencedLocked reports whether any version (across all
// objects) with digest d satisfies keep. Callers hold s.mu.
func (s *Store) digestReferencedLocked(d string, keep func(*metaVersion) bool) bool {
	for _, obj := range s.manifest.Objects {
		for _, mv := range obj.Versions {
			if mv.Digest == d && keep(mv) {
				return true
			}
		}
	}
	return false
}

// Versions lists the full version history of name from oldest to newest.
func (s *Store) Versions(ctx context.Context, name string) ([]VersionInfo, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.manifest.Objects[name]
	if obj == nil {
		return nil, fmt.Errorf("%w: object %q", ErrNotFound, name)
	}
	ids := make([]int64, 0, len(obj.Versions))
	for v := range obj.Versions {
		ids = append(ids, v)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]VersionInfo, 0, len(ids))
	for _, v := range ids {
		out = append(out, toInfo(name, obj.Versions[v]))
	}
	return out, nil
}

// List lists all object names that have at least one version, sorted.
func (s *Store) List(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.manifest.Objects))
	for n := range s.manifest.Objects {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}
