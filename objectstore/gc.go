package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// RecoveryReport describes the result of a post-crash recovery pass.
type RecoveryReport struct {
	StagingRemoved  int      `json:"staging_removed"`
	ManifestTempRm  int      `json:"manifest_temp_removed"`
	MissingBlobs    []string `json:"missing_blobs,omitempty"`
	CheckedVersions int      `json:"checked_versions"`
}

// VerifyReport describes a full integrity verification.
type VerifyReport struct {
	BlobsChecked int      `json:"blobs_checked"`
	Corrupt      []string `json:"corrupt,omitempty"`
	Missing      []string `json:"missing,omitempty"`
}

// refSet classifies blob digests referenced by the current manifest.
type refSet struct {
	// referenced is digest -> true for every version that is active or
	// soft-deleted (i.e. its bytes must remain readable).
	referenced map[string]bool
	// onlyDeleted is true for digests referenced exclusively by
	// soft-deleted versions (eligible when SweepDeletedVersions is set).
	onlyDeleted map[string]bool
}

// buildRefs computes the reference set from the live manifest. Callers
// hold s.mu (snapshot reads while the mutex is held).
func (s *Store) buildRefs() refSet {
	rs := refSet{referenced: map[string]bool{}, onlyDeleted: map[string]bool{}}
	activeRef := map[string]bool{}
	deletedRef := map[string]bool{}
	for _, obj := range s.manifest.Objects {
		for _, mv := range obj.Versions {
			if mv.State == StateCollected {
				continue
			}
			if mv.State == StateDeleted {
				deletedRef[mv.Digest] = true
			} else {
				activeRef[mv.Digest] = true
			}
			rs.referenced[mv.Digest] = true
		}
	}
	for d := range deletedRef {
		if !activeRef[d] {
			rs.onlyDeleted[d] = true
		}
	}
	return rs
}

// computeStatsLocked gathers physical and logical usage. It does not
// modify anything. Callers hold s.mu.
func (s *Store) computeStatsLocked() Stats {
	var st Stats
	blobSize := map[string]int64{}
	_ = s.blobs.walkBlobs(func(digest string, size int64) error {
		blobSize[digest] = size
		return nil
	})
	rs := s.buildRefs()

	refBytes := map[string]int64{}
	for name, obj := range s.manifest.Objects {
		_ = name
		if len(obj.Versions) > 0 {
			st.Objects++
		}
		for _, mv := range obj.Versions {
			switch mv.State {
			case StateActive:
				st.LiveVersions++
			case StateDeleted:
				st.DeletedVersions++
			case StateCollected:
				st.CollectedVersion++
			}
			if mv.State != StateCollected {
				// Logical usage counts every readable version (so shared
				// content contributes once per version that sees it)...
				st.LogicalBytes += mv.Size
				// ...while physical usage counts each stored blob once.
				if _, ok := refBytes[mv.Digest]; !ok {
					refBytes[mv.Digest] = blobSize[mv.Digest]
				}
			}
		}
	}
	st.ReferencedBlobs = len(rs.referenced)
	for _, sz := range refBytes {
		st.PhysicalBytes += sz
	}
	for d, sz := range blobSize {
		if !rs.referenced[d] {
			st.OrphanBlobs++
			st.ReclaimableBytes += sz
		}
	}
	if s.opts.SweepDeletedVersions {
		// Soft-deleted-only bytes are additionally reclaimable under the
		// aggressive policy.
		added := map[string]bool{}
		for _, obj := range s.manifest.Objects {
			for _, mv := range obj.Versions {
				if mv.State == StateDeleted && rs.onlyDeleted[mv.Digest] && !added[mv.Digest] {
					added[mv.Digest] = true
					st.ReclaimableBytes += blobSize[mv.Digest]
				}
			}
		}
	}
	return st
}

// Stats computes usage without deleting anything.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	return s.computeStatsLocked(), nil
}

// Sweep reclaims disk space. It is safe to run repeatedly, concurrently
// with readers (serialized by the store lock) and after a crash:
//
//   - blobs present on disk but referenced by no readable version are
//     removed (true orphans, including leftovers of crashed writes that
//     were already promoted out of tmp);
//   - when Options.SweepDeletedVersions is set, blobs referenced only by
//     soft-deleted versions are removed and those versions transition to
//     StateCollected (irreversible).
//
// Crash-safe order for the destructive policy: metadata is committed
// first (versions marked collected), blobs are deleted second. A crash
// in the middle only delays space release and is finished by the next
// Sweep; it never produces a dangling metadata reference.
func (s *Store) Sweep(ctx context.Context) (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}

	// Phase 1: optionally downgrade soft-deleted-only versions.
	if s.opts.SweepDeletedVersions {
		rs := s.buildRefs()
		changed := false
		for _, obj := range s.manifest.Objects {
			for _, mv := range obj.Versions {
				if mv.State == StateDeleted && rs.onlyDeleted[mv.Digest] {
					mv.State = StateCollected
					if mv.PurgedAt == "" {
						mv.PurgedAt = nowUTC()
					}
					changed = true
				}
			}
		}
		if changed {
			if err := s.manifest.commit(s.root); err != nil {
				return Stats{}, err
			}
			// Test hook: simulate a crash after the destructive metadata
			// commit but before any blob is deleted. On restart the
			// versions are Collected and the orphaned blobs are cleaned by
			// a follow-up Sweep; metadata stays self-consistent.
			if s.blobs.crashAfterSweepCommit != nil {
				if err := s.blobs.crashAfterSweepCommit(); err != nil {
					return s.computeStatsLocked(), err
				}
			}
		}
	}

	// Phase 2: remove every physical blob no longer referenced.
	refs := s.buildRefs()
	err := s.blobs.walkBlobs(func(digest string, size int64) error {
		if refs.referenced[digest] {
			return nil
		}
		// Test hook: simulate a crash after metadata commit but before an
		// orphan blob is physically removed.
		if s.blobs.crashBeforeBlobDelete != nil {
			if err := s.blobs.crashBeforeBlobDelete(digest); err != nil {
				return err
			}
		}
		return s.blobs.remove(digest)
	})
	if err != nil {
		// Even if deletion was interrupted, metadata remains consistent.
		return s.computeStatsLocked(), err
	}
	return s.computeStatsLocked(), nil
}

// recoverLocked removes stale staging/manifest temp files and verifies
// that every blob referenced by a readable version actually exists. It
// never deletes committed blob data. Callers hold s.mu.
func (s *Store) recoverLocked() (RecoveryReport, error) {
	rep := RecoveryReport{}

	n, err := s.blobs.removeStaging()
	if err != nil {
		return rep, err
	}
	rep.StagingRemoved = n

	mn, err := removeManifestTemp(s.root)
	if err != nil {
		return rep, err
	}
	rep.ManifestTempRm = mn

	checked := 0
	missing := map[string]bool{}
	for _, obj := range s.manifest.Objects {
		for _, mv := range obj.Versions {
			if mv.State == StateCollected {
				continue
			}
			checked++
			ok, _, err := s.blobs.exists(mv.Digest)
			if err != nil {
				return rep, err
			}
			if !ok {
				missing[mv.Digest] = true
			}
		}
	}
	rep.CheckedVersions = checked
	for d := range missing {
		rep.MissingBlobs = append(rep.MissingBlobs, d)
	}
	sort.Strings(rep.MissingBlobs)
	return rep, nil
}

// Recover performs an explicit recovery pass and returns its report.
// It is also run automatically on Open.
func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	return s.recoverLocked()
}

// Verify re-hashes every blob referenced by a readable version and
// reports digests that are missing or whose bytes do not hash to their
// content address. The store metadata is not modified.
func (s *Store) Verify(ctx context.Context) (VerifyReport, error) {
	s.mu.Lock()
	type task struct {
		digest string
	}
	var tasks []task
	seen := map[string]bool{}
	for _, obj := range s.manifest.Objects {
		for _, mv := range obj.Versions {
			if mv.State == StateCollected || seen[mv.Digest] {
				continue
			}
			seen[mv.Digest] = true
			tasks = append(tasks, task{mv.Digest})
		}
	}
	s.mu.Unlock()

	rep := VerifyReport{}
	for _, t := range tasks {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		f, err := s.blobs.open(t.digest)
		if err != nil {
			rep.Missing = append(rep.Missing, t.digest)
			continue
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			rep.Corrupt = append(rep.Corrupt, t.digest)
			continue
		}
		f.Close()
		got := digestPrefix + hex.EncodeToString(h.Sum(nil))
		if got != t.digest {
			rep.Corrupt = append(rep.Corrupt, t.digest)
			continue
		}
		rep.BlobsChecked++
	}
	sort.Strings(rep.Corrupt)
	sort.Strings(rep.Missing)
	return rep, nil
}

// removeManifestTemp deletes leftover ".manifest-*.tmp" files from an
// interrupted commit. Such a file is never the committed manifest, so
// removing it is always safe.
func removeManifestTemp(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read store dir: %w", err)
	}
	removed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if isManifestTemp(name) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil &&
				!errors.Is(err, os.ErrNotExist) {
				return removed, fmt.Errorf("remove manifest temp: %w", err)
			}
			removed++
		}
	}
	return removed, nil
}

// isManifestTemp matches names produced by manifest.commit.
func isManifestTemp(name string) bool {
	const pre, suf = ".manifest-", ".tmp"
	return len(name) > len(pre)+len(suf) &&
		name[:len(pre)] == pre &&
		name[len(name)-len(suf):] == suf
}
