package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// blobStore manages the content-addressed data files. A blob's name is
// its SHA-256 digest, so identical content always maps to one physical
// file regardless of write order or chunk boundaries.
//
// Layout under the store root:
//
//	blobs/<aa>/<full-hex-digest>   immutable data files
//	tmp/stage-*                    in-progress writes (safe to delete)
//
// A blob is made visible by rename into its final digest path, so
// readers can never observe a partially written blob: an incomplete
// write stays in tmp/ and is reclaimed on the next Recover/Open.
type blobStore struct {
	root   string // blobs directory
	tmpDir string // staging directory

	// crashAfterInstall, when non-nil, is invoked after a blob has been
	// durably installed but before Put commits the manifest. Returning an
	// error simulates a process crash in that window: bytes are present and
	// durable, metadata never references them. Intended for tests.
	crashAfterInstall func(digest string) error
	// crashBeforeBlobDelete, when non-nil, is invoked inside GC just
	// before an orphan blob is removed. Returning an error simulates a
	// crash during reclamation. Intended for tests.
	crashBeforeBlobDelete func(digest string) error
	// crashAfterSweepCommit fires after the destructive GC metadata commit
	// but before blob deletion, simulating a crash in that window.
	crashAfterSweepCommit func() error
}

const (
	blobsDirName = "blobs"
	tmpDirName   = "tmp"
	// digestPrefix tags the algorithm so a future migration is possible.
	digestPrefix = "sha256:"
	// stageBufSize is the streaming copy buffer; it does not affect the
	// resulting digest, only how often syscalls occur.
	stageBufSize = 64 * 1024
)

func newBlobStore(root string) *blobStore {
	return &blobStore{
		root:   filepath.Join(root, blobsDirName),
		tmpDir: filepath.Join(root, tmpDirName),
	}
}

// ensureDirs creates the on-disk layout, idempotently.
func (b *blobStore) ensureDirs() error {
	for _, d := range []string{b.root, b.tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", d, err)
		}
	}
	return nil
}

// blobPath maps a digest to its physical file path. The first two hex
// chars form a fan-out shard to keep directory sizes bounded.
func (b *blobStore) blobPath(digest string) (string, error) {
	hexPart, ok := strings.CutPrefix(digest, digestPrefix)
	if !ok || len(hexPart) != sha256.Size*2 {
		return "", fmt.Errorf("%w: %q", ErrDigestMismatch, digest)
	}
	return filepath.Join(b.root, hexPart[:2], hexPart), nil
}

// put streams r into a private temp file while hashing, then installs
// the result at its content address.
//
//   - When expectDigest is non-empty, the computed digest must match it,
//     otherwise ErrDigestMismatch is returned and nothing is installed.
//   - If the target blob already exists, the staging file is discarded:
//     identical content never creates a second physical copy.
//
// The staging file is fsynced and atomically renamed into place and the
// containing directory is fsynced, making the new blob durable before
// any metadata can reference it.
func (b *blobStore) put(r io.Reader, expectDigest string) (digest string, size int64, err error) {
	if err := b.ensureDirs(); err != nil {
		return "", 0, err
	}

	tmp, err := os.CreateTemp(b.tmpDir, "stage-*")
	if err != nil {
		return "", 0, fmt.Errorf("create stage file: %w", err)
	}
	tmpName := tmp.Name()
	// On any error path the staging file is removed; a crash in the
	// middle leaves it for Recover to reap.
	committed := false
	defer func() {
		tmp.Close()
		if !committed {
			os.Remove(tmpName)
		}
	}()

	h := sha256.New()
	buf := make([]byte, stageBufSize)
	n, err := io.CopyBuffer(tmp, io.TeeReader(r, h), buf)
	if err != nil {
		return "", 0, fmt.Errorf("stage blob: %w", err)
	}
	digest = digestPrefix + hex.EncodeToString(h.Sum(nil))
	if expectDigest != "" && !constantTimeEqual(digest, expectDigest) {
		return "", 0, fmt.Errorf("%w: expected %s got %s", ErrDigestMismatch, expectDigest, digest)
	}
	if err := tmp.Sync(); err != nil {
		return "", 0, fmt.Errorf("fsync stage blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", 0, fmt.Errorf("close stage blob: %w", err)
	}

	dst, err := b.blobPath(digest)
	if err != nil {
		return "", 0, err
	}
	shardPath := filepath.Dir(dst)
	_, shardErr := os.Stat(shardPath)
	shardExisted := shardErr == nil
	if err := os.MkdirAll(shardPath, 0o755); err != nil {
		return "", 0, fmt.Errorf("create blob shard: %w", err)
	}
	// Persist a freshly created shard directory so the later rename into
	// it cannot be lost after an unclean shutdown.
	if !shardExisted {
		rootDir, err := os.Open(b.root)
		if err != nil {
			return "", 0, fmt.Errorf("open blobs dir: %w", err)
		}
		if err := rootDir.Sync(); err != nil {
			rootDir.Close()
			return "", 0, fmt.Errorf("fsync blobs dir: %w", err)
		}
		rootDir.Close()
	}

	// Content-address dedup: a file already at this digest is by
	// definition the same bytes, so just discard the staging copy.
	if _, statErr := os.Stat(dst); statErr == nil {
		return digest, n, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", 0, fmt.Errorf("stat blob: %w", statErr)
	}

	shardDir, err := os.Open(shardPath)
	if err != nil {
		return "", 0, fmt.Errorf("open blob shard dir: %w", err)
	}
	defer shardDir.Close()
	if err := os.Rename(tmpName, dst); err != nil {
		return "", 0, fmt.Errorf("install blob: %w", err)
	}
	committed = true
	if err := shardDir.Sync(); err != nil {
		return "", 0, fmt.Errorf("fsync blob dir: %w", err)
	}
	if b.crashAfterInstall != nil {
		if err := b.crashAfterInstall(digest); err != nil {
			return "", 0, err
		}
	}
	return digest, n, nil
}

// open opens an existing blob for reading. A missing file is reported as
// a not-found error rather than a raw filesystem error.
func (b *blobStore) open(digest string) (*os.File, error) {
	p, err := b.blobPath(digest)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("blob %s: %w", digest, ErrNotFound)
		}
		return nil, fmt.Errorf("open blob %s: %w", digest, err)
	}
	return f, nil
}

// exists reports whether the blob exists and its on-disk size.
func (b *blobStore) exists(digest string) (bool, int64, error) {
	p, err := b.blobPath(digest)
	if err != nil {
		return false, 0, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("stat blob: %w", err)
	}
	return true, fi.Size(), nil
}

// remove deletes a blob. Missing blobs are not an error so GC and Purge
// are idempotent and safe to replay after a crash.
func (b *blobStore) remove(digest string) error {
	p, err := b.blobPath(digest)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove blob: %w", err)
	}
	return nil
}

// walkBlobs invokes fn for every committed blob (digest, size). It
// never descends into tmp: staging files are not blobs.
func (b *blobStore) walkBlobs(fn func(digest string, size int64) error) error {
	entries, err := os.ReadDir(b.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read blobs dir: %w", err)
	}
	for _, shard := range entries {
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		shardPath := filepath.Join(b.root, shard.Name())
		files, err := os.ReadDir(shardPath)
		if err != nil {
			return fmt.Errorf("read shard %s: %w", shard.Name(), err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			info, err := f.Info()
			if err != nil {
				return fmt.Errorf("stat %s: %w", f.Name(), err)
			}
			if err := fn(digestPrefix+f.Name(), info.Size()); err != nil {
				return err
			}
		}
	}
	return nil
}

// removeStaging deletes all leftover temp files from crashed/aborted
// writes. Staging files can never be referenced by any committed
// manifest, so removing them cannot create dangling references.
func (b *blobStore) removeStaging() (int, error) {
	entries, err := os.ReadDir(b.tmpDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read tmp dir: %w", err)
	}
	removed := 0
	for _, e := range entries {
		// Never follow or remove directories; only stage files live here.
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(b.tmpDir, e.Name())); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("remove staging %s: %w", e.Name(), err)
		}
		removed++
	}
	return removed, nil
}

// constantTimeEqual compares two digest strings without leaking length
// information through timing; both inputs are fixed-length digests.
func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
