package objectstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// metaVersion is the on-disk metadata record for a single version.
type metaVersion struct {
	Version  int64        `json:"version"`
	Size     int64        `json:"size"`
	Digest   string       `json:"digest"`
	Created  string       `json:"created"` // RFC3339Nano UTC
	State    VersionState `json:"state"`
	PurgedAt string       `json:"purged_at,omitempty"`

	// Retention state. All three fields are persisted in the manifest, so
	// protection survives a process restart: bytes covered by an active
	// retention can never be reclaimed merely because the process died and
	// came back.
	//
	//	RetainUntil  RFC3339Nano deadline; the sentinel "permanent" means
	//	             never-expiring. Empty string means no (current or past)
	//	             retention was ever configured.
	//	RetainReleasedAt is set when an active retention was explicitly
	//	             released early; released data is reclaimed first under
	//	             capacity pressure.
	RetainUntil      string `json:"retain_until,omitempty"`
	RetainReleasedAt string `json:"retain_released_at,omitempty"`
}

// retainPermanentSentinel marks a retention that never expires on its own.
const retainPermanentSentinel = "permanent"

// objectMeta holds the version history of one named object.
type objectMeta struct {
	Current int64 `json:"current"` // 0 = object currently deleted
	// DeletedCurrent records which version was current when Delete moved
	// the object to its tombstone state, so Restore can undo exactly that
	// delete rather than guessing from surrounding versions.
	DeletedCurrent int64                  `json:"deleted_current,omitempty"`
	Next           int64                  `json:"next"`
	Versions       map[int64]*metaVersion `json:"versions"`
}

// manifest is the root metadata document. It is serialized atomically
// (temp file + fsync + rename + dir fsync), so it is never observed
// half-written after a crash: the previous complete file survives.
type manifest struct {
	Objects map[string]*objectMeta `json:"objects"`
}

// manifestName is the file name of the committed manifest.
const manifestName = "manifest.json"

// newManifest returns an empty initialized manifest.
func newManifest() *manifest {
	return &manifest{Objects: map[string]*objectMeta{}}
}

// loadManifest reads and decodes the manifest at dir/manifest.json.
// If no manifest exists yet an empty manifest is returned (a brand-new
// store). Because commit is atomic, an existing manifest is always
// either complete or absent; a decode error indicates real corruption.
func loadManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newManifest(), nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	m := newManifest()
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Objects == nil {
		m.Objects = map[string]*objectMeta{}
	}
	return m, nil
}

// commit writes the manifest atomically and durably:
//
//  1. write to a unique temp file in the same directory and fsync it
//  2. rename over the committed path (atomic on POSIX)
//  3. fsync the directory so the rename survives a power loss
//
// A crash during step 1 leaves only a stale temp file (cleaned by
// Recover) and the previous committed manifest intact. A crash during
// step 2 leaves either the old or the new manifest, never a merge.
func (m *manifest) commit(dir string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	dirFd, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open manifest dir: %w", err)
	}
	defer dirFd.Close()

	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("create manifest temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write manifest temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("fsync manifest temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close manifest temp: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, manifestName)); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	if err := dirFd.Sync(); err != nil {
		return fmt.Errorf("fsync manifest dir: %w", err)
	}
	cleanup = false
	return nil
}

// clone returns a deep copy so a snapshot can be mutated under the
// store lock without aliasing the currently committed structure.
func (m *manifest) clone() *manifest {
	cp := newManifest()
	for name, obj := range m.Objects {
		oc := &objectMeta{
			Current:  obj.Current,
			Next:     obj.Next,
			Versions: make(map[int64]*metaVersion, len(obj.Versions)),
		}
		for v, mv := range obj.Versions {
			mvCopy := *mv
			oc.Versions[v] = &mvCopy
		}
		cp.Objects[name] = oc
	}
	return cp
}
