package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

// TestConcurrentPutsSerializesVersioning hammers one object from many
// goroutines. Because all Store mutations hold the same mutex and the
// manifest commit is atomic, exactly N distinct content versions must be
// visible afterwards, each version number from 1..N present exactly once,
// and every version's bytes must read back intact.
func TestConcurrentPutsSerializesVersioning(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("concurrent-payload-%03d", i))
			vi, err := s.Put(context.Background(), "hot", bytes.NewReader(payload))
			if err != nil {
				errs <- err
				return
			}
			t.Logf("writer input=%03d committed version=%d digest=%s", i, vi.Version, vi.Digest)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent put failed: %v", err)
	}

	vs, err := s.Versions(context.Background(), "hot")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	t.Logf("judgment basis: expect exactly %d versions with unique 1..%d numbering; got %d", n, n, len(vs))
	if len(vs) != n {
		t.Fatalf("lost or duplicated versions: got %d want %d", len(vs), n)
	}
	seen := map[int64]bool{}
	contents := map[string]bool{}
	for _, v := range vs {
		if v.Version < 1 || v.Version > n || seen[v.Version] {
			t.Fatalf("bad/duplicate version number %d", v.Version)
		}
		seen[v.Version] = true
		contents[v.Digest] = true
		body := getVersionBody(t, s, "hot", v.Version)
		if v.Digest != digestOf(body) {
			t.Fatalf("version %d body digest mismatch", v.Version)
		}
	}
	if len(contents) != n {
		t.Fatalf("expected %d distinct contents, got %d", n, len(contents))
	}
	t.Logf("result: %d serialized versions, all bytes verified, no lost/duplicate commit", n)
}

// TestConcurrentReadDuringWrites runs readers continuously while writers
// commit. A reader must observe either a complete prior version or a
// complete new one; it must never see a partial read or a digest mismatch.
func TestConcurrentReadDuringWrites(t *testing.T) {
	s, _ := newTestStore(t, Options{})
	putBytes(t, s, "live", []byte("seed-content-v0"))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var readerErr error
	var rmu sync.Mutex

	// Readers.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				rc, info, err := s.Get(ctx, "live")
				if errors.Is(err, ErrDeleted) {
					continue
				}
				if err != nil {
					if ctx.Err() != nil {
						return // shutting down, not an inconsistency
					}
					rmu.Lock()
					readerErr = fmt.Errorf("Get: %w", err)
					rmu.Unlock()
					return
				}
				body, rerr := io.ReadAll(rc)
				rc.Close()
				if rerr != nil {
					rmu.Lock()
					readerErr = fmt.Errorf("reader version=%d: %w", info.Version, rerr)
					rmu.Unlock()
					return
				}
				if info.Digest != digestOf(body) {
					rmu.Lock()
					readerErr = fmt.Errorf("torn read at version=%d", info.Version)
					rmu.Unlock()
					return
				}
			}
		}()
	}

	// Writers commit distinct versions.
	const writes = 30
	for w := 0; w < writes; w++ {
		payload := []byte(fmt.Sprintf("live-content-block-%03d-padding-xxxxxxxx", w))
		putBytes(t, s, "live", payload)
	}
	cancel()
	wg.Wait()

	if readerErr != nil {
		t.Fatalf("reader saw inconsistent state: %v", readerErr)
	}
	vs, _ := s.Versions(context.Background(), "live")
	t.Logf("result: readers ran across %d commits with zero torn/partial reads", len(vs))
}
