package objectstore

import (
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
)

// verifiedReader wraps a blob file so that corruption is detected before
// a caller can trust the bytes. It streams data through a SHA-256 hasher
// and, on reaching EOF, requires both the exact byte length and the
// digest to match the version's content address. A truncated or altered
// blob therefore surfaces as ErrDigestMismatch instead of silently
// returning partial data.
type verifiedReader struct {
	f       io.ReadCloser
	h       hash.Hash
	want    string // "sha256:<hex>"
	wantLen int64
	got     int64
	closed  bool
}

func (r *verifiedReader) Read(p []byte) (int, error) {
	n, err := r.f.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
		r.got += int64(n)
	}
	if err == io.EOF {
		if verifyErr := r.check(); verifyErr != nil {
			return n, verifyErr
		}
	}
	return n, err
}

// check validates length and digest of everything read so far.
func (r *verifiedReader) check() error {
	if r.got != r.wantLen {
		return fmt.Errorf("%w: length want %d got %d", ErrDigestMismatch, r.wantLen, r.got)
	}
	got := digestPrefix + hex.EncodeToString(r.h.Sum(nil))
	if !strings.HasPrefix(got, digestPrefix) || got != r.want {
		return fmt.Errorf("%w: digest want %s got %s", ErrDigestMismatch, r.want, got)
	}
	return nil
}

func (r *verifiedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.f.Close()
}
