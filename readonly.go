package guestbd

import (
	"io"
	"sync"
)

// baseImageState represents a shared read-only backing image.
// Multiple connections share one baseImageState.
//
// Locking: fields are protected by either mu or Server.mu as noted.
// base and size are immutable after construction and need no lock.
// When both locks are needed, Server.mu must be acquired first;
// mu is never held when acquiring Server.mu.
type baseImageState struct {
	srv         *Server   // immutable
	base        BaseImage // immutable; used for page reads
	size        int64     // immutable; cached from base.Size()
	identityKey any       // immutable; non-nil if base returned a key; used as roFiles map key

	idleSince int64 // protected by Server.mu; monotonic seq for LRU eviction; 0 while active

	mu       sync.Mutex
	refcount int32 // protected by mu
	// pageHashes is lazily computed per page.
	// Absent from the map means the page has not been read from disk yet.
	// A value equal to Server.zeroPageHash means the page is all zeros.
	pageHashes map[int64]pageHash // protected by mu
}

// newBaseImageState creates a new baseImageState for the given base image.
// The refcount starts at 1.
func newBaseImageState(srv *Server, base BaseImage, key any) *baseImageState {
	return &baseImageState{
		srv:         srv,
		base:        base,
		size:        base.Size(),
		identityKey: key,
		refcount:    1,
		pageHashes:  make(map[int64]pageHash),
	}
}

// readResult describes where a page read was served from.
type readResult int

const (
	readFromCache    readResult = iota // hash known, data in LRU cache
	readFromDiskCold                   // hash unknown (first read of this page), read from disk
	readFromDiskMiss                   // hash known but evicted from LRU cache, re-read from disk
)

// readPage reads page n from the backing file and returns its data, hash,
// and how the read was served (cache hit, cold disk read, or cache miss disk read).
func (r *baseImageState) readPage(n int64) (data []byte, hash pageHash, result readResult, err error) {
	r.mu.Lock()
	h, hashKnown := r.pageHashes[n]
	r.mu.Unlock()

	cache := r.srv.cache
	pageSize := r.srv.pageSize
	if hashKnown {
		// Already have the hash; try cache.
		if d, ok := cache.Get(h); ok {
			return d, h, readFromCache, nil
		}
	}

	// Need to read from disk.
	buf := make([]byte, pageSize)
	offset := n * int64(pageSize)
	nr, readErr := r.base.ReadAt(buf, offset)
	if readErr != nil && readErr != io.EOF {
		return nil, pageHash{}, 0, readErr
	}
	// Zero-fill remainder (last page may be short).
	for i := nr; i < pageSize; i++ {
		buf[i] = 0
	}

	h = hashPage(buf)

	r.mu.Lock()
	if _, ok := r.pageHashes[n]; !ok {
		r.pageHashes[n] = h
	}
	r.mu.Unlock()

	cache.Put(h, buf)
	if hashKnown {
		return buf, h, readFromDiskMiss, nil
	}
	return buf, h, readFromDiskCold, nil
}
