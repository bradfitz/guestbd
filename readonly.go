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
	bs := &baseImageState{
		srv:         srv,
		base:        base,
		size:        base.Size(),
		identityKey: key,
		refcount:    1,
	}
	if srv.cache != nil {
		bs.pageHashes = make(map[int64]pageHash)
	}
	return bs
}

// readResult describes where a page read was served from.
type readResult int

const (
	readFromCache    readResult = iota // hash known, data in LRU cache
	readFromDiskCold                   // hash unknown (first read of this page), read from disk
	readFromDiskMiss                   // hash known but evicted from LRU cache, re-read from disk
)

// readPage reads page n from the backing file into buf and returns its hash
// and how the read was served (cache hit, cold disk read, or cache miss disk read).
// buf must be at least pageSize bytes long or readPage panics.
//
// When the server has no page cache (WithMaxMem(0)), readPage skips hashing
// and cache operations and reads directly from the base image.
func (bs *baseImageState) readPage(buf []byte, n int64) (hash pageHash, result readResult, err error) {
	cache := bs.srv.cache
	pageSize := bs.srv.pageSize
	_ = buf[pageSize-1] // bounds check hint; panics if too small

	if cache == nil {
		// No caching; read straight from the base image.
		offset := n * int64(pageSize)
		nr, readErr := bs.base.ReadAt(buf[:pageSize], offset)
		if readErr != nil && readErr != io.EOF {
			return pageHash{}, 0, readErr
		}
		for i := nr; i < pageSize; i++ {
			buf[i] = 0
		}
		return pageHash{}, readFromDiskCold, nil
	}

	bs.mu.Lock()
	h, hashKnown := bs.pageHashes[n]
	bs.mu.Unlock()

	if hashKnown {
		// Already have the hash; try cache.
		if d, ok := cache.Get(h); ok {
			copy(buf, d)
			return h, readFromCache, nil
		}
	}

	// Need to read from disk.
	offset := n * int64(pageSize)
	nr, readErr := bs.base.ReadAt(buf[:pageSize], offset)
	if readErr != nil && readErr != io.EOF {
		return pageHash{}, 0, readErr
	}
	// Zero-fill remainder (last page may be short).
	for i := nr; i < pageSize; i++ {
		buf[i] = 0
	}

	h = hashPage(buf[:pageSize])

	bs.mu.Lock()
	if _, ok := bs.pageHashes[n]; !ok {
		bs.pageHashes[n] = h
	}
	bs.mu.Unlock()

	cache.Put(h, buf[:pageSize])
	if hashKnown {
		return h, readFromDiskMiss, nil
	}
	return h, readFromDiskCold, nil
}
