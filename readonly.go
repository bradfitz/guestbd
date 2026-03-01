package main

import (
	"io"
	"os"
	"sync"
	"syscall"
)

// inodeKey uniquely identifies a file by device and inode number.
type inodeKey struct {
	dev uint64
	ino uint64
}

func fileInodeKey(fi os.FileInfo) inodeKey {
	stat := fi.Sys().(*syscall.Stat_t)
	return inodeKey{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}
}

// readonlyFile represents a shared read-only backing file, keyed by inode.
// Multiple connections to the same file share one readonlyFile.
type readonlyFile struct {
	srv      *Server
	mu       sync.Mutex
	f        *os.File
	size     int64
	refcount int32

	// pageHashes is lazily computed per page.
	// A zero value means the page has not been read from disk yet.
	// A value equal to Server.zeroPageHash means the page is all zeros.
	pageHashes []pageHash
}

func newReadonlyFile(srv *Server, f *os.File, size int64) *readonlyFile {
	pageSize := srv.pageSize
	numPages := (size + int64(pageSize) - 1) / int64(pageSize)
	return &readonlyFile{
		srv:        srv,
		f:          f,
		size:       size,
		refcount:   1,
		pageHashes: make([]pageHash, numPages),
	}
}

// fileInfo returns the FileInfo for the underlying file.
func (r *readonlyFile) fileInfo() os.FileInfo {
	fi, _ := r.f.Stat()
	return fi
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
func (r *readonlyFile) readPage(n int64) (data []byte, hash pageHash, result readResult, err error) {
	r.mu.Lock()
	h := r.pageHashes[n]
	r.mu.Unlock()

	cache := r.srv.cache
	pageSize := r.srv.pageSize

	var zeroHash pageHash
	hashKnown := h != zeroHash
	if hashKnown {
		// Already have the hash; try cache.
		if d, ok := cache.Get(h); ok {
			return d, h, readFromCache, nil
		}
	}

	// Need to read from disk.
	buf := make([]byte, pageSize)
	offset := n * int64(pageSize)
	nr, readErr := r.f.ReadAt(buf, offset)
	if readErr != nil && readErr != io.EOF {
		return nil, pageHash{}, 0, readErr
	}
	// Zero-fill remainder (last page may be short).
	for i := nr; i < pageSize; i++ {
		buf[i] = 0
	}

	h = hashPage(buf)

	r.mu.Lock()
	if r.pageHashes[n] == zeroHash {
		r.pageHashes[n] = h
	}
	r.mu.Unlock()

	cache.Put(h, buf)
	if hashKnown {
		return buf, h, readFromDiskMiss, nil
	}
	return buf, h, readFromDiskCold, nil
}
