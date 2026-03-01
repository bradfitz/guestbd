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
	return inodeKey{dev: stat.Dev, ino: stat.Ino}
}

// readonlyFile represents a shared read-only backing file, keyed by inode.
// Multiple connections to the same file share one readonlyFile.
type readonlyFile struct {
	mu       sync.Mutex
	f        *os.File
	size     int64
	refcount int32
	pageSize int

	// pageHashes is lazily computed per page.
	// A zero value means the page has not been read from disk yet.
	// A value equal to zeroPageHash means the page is all zeros.
	pageHashes []pageHash
}

func newReadonlyFile(f *os.File, size int64, pageSize int) *readonlyFile {
	numPages := (size + int64(pageSize) - 1) / int64(pageSize)
	return &readonlyFile{
		f:          f,
		size:       size,
		pageSize:   pageSize,
		refcount:   1,
		pageHashes: make([]pageHash, numPages),
	}
}

// numPages returns the total number of pages in the file.
func (r *readonlyFile) numPages() int64 {
	return int64(len(r.pageHashes))
}

// readPage reads page n from the backing file and returns its data and hash.
// It uses the cache and lazily populates pageHashes.
func (r *readonlyFile) readPage(n int64, cache *pageCache) ([]byte, pageHash, error) {
	r.mu.Lock()
	h := r.pageHashes[n]
	r.mu.Unlock()

	var zeroHash pageHash
	if h != zeroHash {
		// Already have the hash; try cache.
		if data, ok := cache.Get(h); ok {
			return data, h, nil
		}
	}

	// Need to read from disk.
	buf := make([]byte, r.pageSize)
	offset := n * int64(r.pageSize)
	nr, err := r.f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, pageHash{}, err
	}
	// Zero-fill remainder (last page may be short).
	for i := nr; i < r.pageSize; i++ {
		buf[i] = 0
	}

	h = hashPage(buf)

	r.mu.Lock()
	if r.pageHashes[n] == zeroHash {
		r.pageHashes[n] = h
	}
	r.mu.Unlock()

	cache.Put(h, buf)
	return buf, h, nil
}
