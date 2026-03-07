package guestbd

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Compile-time interface checks.
var (
	_ io.ReaderAt = (*Snapshot)(nil)
	_ io.WriterAt = (*Snapshot)(nil)
)

// Snapshot is a writable layer on top of a read-only base image.
// Reads fall through to the shared base image, while writes are tracked
// in the snapshot. A Snapshot can outlive individual TCP connections,
// allowing reconnections to reattach to the same writable state.
//
// Snapshot implements io.ReaderAt and io.WriterAt.
type Snapshot struct {
	server *Server
	roFile *readonlyFile

	mu           sync.Mutex
	dirtyPages   map[int64]*dirtyPageInfo // pageNum => dirty info
	dirtyFile    *os.File                 // temp file for dirty page data
	nextDirtyNum int                      // monotonically increasing
}

// dirtyPageInfo tracks a written page in a snapshot.
type dirtyPageInfo struct {
	hash     pageHash
	dirtyNum int // index into the snapshot's dirtyFile
}

// newSnapshot creates a new Snapshot for the given read-only backing file.
// It creates an unlinked temporary file for dirty page storage.
func newSnapshot(srv *Server, ro *readonlyFile) (*Snapshot, error) {
	tmpFile, err := os.CreateTemp("", "guestbd-dirty-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	// Unlink immediately so it's cleaned up when the fd closes.
	os.Remove(tmpFile.Name())

	return &Snapshot{
		server:     srv,
		roFile:     ro,
		dirtyPages: make(map[int64]*dirtyPageInfo),
		dirtyFile:  tmpFile,
	}, nil
}

// Close releases the snapshot's dirty page storage and unregisters it from
// the server. It does not close any TCP connection using the snapshot.
func (c *Snapshot) Close() error {
	c.mu.Lock()
	if c.dirtyFile != nil {
		c.dirtyFile.Close()
		c.dirtyFile = nil
	}
	c.mu.Unlock()

	c.server.mu.Lock()
	c.server.snapshots.Delete(c)
	c.server.mu.Unlock()

	c.server.releaseReadonlyFile(c.roFile)
	return nil
}

// readPageData reads a full page, checking dirty pages first,
// then falling back to the readonly file.
func (c *Snapshot) readPageData(pageNum int64) ([]byte, error) {
	c.mu.Lock()
	dp, dirty := c.dirtyPages[pageNum]
	c.mu.Unlock()

	c.server.readPages.Add(1)

	if dirty {
		c.server.readPath.Add("from_write", 1)
		// Try the global cache first.
		if data, ok := c.server.cache.Get(dp.hash); ok {
			return data, nil
		}
		// Read from the snapshot's dirty file.
		buf := make([]byte, c.server.pageSize)
		_, err := c.dirtyFile.ReadAt(buf, int64(dp.dirtyNum)*int64(c.server.pageSize))
		if err != nil {
			return nil, fmt.Errorf("reading dirty page: %w", err)
		}
		c.server.cache.Put(dp.hash, buf)
		return buf, nil
	}

	data, _, result, err := c.roFile.readPage(pageNum)
	if err != nil {
		return nil, err
	}
	switch result {
	case readFromCache:
		c.server.readPath.Add("base_mem", 1)
	case readFromDiskCold:
		c.server.readPath.Add("base_disk_cold", 1)
	case readFromDiskMiss:
		c.server.readPath.Add("base_disk_miss", 1)
	}
	return data, nil
}

// ReadAt reads len(p) bytes from the snapshot starting at byte offset off.
// It implements io.ReaderAt.
func (c *Snapshot) ReadAt(p []byte, off int64) (int, error) {
	length := uint64(len(p))
	c.server.readBytes.Add(int64(length))

	offset := uint64(off)
	pageSize := uint64(c.server.pageSize)

	pos := uint64(0)
	for pos < length {
		absOffset := offset + pos
		pageNum := int64(absOffset / pageSize)
		pageOffset := absOffset % pageSize

		pageData, err := c.readPageData(pageNum)
		if err != nil {
			return int(pos), err
		}

		n := pageSize - pageOffset
		if n > length-pos {
			n = length - pos
		}
		copy(p[pos:pos+n], pageData[pageOffset:pageOffset+n])
		pos += n
	}
	return int(pos), nil
}

// WriteAt writes len(p) bytes to the snapshot starting at byte offset off.
// Sub-page writes trigger a read-modify-write cycle.
// It implements io.WriterAt.
func (c *Snapshot) WriteAt(p []byte, off int64) (int, error) {
	length := uint64(len(p))
	pageSize := uint64(c.server.pageSize)

	c.server.writeBytes.Add(int64(length))

	offset := uint64(off)
	pos := uint64(0)
	for pos < length {
		absOffset := offset + pos
		pageNum := int64(absOffset / pageSize)
		pageOffset := absOffset % pageSize

		n := pageSize - pageOffset
		if n > length-pos {
			n = length - pos
		}

		var pageData []byte
		if pageOffset == 0 && n == pageSize {
			// Full page write.
			pageData = make([]byte, pageSize)
			copy(pageData, p[pos:pos+n])
		} else {
			// Partial page write: read-modify-write.
			existing, err := c.readPageData(pageNum)
			if err != nil {
				return int(pos), err
			}
			pageData = make([]byte, pageSize)
			copy(pageData, existing)
			copy(pageData[pageOffset:], p[pos:pos+n])
		}

		h := hashPage(pageData)

		c.mu.Lock()
		dirtyNum := c.nextDirtyNum
		c.nextDirtyNum++

		_, err := c.dirtyFile.WriteAt(pageData, int64(dirtyNum)*int64(pageSize))
		if err != nil {
			c.mu.Unlock()
			return int(pos), fmt.Errorf("writing dirty page: %w", err)
		}

		c.dirtyPages[pageNum] = &dirtyPageInfo{
			hash:     h,
			dirtyNum: dirtyNum,
		}
		c.mu.Unlock()

		c.server.cache.Put(h, pageData)
		c.server.writePages.Add(1)
		pos += n
	}
	return int(pos), nil
}

// handleTrim forgets dirty pages in the given range, reverting
// those pages to the base image.
func (c *Snapshot) handleTrim(offset, length uint64) {
	pageSize := uint64(c.server.pageSize)
	startPage := int64(offset / pageSize)
	endPage := int64((offset + length + pageSize - 1) / pageSize)

	c.mu.Lock()
	defer c.mu.Unlock()
	for p := startPage; p < endPage; p++ {
		delete(c.dirtyPages, p)
	}
}
