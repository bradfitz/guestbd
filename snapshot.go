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
	roFile *baseImageState

	mu           sync.Mutex
	dirtyPages   map[int64]dirtyPageInfo // pageNum => dirty info
	dirtyFile    *os.File                // temp file for dirty page data
	nextDirtyNum int                     // monotonically increasing
}

// dirtyPageInfo tracks a written page in a snapshot.
type dirtyPageInfo struct {
	hash     pageHash
	dirtyNum int // index into the snapshot's dirtyFile
}

// newSnapshot creates a new Snapshot for the given read-only backing file.
// It creates an unlinked temporary file for dirty page storage.
func newSnapshot(srv *Server, ro *baseImageState) (*Snapshot, error) {
	tmpFile, err := os.CreateTemp("", "guestbd-dirty-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	// Unlink immediately so it's cleaned up when the fd closes.
	os.Remove(tmpFile.Name())

	return &Snapshot{
		server:     srv,
		roFile:     ro,
		dirtyPages: make(map[int64]dirtyPageInfo),
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

	c.server.releaseBaseImageState(c.roFile)
	return nil
}

// readPageData reads a full page into buf, checking dirty pages first,
// then falling back to the readonly file.
// buf must be at least pageSize bytes long or readPageData panics.
func (c *Snapshot) readPageData(buf []byte, pageNum int64) error {
	pageSize := c.server.pageSize
	_ = buf[pageSize-1] // bounds check hint; panics if too small

	c.mu.Lock()
	dp, dirty := c.dirtyPages[pageNum]
	c.mu.Unlock()

	c.server.readPages.Add(1)

	if dirty {
		c.server.readPath.Add("from_write", 1)
		cache := c.server.cache
		if cache != nil {
			// Try the global cache first.
			if data, ok := cache.Get(dp.hash); ok {
				copy(buf, data)
				return nil
			}
		}
		// Read from the snapshot's dirty file.
		_, err := c.dirtyFile.ReadAt(buf[:pageSize], int64(dp.dirtyNum)*int64(pageSize))
		if err != nil {
			return fmt.Errorf("reading dirty page: %w", err)
		}
		if cache != nil {
			cache.Put(dp.hash, buf[:pageSize])
		}
		return nil
	}

	_, result, err := c.roFile.readPage(buf, pageNum)
	if err != nil {
		return err
	}
	switch result {
	case readFromCache:
		c.server.readPath.Add("base_mem", 1)
	case readFromDiskCold:
		c.server.readPath.Add("base_disk_cold", 1)
	case readFromDiskMiss:
		c.server.readPath.Add("base_disk_miss", 1)
	}
	return nil
}

// ReadAt reads len(p) bytes from the snapshot starting at byte offset off.
// It implements io.ReaderAt.
func (c *Snapshot) ReadAt(p []byte, off int64) (int, error) {
	length := uint64(len(p))
	c.server.readBytes.Add(int64(length))

	pageSize := uint64(c.server.pageSize)

	pos := uint64(0)
	for pos < length {
		absOffset := uint64(off) + pos
		pageNum := int64(absOffset / pageSize)
		pageOffset := absOffset % pageSize

		// Read up to the end of the page or the end of the requested length,
		// whichever is smaller. Usually this will be pageSize, except for the
		// first and last pages if the ReadAt call isn't page-aligned.
		n := min(pageSize-pageOffset, length-pos)

		if pageOffset == 0 && n == pageSize {
			// Full page; read directly into the caller's buffer.
			if err := c.readPageData(p[pos:pos+pageSize], pageNum); err != nil {
				return int(pos), err
			}
		} else {
			// Partial page; need a scratch buffer from the pool.
			bufp := c.server.pageBufPool.Get().(*[]byte)
			if err := c.readPageData(*bufp, pageNum); err != nil {
				return int(pos), err
			}
			copy(p[pos:pos+n], (*bufp)[pageOffset:pageOffset+n])
			c.server.pageBufPool.Put(bufp)
		}
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

		pageData := make([]byte, pageSize)
		if pageOffset == 0 && n == pageSize {
			// Full page write.
			copy(pageData, p[pos:pos+n])
		} else {
			// Partial page write: read-modify-write.
			if err := c.readPageData(pageData, pageNum); err != nil {
				return int(pos), err
			}
			copy(pageData[pageOffset:], p[pos:pos+n])
		}

		cache := c.server.cache
		var h pageHash
		if cache != nil {
			h = hashPage(pageData)
		}

		c.mu.Lock()
		dirtyNum := c.nextDirtyNum
		c.nextDirtyNum++

		_, err := c.dirtyFile.WriteAt(pageData, int64(dirtyNum)*int64(pageSize))
		if err != nil {
			c.mu.Unlock()
			return int(pos), fmt.Errorf("writing dirty page: %w", err)
		}

		c.dirtyPages[pageNum] = dirtyPageInfo{
			hash:     h,
			dirtyNum: dirtyNum,
		}
		c.mu.Unlock()

		if cache != nil {
			cache.Put(h, pageData)
		}
		c.server.writePages.Add(1)
		pos += n
	}
	return int(pos), nil
}

// handleTrim zeros the given byte range and reverts any fully-covered
// dirty pages to the base image. Partial pages at the start and end of
// the range are zero-filled via WriteAt.
func (c *Snapshot) handleTrim(offset, length uint64) error {
	end := offset + length
	pageSize := uint64(c.server.pageSize)

	// Round up to the first fully-covered page.
	firstFullPage := int64((offset + pageSize - 1) / pageSize)
	// Round down to the last fully-covered page (exclusive).
	lastFullPageExcl := int64(end / pageSize)

	// Zero partial start page.
	if startRem := offset % pageSize; startRem != 0 {
		n := min(pageSize-startRem, length)
		bufp := c.server.pageBufPool.Get().(*[]byte)
		clear((*bufp)[:n])
		_, err := c.WriteAt((*bufp)[:n], int64(offset))
		c.server.pageBufPool.Put(bufp)
		if err != nil {
			return err
		}
	}

	// Zero partial end page, if on a different page than the start.
	if endRem := end % pageSize; endRem != 0 && int64(end/pageSize) >= firstFullPage {
		bufp := c.server.pageBufPool.Get().(*[]byte)
		clear((*bufp)[:endRem])
		_, err := c.WriteAt((*bufp)[:endRem], int64(lastFullPageExcl)*int64(pageSize))
		c.server.pageBufPool.Put(bufp)
		if err != nil {
			return err
		}
	}

	// Delete fully-covered pages, reverting them to the base image.
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := firstFullPage; p < lastFullPageExcl; p++ {
		delete(c.dirtyPages, p)
	}
	return nil
}
