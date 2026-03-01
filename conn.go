package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

// Conn represents a single NBD client connection.
// Each connection has its own virtual read/write namespace:
// reads fall through to the shared readonlyFile, while writes
// are tracked per-connection and lost on disconnect.
type Conn struct {
	server  *Server
	tcpConn net.Conn
	roFile  *readonlyFile

	mu           sync.Mutex
	dirtyPages   map[int64]*dirtyPageInfo // pageNum => dirty info
	dirtyFile    *os.File                 // temp file for dirty page data
	nextDirtyNum int                      // monotonically increasing
}

// dirtyPageInfo tracks a written page for a connection.
type dirtyPageInfo struct {
	hash     pageHash
	dirtyNum int // index into the connection's dirtyFile
}

func newConn(srv *Server, tc net.Conn, ro *readonlyFile) (*Conn, error) {
	tmpFile, err := os.CreateTemp("", "guestbd-dirty-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	// Unlink immediately so it's cleaned up when the fd closes.
	os.Remove(tmpFile.Name())

	return &Conn{
		server:     srv,
		tcpConn:    tc,
		roFile:     ro,
		dirtyPages: make(map[int64]*dirtyPageInfo),
		dirtyFile:  tmpFile,
	}, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dirtyFile != nil {
		c.dirtyFile.Close()
		c.dirtyFile = nil
	}
	return nil
}

// readPageData reads a full page, checking dirty pages first,
// then falling back to the readonly file.
func (c *Conn) readPageData(pageNum int64) ([]byte, error) {
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
		// Read from the connection's dirty file.
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

// handleRead handles an NBD read request, assembling data from
// potentially multiple pages into dst.
func (c *Conn) handleRead(dst []byte, offset uint64) error {
	length := uint64(len(dst))
	c.server.readBytes.Add(int64(length))

	pageSize := uint64(c.server.pageSize)

	pos := uint64(0)
	for pos < length {
		absOffset := offset + pos
		pageNum := int64(absOffset / pageSize)
		pageOffset := absOffset % pageSize

		pageData, err := c.readPageData(pageNum)
		if err != nil {
			return err
		}

		n := pageSize - pageOffset
		if n > length-pos {
			n = length - pos
		}
		copy(dst[pos:pos+n], pageData[pageOffset:pageOffset+n])
		pos += n
	}
	return nil
}

// handleWrite handles an NBD write request. Sub-page writes trigger
// a read-modify-write cycle.
func (c *Conn) handleWrite(offset uint64, data []byte) error {
	length := uint64(len(data))
	pageSize := uint64(c.server.pageSize)

	c.server.writeBytes.Add(int64(length))

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
			copy(pageData, data[pos:pos+n])
		} else {
			// Partial page write: read-modify-write.
			existing, err := c.readPageData(pageNum)
			if err != nil {
				return err
			}
			pageData = make([]byte, pageSize)
			copy(pageData, existing)
			copy(pageData[pageOffset:], data[pos:pos+n])
		}

		h := hashPage(pageData)

		c.mu.Lock()
		dirtyNum := c.nextDirtyNum
		c.nextDirtyNum++

		_, err := c.dirtyFile.WriteAt(pageData, int64(dirtyNum)*int64(pageSize))
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("writing dirty page: %w", err)
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
	return nil
}

// handleTrim forgets dirty pages in the given range, reverting
// those pages to the base image.
func (c *Conn) handleTrim(offset, length uint64) {
	pageSize := uint64(c.server.pageSize)
	startPage := int64(offset / pageSize)
	endPage := int64((offset + length + pageSize - 1) / pageSize)

	c.mu.Lock()
	defer c.mu.Unlock()
	for p := startPage; p < endPage; p++ {
		delete(c.dirtyPages, p)
	}
}
