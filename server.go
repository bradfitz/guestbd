package main

import (
	"bufio"
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"sync"

	"tailscale.com/metrics"
	"tailscale.com/util/set"
)

// Server is the main guestbd NBD server.
type Server struct {
	mu           sync.Mutex
	filePath     string
	pageSize     int
	zeroPageHash pageHash // sha256 of a page filled entirely with zero bytes
	cache        *pageCache

	readonlyFiles map[inodeKey]*readonlyFile
	conns         set.Set[*Conn]

	totalConns       expvar.Int         // counter_guestbd_total_conns
	activeConns      expvar.Int         // gauge_guestbd_active_conns
	ops              metrics.LabelMap   // counter_guestbd_nbd_ops{type=...}
	readPath         metrics.LabelMap   // counter_guestbd_read_path{type=...}
	readBytes        expvar.Int         // counter_guestbd_read_bytes
	readPages        expvar.Int         // counter_guestbd_read_pages
	writeBytes       expvar.Int         // counter_guestbd_write_bytes
	writePages       expvar.Int         // counter_guestbd_write_pages
	readSizeHist     *metrics.Histogram // histogram_guestbd_read_size_bytes
	writeSizeHist    *metrics.Histogram // histogram_guestbd_write_size_bytes
	baseImagesActive expvar.Int         // gauge_guestbd_base_images_active
	baseImagesCached expvar.Int         // gauge_guestbd_base_images_cached
}

// NewServer creates a new guestbd server.
func NewServer(filePath string, pageSize int, maxMem int64) *Server {
	maxPages := int(maxMem) / pageSize
	if maxPages < 1 {
		maxPages = 1
	}

	// Powers of two from 1 to 4MB.
	var sizeBuckets []float64
	for v := float64(1); v <= 4*1024*1024; v *= 2 {
		sizeBuckets = append(sizeBuckets, v)
	}

	srv := &Server{
		filePath:      filePath,
		pageSize:      pageSize,
		zeroPageHash:  hashPage(make([]byte, pageSize)),
		cache:         newPageCache(maxPages, pageSize),
		readonlyFiles: make(map[inodeKey]*readonlyFile),
		conns:         make(set.Set[*Conn]),
		ops:           metrics.LabelMap{Label: "type"},
		readPath:      metrics.LabelMap{Label: "type"},
		readSizeHist:  metrics.NewHistogram(sizeBuckets),
		writeSizeHist: metrics.NewHistogram(sizeBuckets),
	}

	return srv
}

// initExpvar publishes the server's metrics to expvar.
// Called once from main; not called in tests to avoid duplicate registration panics.
func (s *Server) initExpvar() {
	expvar.Publish("counter_guestbd_total_conns", &s.totalConns)
	expvar.Publish("gauge_guestbd_active_conns", &s.activeConns)
	expvar.Publish("counter_guestbd_nbd_ops", &s.ops)
	expvar.Publish("counter_guestbd_read_path", &s.readPath)
	expvar.Publish("counter_guestbd_read_bytes", &s.readBytes)
	expvar.Publish("counter_guestbd_read_pages", &s.readPages)
	expvar.Publish("counter_guestbd_write_bytes", &s.writeBytes)
	expvar.Publish("counter_guestbd_write_pages", &s.writePages)
	expvar.Publish("histogram_guestbd_read_size_bytes", s.readSizeHist)
	expvar.Publish("histogram_guestbd_write_size_bytes", s.writeSizeHist)
	expvar.Publish("counter_guestbd_cache", &s.cache.path)
	expvar.Publish("gauge_guestbd_cache_entries", &s.cache.entries)
	expvar.Publish("gauge_guestbd_cache_bytes", &s.cache.bytes)

	expvar.Publish("gauge_guestbd_base_images_active", &s.baseImagesActive)
	expvar.Publish("gauge_guestbd_base_images_cached", &s.baseImagesCached)

	ps := new(expvar.Int)
	ps.Set(int64(s.pageSize))
	expvar.Publish("gauge_guestbd_page_size", ps)

	expvar.Publish("gauge_guestbd_max_dirty_bytes", expvar.Func(func() any {
		s.mu.Lock()
		defer s.mu.Unlock()

		var max int64
		for c := range s.conns {
			c.mu.Lock()
			n := int64(len(c.dirtyPages)) * int64(s.pageSize)
			c.mu.Unlock()
			if n > max {
				max = n
			}
		}
		return max
	}))
}

// getReadonlyFile returns the readonlyFile for the backing file,
// sharing it across connections with the same inode. The readonlyFile
// and its pageHashes table persist even when refcount drops to zero,
// so that subsequent connections to the same inode benefit from the
// already-computed hash table (which is the index into the LRU cache).
func (s *Server) getReadonlyFile() (*readonlyFile, error) {
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	key := fileInodeKey(fi)

	s.mu.Lock()
	defer s.mu.Unlock()

	if ro, ok := s.readonlyFiles[key]; ok {
		ro.mu.Lock()
		wasIdle := ro.refcount == 0
		ro.refcount++
		ro.mu.Unlock()
		if wasIdle {
			s.baseImagesActive.Add(1)
		}
		f.Close()
		return ro, nil
	}

	ro := newReadonlyFile(s, f, fi.Size())
	s.readonlyFiles[key] = ro
	s.baseImagesActive.Add(1) // new entry starts with refcount 1
	s.baseImagesCached.Add(1)
	return ro, nil
}

// releaseReadonlyFile decrements the refcount. If the refcount reaches
// zero, it checks whether the file on disk still points to the same
// inode. If the file is gone or has a different inode, the readonlyFile
// is removed and its fd closed — no future client can use it. Otherwise
// it stays in the map so the pageHashes index remains available for the
// next connection.
func (s *Server) releaseReadonlyFile(ro *readonlyFile) {
	ro.mu.Lock()
	ro.refcount--
	rc := ro.refcount
	ro.mu.Unlock()

	if rc > 0 {
		return
	}

	s.baseImagesActive.Add(-1)

	// Check whether the file on disk still matches this inode.
	fi, err := os.Stat(s.filePath)
	if err != nil || fileInodeKey(fi) != fileInodeKey(ro.fileInfo()) {
		// File is gone or replaced; no future client will ever
		// open this inode. Clean it up.
		s.mu.Lock()
		for k, v := range s.readonlyFiles {
			if v == ro {
				delete(s.readonlyFiles, k)
				break
			}
		}
		s.mu.Unlock()
		ro.f.Close()
		s.baseImagesCached.Add(-1)
	}
}

// HandleConn handles a new TCP connection, performing the NBD
// handshake and entering the transmission phase.
func (s *Server) HandleConn(nc net.Conn) {
	s.totalConns.Add(1)
	s.activeConns.Add(1)
	defer s.activeConns.Add(-1)

	ro, err := s.getReadonlyFile()
	if err != nil {
		log.Printf("open backing file for %v: %v", nc.RemoteAddr(), err)
		nc.Close()
		return
	}

	c, err := newConn(s, nc, ro)
	if err != nil {
		log.Printf("create conn for %v: %v", nc.RemoteAddr(), err)
		s.releaseReadonlyFile(ro)
		nc.Close()
		return
	}

	s.mu.Lock()
	s.conns.Add(c)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.conns.Delete(c)
		s.mu.Unlock()
		c.Close()
		s.releaseReadonlyFile(ro)
		nc.Close()
	}()

	if err := s.serveNBD(c); err != nil {
		log.Printf("nbd %v: %v", nc.RemoteAddr(), err)
	}
}

// serveNBD runs the NBD protocol on the given connection.
func (s *Server) serveNBD(c *Conn) error {
	r := c.tcpConn
	bw := bufio.NewWriterSize(c.tcpConn, 1<<20) // 1MB

	// Phase 1: Newstyle handshake.
	var handshake [18]byte
	binary.BigEndian.PutUint64(handshake[0:8], nbdMagic)
	binary.BigEndian.PutUint64(handshake[8:16], nbdOptsMagic)
	binary.BigEndian.PutUint16(handshake[16:18], uint16(nbdFlagFixedNewstyle))
	if _, err := bw.Write(handshake[:]); err != nil {
		return fmt.Errorf("writing handshake: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flushing handshake: %w", err)
	}

	var clientFlagsBuf [4]byte
	if _, err := io.ReadFull(r, clientFlagsBuf[:]); err != nil {
		return fmt.Errorf("reading client flags: %w", err)
	}
	cflags := binary.BigEndian.Uint32(clientFlagsBuf[:])
	noZeroes := cflags&nbdFlagCNoZeroes != 0

	// Phase 2: Option haggling.
	exportSize := uint64(c.roFile.size)
	txFlags := nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendTrim

	for {
		var optHeader [16]byte
		if _, err := io.ReadFull(r, optHeader[:]); err != nil {
			return fmt.Errorf("reading option: %w", err)
		}
		optMagic := binary.BigEndian.Uint64(optHeader[0:8])
		if optMagic != nbdOptsMagic {
			return fmt.Errorf("bad option magic: %#x", optMagic)
		}
		optCode := binary.BigEndian.Uint32(optHeader[8:12])
		optLen := binary.BigEndian.Uint32(optHeader[12:16])

		optData := make([]byte, optLen)
		if optLen > 0 {
			if _, err := io.ReadFull(r, optData); err != nil {
				return fmt.Errorf("reading option data: %w", err)
			}
		}

		switch optCode {
		case nbdOptExportName:
			var reply [10]byte
			binary.BigEndian.PutUint64(reply[0:8], exportSize)
			binary.BigEndian.PutUint16(reply[8:10], txFlags)
			if _, err := bw.Write(reply[:]); err != nil {
				return err
			}
			if !noZeroes {
				var zeros [124]byte
				if _, err := bw.Write(zeros[:]); err != nil {
					return err
				}
			}
			if err := bw.Flush(); err != nil {
				return err
			}
			return s.serveTransmission(c, r, bw)

		case nbdOptAbort:
			if err := s.sendOptReply(bw, optCode, nbdRepAck, nil); err != nil {
				return err
			}
			return bw.Flush()

		case nbdOptGo:
			// Send NBD_INFO_EXPORT.
			var infoData [12]byte
			binary.BigEndian.PutUint16(infoData[0:2], nbdInfoExport)
			binary.BigEndian.PutUint64(infoData[2:10], exportSize)
			binary.BigEndian.PutUint16(infoData[10:12], txFlags)
			if err := s.sendOptReply(bw, optCode, nbdRepInfo, infoData[:]); err != nil {
				return err
			}
			// Send NBD_INFO_BLOCK_SIZE.
			var bsData [14]byte
			binary.BigEndian.PutUint16(bsData[0:2], nbdInfoBlockSize)
			binary.BigEndian.PutUint32(bsData[2:6], 1)                   // minimum
			binary.BigEndian.PutUint32(bsData[6:10], uint32(s.pageSize)) // preferred
			binary.BigEndian.PutUint32(bsData[10:14], 32*1024*1024)      // maximum
			if err := s.sendOptReply(bw, optCode, nbdRepInfo, bsData[:]); err != nil {
				return err
			}
			// ACK
			if err := s.sendOptReply(bw, optCode, nbdRepAck, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
			return s.serveTransmission(c, r, bw)

		case nbdOptList:
			// One export with empty name (default).
			var nameData [4]byte
			if err := s.sendOptReply(bw, optCode, nbdRepServer, nameData[:]); err != nil {
				return err
			}
			if err := s.sendOptReply(bw, optCode, nbdRepAck, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}

		default:
			if err := s.sendOptReply(bw, optCode, nbdRepErrUnsup, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
}

// serveTransmission handles the NBD transmission phase.
func (s *Server) serveTransmission(c *Conn, r io.Reader, bw *bufio.Writer) error {
	var buf []byte

	for {
		var req nbdRequest
		if err := binary.Read(r, binary.BigEndian, &req); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("reading request: %w", err)
		}
		if req.Magic != nbdRequestMagic {
			return fmt.Errorf("bad request magic: %#x", req.Magic)
		}

		if (req.Type == nbdCmdRead || req.Type == nbdCmdWrite) && req.Length > nbdMaxPayload {
			return fmt.Errorf("request length %d exceeds NBD max payload %d", req.Length, nbdMaxPayload)
		}

		switch req.Type {
		case nbdCmdRead:
			s.ops.Add("read", 1)
			s.readSizeHist.Observe(float64(req.Length))
			buf = slices.Grow(buf[:0], int(req.Length))[:req.Length]
			if err := c.handleRead(buf, req.Offset); err != nil {
				if werr := s.sendReply(bw, req.Handle, nbdEIO, nil); werr != nil {
					return werr
				}
				if werr := bw.Flush(); werr != nil {
					return werr
				}
				continue
			}
			if err := s.sendReply(bw, req.Handle, 0, buf); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}

		case nbdCmdWrite:
			s.ops.Add("write", 1)
			s.writeSizeHist.Observe(float64(req.Length))
			buf = slices.Grow(buf[:0], int(req.Length))[:req.Length]
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("reading write data: %w", err)
			}
			if err := c.handleWrite(req.Offset, buf); err != nil {
				if werr := s.sendReply(bw, req.Handle, nbdEIO, nil); werr != nil {
					return werr
				}
				if werr := bw.Flush(); werr != nil {
					return werr
				}
				continue
			}
			if err := s.sendReply(bw, req.Handle, 0, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}

		case nbdCmdDisc:
			s.ops.Add("disconnect", 1)
			return nil

		case nbdCmdFlush:
			s.ops.Add("flush", 1)
			// No-op: dirty data is ephemeral and lost on disconnect,
			// so there's no point in syncing to disk for guest VM performance.
			if err := s.sendReply(bw, req.Handle, 0, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}

		case nbdCmdTrim:
			s.ops.Add("trim", 1)
			c.handleTrim(req.Offset, uint64(req.Length))
			if err := s.sendReply(bw, req.Handle, 0, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}

		default:
			if err := s.sendReply(bw, req.Handle, nbdEINVAL, nil); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
}

func (s *Server) sendOptReply(w io.Writer, optCode uint32, replyType uint32, data []byte) error {
	var header [20]byte
	binary.BigEndian.PutUint64(header[0:8], nbdOptReplyMagic)
	binary.BigEndian.PutUint32(header[8:12], optCode)
	binary.BigEndian.PutUint32(header[12:16], replyType)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(data)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) sendReply(w io.Writer, handle uint64, errCode uint32, data []byte) error {
	var header [16]byte
	binary.BigEndian.PutUint32(header[0:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(header[4:8], errCode)
	binary.BigEndian.PutUint64(header[8:16], handle)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}
