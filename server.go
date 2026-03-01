package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"tailscale.com/metrics"
	"tailscale.com/util/set"
)

// Server is the main guestbd NBD server.
type Server struct {
	mu       sync.Mutex
	filePath string
	pageSize int
	cache    *pageCache

	readonlyFiles map[inodeKey]*readonlyFile
	conns         set.Set[*Conn]

	activeConns atomic.Int64
	totalConns  atomic.Int64
	ops         metrics.LabelMap
}

// NewServer creates a new guestbd server.
func NewServer(filePath string, pageSize int, maxMem int64) *Server {
	maxPages := int(maxMem) / pageSize
	if maxPages < 1 {
		maxPages = 1
	}

	initZeroPageHash(pageSize)

	srv := &Server{
		filePath:      filePath,
		pageSize:      pageSize,
		cache:         newPageCache(maxPages, pageSize),
		readonlyFiles: make(map[inodeKey]*readonlyFile),
		conns:         make(set.Set[*Conn]),
		ops:           metrics.LabelMap{Label: "op"},
	}

	return srv
}

// getReadonlyFile returns the readonlyFile for the backing file,
// sharing it across connections with the same inode.
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
		ro.refcount++
		ro.mu.Unlock()
		f.Close()
		return ro, nil
	}

	ro := newReadonlyFile(f, fi.Size(), s.pageSize)
	s.readonlyFiles[key] = ro
	return ro, nil
}

// releaseReadonlyFile decrements the refcount and cleans up if zero.
func (s *Server) releaseReadonlyFile(ro *readonlyFile) {
	ro.mu.Lock()
	ro.refcount--
	rc := ro.refcount
	ro.mu.Unlock()

	if rc <= 0 {
		s.mu.Lock()
		for k, v := range s.readonlyFiles {
			if v == ro {
				delete(s.readonlyFiles, k)
				break
			}
		}
		s.mu.Unlock()
		ro.f.Close()
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
	rw := c.tcpConn

	// Phase 1: Newstyle handshake.
	var handshake [18]byte
	binary.BigEndian.PutUint64(handshake[0:8], nbdMagic)
	binary.BigEndian.PutUint64(handshake[8:16], nbdOptsMagic)
	binary.BigEndian.PutUint16(handshake[16:18], uint16(nbdFlagFixedNewstyle))
	if _, err := rw.Write(handshake[:]); err != nil {
		return fmt.Errorf("writing handshake: %w", err)
	}

	var clientFlagsBuf [4]byte
	if _, err := io.ReadFull(rw, clientFlagsBuf[:]); err != nil {
		return fmt.Errorf("reading client flags: %w", err)
	}
	cflags := binary.BigEndian.Uint32(clientFlagsBuf[:])
	noZeroes := cflags&nbdFlagCNoZeroes != 0

	// Phase 2: Option haggling.
	exportSize := uint64(c.roFile.size)
	txFlags := nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendTrim

	for {
		var optHeader [16]byte
		if _, err := io.ReadFull(rw, optHeader[:]); err != nil {
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
			if _, err := io.ReadFull(rw, optData); err != nil {
				return fmt.Errorf("reading option data: %w", err)
			}
		}

		switch optCode {
		case nbdOptExportName:
			var reply [10]byte
			binary.BigEndian.PutUint64(reply[0:8], exportSize)
			binary.BigEndian.PutUint16(reply[8:10], txFlags)
			if _, err := rw.Write(reply[:]); err != nil {
				return err
			}
			if !noZeroes {
				var zeros [124]byte
				if _, err := rw.Write(zeros[:]); err != nil {
					return err
				}
			}
			return s.serveTransmission(c)

		case nbdOptAbort:
			return s.sendOptReply(rw, optCode, nbdRepAck, nil)

		case nbdOptGo:
			// Send NBD_INFO_EXPORT.
			var infoData [12]byte
			binary.BigEndian.PutUint16(infoData[0:2], nbdInfoExport)
			binary.BigEndian.PutUint64(infoData[2:10], exportSize)
			binary.BigEndian.PutUint16(infoData[10:12], txFlags)
			if err := s.sendOptReply(rw, optCode, nbdRepInfo, infoData[:]); err != nil {
				return err
			}
			// Send NBD_INFO_BLOCK_SIZE.
			var bsData [14]byte
			binary.BigEndian.PutUint16(bsData[0:2], nbdInfoBlockSize)
			binary.BigEndian.PutUint32(bsData[2:6], 1)                   // minimum
			binary.BigEndian.PutUint32(bsData[6:10], uint32(s.pageSize)) // preferred
			binary.BigEndian.PutUint32(bsData[10:14], 32*1024*1024)      // maximum
			if err := s.sendOptReply(rw, optCode, nbdRepInfo, bsData[:]); err != nil {
				return err
			}
			// ACK
			if err := s.sendOptReply(rw, optCode, nbdRepAck, nil); err != nil {
				return err
			}
			return s.serveTransmission(c)

		case nbdOptList:
			// One export with empty name (default).
			var nameData [4]byte
			if err := s.sendOptReply(rw, optCode, nbdRepServer, nameData[:]); err != nil {
				return err
			}
			if err := s.sendOptReply(rw, optCode, nbdRepAck, nil); err != nil {
				return err
			}

		default:
			if err := s.sendOptReply(rw, optCode, nbdRepErrUnsup, nil); err != nil {
				return err
			}
		}
	}
}

// serveTransmission handles the NBD transmission phase.
func (s *Server) serveTransmission(c *Conn) error {
	rw := c.tcpConn

	for {
		var req nbdRequest
		if err := binary.Read(rw, binary.BigEndian, &req); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("reading request: %w", err)
		}
		if req.Magic != nbdRequestMagic {
			return fmt.Errorf("bad request magic: %#x", req.Magic)
		}

		switch req.Type {
		case nbdCmdRead:
			s.ops.Add("read", 1)
			data, err := c.handleRead(req.Offset, uint64(req.Length))
			if err != nil {
				if werr := s.sendReply(rw, req.Handle, nbdEIO, nil); werr != nil {
					return werr
				}
				continue
			}
			if err := s.sendReply(rw, req.Handle, 0, data); err != nil {
				return err
			}

		case nbdCmdWrite:
			s.ops.Add("write", 1)
			data := make([]byte, req.Length)
			if _, err := io.ReadFull(rw, data); err != nil {
				return fmt.Errorf("reading write data: %w", err)
			}
			if err := c.handleWrite(req.Offset, data); err != nil {
				if werr := s.sendReply(rw, req.Handle, nbdEIO, nil); werr != nil {
					return werr
				}
				continue
			}
			if err := s.sendReply(rw, req.Handle, 0, nil); err != nil {
				return err
			}

		case nbdCmdDisc:
			s.ops.Add("disconnect", 1)
			return nil

		case nbdCmdFlush:
			s.ops.Add("flush", 1)
			c.mu.Lock()
			err := c.dirtyFile.Sync()
			c.mu.Unlock()
			errCode := uint32(0)
			if err != nil {
				errCode = nbdEIO
			}
			if err := s.sendReply(rw, req.Handle, errCode, nil); err != nil {
				return err
			}

		case nbdCmdTrim:
			s.ops.Add("trim", 1)
			c.handleTrim(req.Offset, uint64(req.Length))
			if err := s.sendReply(rw, req.Handle, 0, nil); err != nil {
				return err
			}

		default:
			if err := s.sendReply(rw, req.Handle, nbdEINVAL, nil); err != nil {
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
