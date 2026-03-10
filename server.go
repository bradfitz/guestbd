package guestbd

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
	"strings"
	"sync"
	"syscall"

	"github.com/bradfitz/qcow2"
	"tailscale.com/metrics"
	"tailscale.com/util/set"
)

// BaseImage is a read-only random-access image with a known size.
// The server calls Close when the image is no longer needed.
// Implementations that return a non-nil key from BaseImageKey enable
// equivalence-keyed caching: idle baseImageStates whose key matches a
// newly opened image are reused, preserving the page hash table.
type BaseImage interface {
	io.ReaderAt
	io.Closer
	Size() int64
	BaseImageKey() any // nil = no coalescing; non-nil must be comparable
}

// BaseImageSource is a function that opens and returns the base image.
// It is called lazily when the first snapshot is created.
type BaseImageSource func() (BaseImage, error)

// NewBaseImage returns a BaseImage wrapping an io.ReaderAt with a fixed size.
// The returned BaseImage has no identity key (BaseImageKey returns nil),
// so idle baseImageStates using it will not be reused across connections.
func NewBaseImage(r io.ReaderAt, size int64) BaseImage {
	return &plainBaseImage{r: r, size: size}
}

type plainBaseImage struct {
	r    io.ReaderAt
	size int64
}

func (p *plainBaseImage) ReadAt(b []byte, off int64) (int, error) { return p.r.ReadAt(b, off) }
func (p *plainBaseImage) Size() int64                             { return p.size }
func (p *plainBaseImage) Close() error                            { return nil }
func (p *plainBaseImage) BaseImageKey() any                       { return nil }

// fileIdentityKey uniquely identifies an on-disk file by device and inode.
type fileIdentityKey struct {
	dev uint64
	ino uint64
}

// fileSizeReaderAt wraps an *os.File as a BaseImage.
type fileSizeReaderAt struct {
	f    *os.File
	size int64
	key  fileIdentityKey
}

func (r *fileSizeReaderAt) ReadAt(p []byte, off int64) (int, error) { return r.f.ReadAt(p, off) }
func (r *fileSizeReaderAt) Size() int64                             { return r.size }
func (r *fileSizeReaderAt) Close() error                            { return r.f.Close() }
func (r *fileSizeReaderAt) BaseImageKey() any                       { return r.key }

// qcow2SizeReaderAt wraps a *qcow2.Image as a BaseImage, holding the
// underlying *os.File so it can be closed.
type qcow2SizeReaderAt struct {
	img *qcow2.Image
	f   *os.File
	key fileIdentityKey
}

func (r *qcow2SizeReaderAt) ReadAt(p []byte, off int64) (int, error) { return r.img.ReadAt(p, off) }
func (r *qcow2SizeReaderAt) Size() int64                             { return r.img.Size() }
func (r *qcow2SizeReaderAt) Close() error                            { return r.f.Close() }
func (r *qcow2SizeReaderAt) BaseImageKey() any                       { return r.key }

// FileSource returns a BaseImageSource that opens the file at path.
// Files ending in ".qcow2" are opened as qcow2 images; all others are
// treated as raw disk images.
func FileSource(path string) BaseImageSource {
	return func() (BaseImage, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		st := fi.Sys().(*syscall.Stat_t)
		key := fileIdentityKey{dev: st.Dev, ino: st.Ino}
		if strings.HasSuffix(path, ".qcow2") {
			img, err := qcow2.Open(f)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("opening qcow2 image: %w", err)
			}
			return &qcow2SizeReaderAt{img, f, key}, nil
		}
		return &fileSizeReaderAt{f, fi.Size(), key}, nil
	}
}

// ServerOption configures optional Server parameters.
type ServerOption func(*serverConfig)

type serverConfig struct {
	pageSize       int
	maxMem         int64
	sharedSnapshot bool
	maxIdleBase    int
}

// WithPageSize sets the page size in bytes. It must be a positive power of two.
// The default is 4096.
func WithPageSize(n int) ServerOption {
	return func(c *serverConfig) { c.pageSize = n }
}

// WithMaxMem sets the maximum memory used by the shared page cache.
// The default is 1GB. A value of 0 disables page hashing and caching
// entirely; reads of non-dirty pages go directly to the base image.
func WithMaxMem(n int64) ServerOption {
	return func(c *serverConfig) { c.maxMem = n }
}

// WithSharedSnapshot makes all connections share a single writable Snapshot
// instead of each getting its own. This allows reconnecting clients to see
// writes from previous connections.
func WithSharedSnapshot() ServerOption {
	return func(c *serverConfig) { c.sharedSnapshot = true }
}

// WithMaxIdleBase sets the maximum number of idle base images to keep cached
// for equivalence-keyed reuse. When a baseImageState becomes idle (refcount 0)
// and its BaseImageKey is non-nil, it is kept in a cache of up to n entries.
// The default is 1.
func WithMaxIdleBase(n int) ServerOption {
	return func(c *serverConfig) { c.maxIdleBase = n }
}

// Server is an NBD server that serves a single backing image to multiple
// concurrent clients. Each client connection may get an independent
// copy-on-write Snapshot, or multiple connections may share a single Snapshot
// to allow reconnection to a previous writable state. The snapshot policy is
// configured via WithSharedSnapshot at construction time.
type Server struct {
	// Immutable after construction:
	getBase           BaseImageSource // immutable
	pageSize          int             // immutable
	zeroPageHash      pageHash        // immutable; sha256 of an all-zero page
	cache             *pageCache      // immutable; nil when WithMaxMem(0) disables caching
	useSharedSnapshot bool            // immutable; set by WithSharedSnapshot
	maxIdleBase       int             // immutable; max idle entries in roFiles

	mu          sync.Mutex
	sharedSnap  *Snapshot                // lazily created when useSharedSnapshot is true
	roFiles     map[any]*baseImageState  // identity-keyed entries (active + idle)
	roFileNoKey *baseImageState          // single entry for nil-key sources
	nextIdleSeq int64                    // monotonic counter for idle LRU ordering
	snapshots   set.Set[*Snapshot]

	// Metrics (atomic, no mutex needed):
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

// NewServer creates a new Server that serves the base image returned by getBase.
func NewServer(getBase BaseImageSource, opts ...ServerOption) *Server {
	cfg := serverConfig{
		pageSize: 4096,
		maxMem:   1 << 30, // 1GB default
	}
	for _, o := range opts {
		o(&cfg)
	}

	pageSize := cfg.pageSize

	var cache *pageCache
	if cfg.maxMem > 0 {
		maxPages := int(cfg.maxMem) / pageSize
		if maxPages < 1 {
			maxPages = 1
		}
		cache = newPageCache(maxPages, pageSize)
	}

	// Powers of two from 1 to 4MB.
	var sizeBuckets []float64
	for v := float64(1); v <= 4*1024*1024; v *= 2 {
		sizeBuckets = append(sizeBuckets, v)
	}

	maxIdleBase := cfg.maxIdleBase
	if maxIdleBase == 0 {
		maxIdleBase = 1
	}

	srv := &Server{
		getBase:           getBase,
		pageSize:          pageSize,
		useSharedSnapshot: cfg.sharedSnapshot,
		cache:             cache,
		roFiles:           make(map[any]*baseImageState),
		maxIdleBase:       maxIdleBase,
		snapshots:         make(set.Set[*Snapshot]),
		ops:               metrics.LabelMap{Label: "type"},
		readPath:          metrics.LabelMap{Label: "type"},
		readSizeHist:      metrics.NewHistogram(sizeBuckets),
		writeSizeHist:     metrics.NewHistogram(sizeBuckets),
	}
	if cache != nil {
		srv.zeroPageHash = hashPage(make([]byte, pageSize))
	}

	return srv
}

// InitExpvar publishes the server's metrics to expvar. It should be called
// once before serving. It is not called in tests to avoid duplicate
// registration panics.
func (s *Server) InitExpvar() {
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
	if s.cache != nil {
		expvar.Publish("counter_guestbd_cache", &s.cache.path)
		expvar.Publish("gauge_guestbd_cache_entries", &s.cache.entries)
		expvar.Publish("gauge_guestbd_cache_bytes", &s.cache.bytes)
	}

	expvar.Publish("gauge_guestbd_base_images_active", &s.baseImagesActive)
	expvar.Publish("gauge_guestbd_base_images_cached", &s.baseImagesCached)

	ps := new(expvar.Int)
	ps.Set(int64(s.pageSize))
	expvar.Publish("gauge_guestbd_page_size", ps)

	expvar.Publish("gauge_guestbd_max_dirty_bytes", expvar.Func(func() any {
		s.mu.Lock()
		defer s.mu.Unlock()

		var max int64
		for c := range s.snapshots {
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

// getBaseImageState returns a baseImageState for the backing image.
// getBase is called on every invocation. If there is already an active or
// idle baseImageState with a matching BaseImageKey, the newly opened base is
// closed and the existing baseImageState is reused (preserving its page hash
// table). Sources returning nil keys share while active but are replaced
// when idle.
func (s *Server) getBaseImageState() (*baseImageState, error) {
	base, err := s.getBase()
	if err != nil {
		return nil, err
	}

	key := base.BaseImageKey()

	s.mu.Lock()
	defer s.mu.Unlock()

	if key != nil {
		if ro, ok := s.roFiles[key]; ok {
			// Found a matching entry (active or idle). Close the new base
			// and bump refcount.
			base.Close()
			ro.mu.Lock()
			wasIdle := ro.refcount == 0
			ro.refcount++
			ro.mu.Unlock()
			if wasIdle {
				s.baseImagesActive.Add(1)
				ro.idleSince = 0
			}
			return ro, nil
		}
		// Not found — create new and insert.
		ro := newBaseImageState(s, base, key)
		s.roFiles[key] = ro
		s.baseImagesActive.Add(1)
		s.baseImagesCached.Add(1)
		s.evictIdleLocked()
		return ro, nil
	}

	// key == nil
	if ro := s.roFileNoKey; ro != nil {
		ro.mu.Lock()
		rc := ro.refcount
		ro.mu.Unlock()
		if rc > 0 {
			// Active — share.
			base.Close()
			ro.mu.Lock()
			ro.refcount++
			ro.mu.Unlock()
			return ro, nil
		}
		// Idle — close old base and replace.
		ro.base.Close()
		s.baseImagesCached.Add(-1)
	}

	ro := newBaseImageState(s, base, nil)
	s.roFileNoKey = ro
	s.baseImagesActive.Add(1)
	s.baseImagesCached.Add(1)
	return ro, nil
}

// releaseBaseImageState decrements the refcount. When it reaches zero the
// baseImagesActive metric is updated. For keyed entries, the baseImageState
// stays in the roFiles map as an idle entry (subject to LRU eviction).
// For nil-key entries, it stays in roFileNoKey and is replaced next call.
func (s *Server) releaseBaseImageState(ro *baseImageState) {
	ro.mu.Lock()
	ro.refcount--
	rc := ro.refcount
	ro.mu.Unlock()

	if rc > 0 {
		return
	}

	s.baseImagesActive.Add(-1)

	s.mu.Lock()
	defer s.mu.Unlock()

	if ro.identityKey != nil {
		s.nextIdleSeq++
		ro.idleSince = s.nextIdleSeq
		s.evictIdleLocked()
	}
}

// evictIdleLocked removes the oldest idle entries from s.roFiles until
// the number of idle entries is at most s.maxIdleBase.
func (s *Server) evictIdleLocked() {
	for {
		var (
			idleCount   int
			oldestKey   any
			oldestSeq   int64
			oldestEntry *baseImageState
		)
		for k, ro := range s.roFiles {
			ro.mu.Lock()
			rc := ro.refcount
			ro.mu.Unlock()
			if rc == 0 {
				idleCount++
				if oldestEntry == nil || ro.idleSince < oldestSeq {
					oldestKey = k
					oldestSeq = ro.idleSince
					oldestEntry = ro
				}
			}
		}
		if idleCount <= s.maxIdleBase {
			return
		}
		// Evict oldest idle.
		oldestEntry.base.Close()
		delete(s.roFiles, oldestKey)
		s.baseImagesCached.Add(-1)
	}
}

// NewSnapshot creates a new writable Snapshot backed by the server's base
// image. The caller is responsible for calling Close when the snapshot is no
// longer needed.
func (s *Server) NewSnapshot() (*Snapshot, error) {
	ro, err := s.getBaseImageState()
	if err != nil {
		return nil, fmt.Errorf("opening base image: %w", err)
	}
	snap, err := newSnapshot(s, ro)
	if err != nil {
		s.releaseBaseImageState(ro)
		return nil, err
	}
	s.mu.Lock()
	s.snapshots.Add(snap)
	s.mu.Unlock()
	return snap, nil
}

// Close cleans up server resources, including any shared Snapshot created
// via WithSharedSnapshot and all cached baseImageStates.
func (s *Server) Close() error {
	s.mu.Lock()
	snap := s.sharedSnap
	s.sharedSnap = nil
	roFiles := s.roFiles
	s.roFiles = nil
	roNoKey := s.roFileNoKey
	s.roFileNoKey = nil
	s.mu.Unlock()
	if snap != nil {
		snap.Close()
	}
	for _, ro := range roFiles {
		ro.base.Close()
	}
	if roNoKey != nil {
		roNoKey.base.Close()
	}
	return nil
}

// getOrCreateSnapshot returns a snapshot for a new connection. If the server
// is configured with WithSharedSnapshot, it lazily creates a single shared
// snapshot and returns it with owned=false. Otherwise it creates a fresh
// per-connection snapshot with owned=true. The caller must close the snapshot
// when owned is true.
func (s *Server) getOrCreateSnapshot() (snap *Snapshot, owned bool, err error) {
	if !s.useSharedSnapshot {
		snap, err = s.NewSnapshot()
		return snap, true, err
	}

	s.mu.Lock()
	snap = s.sharedSnap
	s.mu.Unlock()
	if snap != nil {
		return snap, false, nil
	}

	// Lazy creation; must not hold s.mu across NewSnapshot.
	snap, err = s.NewSnapshot()
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	if s.sharedSnap != nil {
		// Another goroutine created it first.
		s.mu.Unlock()
		snap.Close()
		return s.sharedSnap, false, nil
	}
	s.sharedSnap = snap
	s.mu.Unlock()
	return snap, false, nil
}

// Serve accepts incoming connections on the listener ln, handling
// each one on a new goroutine. Serve blocks until the listener
// returns an error. The caller is responsible for closing ln.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.HandleConn(conn)
	}
}

// HandleConn handles a new TCP connection, performing the NBD
// handshake and entering the transmission phase. The snapshot policy
// (per-connection or shared) is determined by the server's configuration.
func (s *Server) HandleConn(nc net.Conn) {
	s.totalConns.Add(1)
	s.activeConns.Add(1)
	defer s.activeConns.Add(-1)

	snap, owned, err := s.getOrCreateSnapshot()
	if err != nil {
		log.Printf("create snapshot for %v: %v", nc.RemoteAddr(), err)
		nc.Close()
		return
	}

	defer func() {
		if owned {
			snap.Close()
		}
		nc.Close()
	}()

	if err := s.serveNBD(snap, nc); err != nil {
		log.Printf("nbd %v: %v", nc.RemoteAddr(), err)
	}
}

// serveNBD runs the NBD protocol on the given connection.
func (s *Server) serveNBD(snap *Snapshot, nc net.Conn) error {
	r := nc
	bw := bufio.NewWriterSize(nc, 1<<20) // 1MB

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
	exportSize := uint64(snap.roFile.size)
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
			return s.serveTransmission(snap, r, bw)

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
			return s.serveTransmission(snap, r, bw)

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
func (s *Server) serveTransmission(snap *Snapshot, r io.Reader, bw *bufio.Writer) error {
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
			if _, err := snap.ReadAt(buf, int64(req.Offset)); err != nil {
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
			if _, err := snap.WriteAt(buf, int64(req.Offset)); err != nil {
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
			snap.handleTrim(req.Offset, uint64(req.Length))
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

// sendOptReply writes an NBD option reply with the given option code, reply
// type, and optional payload data.
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

// sendReply writes an NBD transmission-phase reply with the given handle,
// error code, and optional payload data.
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
