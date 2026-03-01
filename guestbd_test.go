package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
)

// testNBDClient is a pure Go NBD client for testing.
type testNBDClient struct {
	t          *testing.T
	conn       net.Conn
	exportSize uint64
	nextHandle uint64
}

func newTestClient(t *testing.T, addr string) *testNBDClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := &testNBDClient{t: t, conn: conn}
	c.handshake()
	return c
}

func (c *testNBDClient) handshake() {
	c.t.Helper()

	// Read server handshake: magic(8) + opts_magic(8) + flags(2)
	var handshake [18]byte
	if _, err := io.ReadFull(c.conn, handshake[:]); err != nil {
		c.t.Fatalf("read handshake: %v", err)
	}
	magic := binary.BigEndian.Uint64(handshake[0:8])
	if magic != nbdMagic {
		c.t.Fatalf("bad magic: %#x", magic)
	}
	optsMagic := binary.BigEndian.Uint64(handshake[8:16])
	if optsMagic != nbdOptsMagic {
		c.t.Fatalf("bad opts magic: %#x", optsMagic)
	}

	// Send client flags (fixed newstyle + no zeroes).
	var clientFlags [4]byte
	binary.BigEndian.PutUint32(clientFlags[:], uint32(nbdFlagCFixedNewstyle|nbdFlagCNoZeroes))
	if _, err := c.conn.Write(clientFlags[:]); err != nil {
		c.t.Fatalf("write client flags: %v", err)
	}

	// Send NBD_OPT_GO with empty export name.
	var optBuf [22]byte
	binary.BigEndian.PutUint64(optBuf[0:8], nbdOptsMagic)
	binary.BigEndian.PutUint32(optBuf[8:12], nbdOptGo)
	binary.BigEndian.PutUint32(optBuf[12:16], 6) // data length: name_len(4) + name(0) + info_count(2)
	binary.BigEndian.PutUint32(optBuf[16:20], 0) // name length
	binary.BigEndian.PutUint16(optBuf[20:22], 0) // number of info requests
	if _, err := c.conn.Write(optBuf[:]); err != nil {
		c.t.Fatalf("write opt go: %v", err)
	}

	// Read option replies until ACK.
	for {
		var replyHeader [20]byte
		if _, err := io.ReadFull(c.conn, replyHeader[:]); err != nil {
			c.t.Fatalf("read opt reply: %v", err)
		}
		replyMagic := binary.BigEndian.Uint64(replyHeader[0:8])
		if replyMagic != nbdOptReplyMagic {
			c.t.Fatalf("bad opt reply magic: %#x", replyMagic)
		}
		replyType := binary.BigEndian.Uint32(replyHeader[12:16])
		replyLen := binary.BigEndian.Uint32(replyHeader[16:20])

		replyData := make([]byte, replyLen)
		if replyLen > 0 {
			if _, err := io.ReadFull(c.conn, replyData); err != nil {
				c.t.Fatalf("read opt reply data: %v", err)
			}
		}

		if replyType == nbdRepInfo && replyLen >= 12 {
			infoType := binary.BigEndian.Uint16(replyData[0:2])
			if infoType == nbdInfoExport {
				c.exportSize = binary.BigEndian.Uint64(replyData[2:10])
			}
		}

		if replyType == nbdRepAck {
			break
		}
		if replyType&(1<<31) != 0 {
			c.t.Fatalf("opt reply error: type=%#x", replyType)
		}
	}
}

func (c *testNBDClient) read(offset uint64, length uint32) []byte {
	c.t.Helper()
	handle := c.nextHandle
	c.nextHandle++

	var req [28]byte
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdRead))
	binary.BigEndian.PutUint64(req[8:16], handle)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], length)
	if _, err := c.conn.Write(req[:]); err != nil {
		c.t.Fatalf("write read request: %v", err)
	}

	var reply [16]byte
	if _, err := io.ReadFull(c.conn, reply[:]); err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	replyMagic := binary.BigEndian.Uint32(reply[0:4])
	if replyMagic != nbdReplyMagic {
		c.t.Fatalf("bad reply magic: %#x", replyMagic)
	}
	errCode := binary.BigEndian.Uint32(reply[4:8])
	if errCode != 0 {
		c.t.Fatalf("read error: %d", errCode)
	}
	replyHandle := binary.BigEndian.Uint64(reply[8:16])
	if replyHandle != handle {
		c.t.Fatalf("handle mismatch: got %d want %d", replyHandle, handle)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		c.t.Fatalf("read data: %v", err)
	}
	return data
}

func (c *testNBDClient) write(offset uint64, data []byte) {
	c.t.Helper()
	handle := c.nextHandle
	c.nextHandle++

	var req [28]byte
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdWrite))
	binary.BigEndian.PutUint64(req[8:16], handle)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], uint32(len(data)))
	if _, err := c.conn.Write(req[:]); err != nil {
		c.t.Fatalf("write request: %v", err)
	}
	if _, err := c.conn.Write(data); err != nil {
		c.t.Fatalf("write data: %v", err)
	}

	var reply [16]byte
	if _, err := io.ReadFull(c.conn, reply[:]); err != nil {
		c.t.Fatalf("read write reply: %v", err)
	}
	replyMagic := binary.BigEndian.Uint32(reply[0:4])
	if replyMagic != nbdReplyMagic {
		c.t.Fatalf("bad reply magic: %#x", replyMagic)
	}
	errCode := binary.BigEndian.Uint32(reply[4:8])
	if errCode != 0 {
		c.t.Fatalf("write error: %d", errCode)
	}
}

func (c *testNBDClient) trim(offset uint64, length uint32) {
	c.t.Helper()
	handle := c.nextHandle
	c.nextHandle++

	var req [28]byte
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdTrim))
	binary.BigEndian.PutUint64(req[8:16], handle)
	binary.BigEndian.PutUint64(req[16:24], offset)
	binary.BigEndian.PutUint32(req[24:28], length)
	if _, err := c.conn.Write(req[:]); err != nil {
		c.t.Fatalf("write trim request: %v", err)
	}

	var reply [16]byte
	if _, err := io.ReadFull(c.conn, reply[:]); err != nil {
		c.t.Fatalf("read trim reply: %v", err)
	}
	errCode := binary.BigEndian.Uint32(reply[4:8])
	if errCode != 0 {
		c.t.Fatalf("trim error: %d", errCode)
	}
}

func (c *testNBDClient) flush() {
	c.t.Helper()
	handle := c.nextHandle
	c.nextHandle++

	var req [28]byte
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdFlush))
	binary.BigEndian.PutUint64(req[8:16], handle)
	if _, err := c.conn.Write(req[:]); err != nil {
		c.t.Fatalf("write flush request: %v", err)
	}

	var reply [16]byte
	if _, err := io.ReadFull(c.conn, reply[:]); err != nil {
		c.t.Fatalf("read flush reply: %v", err)
	}
	errCode := binary.BigEndian.Uint32(reply[4:8])
	if errCode != 0 {
		c.t.Fatalf("flush error: %d", errCode)
	}
}

func (c *testNBDClient) disconnect() {
	handle := c.nextHandle
	c.nextHandle++

	var req [28]byte
	binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(req[4:6], 0)
	binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdDisc))
	binary.BigEndian.PutUint64(req[8:16], handle)
	c.conn.Write(req[:])
	c.conn.Close()
}

// startTestServer creates a temp file, starts a server, and returns
// the address, server, and a cleanup function.
func startTestServer(t *testing.T, fileData []byte, pageSize int) (addr string, srv *Server, cleanup func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "guestbd-test-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmpFile.Write(fileData); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	tmpFile.Close()

	srv = NewServer(tmpFile.Name(), pageSize, int64(pageSize)*256)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.HandleConn(conn)
		}
	}()

	return ln.Addr().String(), srv, func() {
		ln.Close()
		os.Remove(tmpFile.Name())
	}
}

func TestReadBasic(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*4)
	for i := range data {
		data[i] = byte(i % 251)
	}

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	if c.exportSize != uint64(len(data)) {
		t.Fatalf("export size: got %d, want %d", c.exportSize, len(data))
	}

	// Read full first page.
	got := c.read(0, pageSize)
	if !bytes.Equal(got, data[:pageSize]) {
		t.Fatal("first page mismatch")
	}

	// Read across page boundary.
	off := uint64(pageSize - 100)
	got = c.read(off, 200)
	if !bytes.Equal(got, data[off:off+200]) {
		t.Fatal("cross-page read mismatch")
	}

	// Read last page.
	off = uint64(pageSize * 3)
	got = c.read(off, pageSize)
	if !bytes.Equal(got, data[off:off+pageSize]) {
		t.Fatal("last page mismatch")
	}
}

func TestWriteAndReadBack(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*4)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Write a full page.
	writeData := make([]byte, pageSize)
	for i := range writeData {
		writeData[i] = 0xAB
	}
	c.write(0, writeData)

	// Read it back.
	got := c.read(0, pageSize)
	if !bytes.Equal(got, writeData) {
		t.Fatal("written page mismatch")
	}

	// Second page should still be zeros.
	got = c.read(pageSize, pageSize)
	if !bytes.Equal(got, make([]byte, pageSize)) {
		t.Fatal("unwritten page should be zeros")
	}
}

func TestSubPageWrite(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Write 100 bytes in the middle of the page.
	patch := bytes.Repeat([]byte{0xFF}, 100)
	c.write(200, patch)

	// Read back full page.
	got := c.read(0, pageSize)

	// Build expected result.
	expected := make([]byte, pageSize)
	copy(expected, data)
	copy(expected[200:], patch)
	if !bytes.Equal(got, expected) {
		t.Fatal("sub-page write mismatch")
	}
}

func TestCrossPageWrite(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*2)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Write data spanning page boundary.
	writeData := make([]byte, 200)
	for i := range writeData {
		writeData[i] = byte(i)
	}
	c.write(uint64(pageSize-100), writeData)

	// Read back the region.
	got := c.read(uint64(pageSize-100), 200)
	if !bytes.Equal(got, writeData) {
		t.Fatal("cross-page write mismatch")
	}
}

func TestWritesLostOnDisconnect(t *testing.T) {
	const pageSize = 4096
	original := make([]byte, pageSize)
	for i := range original {
		original[i] = byte(i)
	}

	addr, _, cleanup := startTestServer(t, original, pageSize)
	defer cleanup()

	// First connection: write some data.
	c1 := newTestClient(t, addr)
	patch := bytes.Repeat([]byte{0xDE}, pageSize)
	c1.write(0, patch)
	got := c1.read(0, pageSize)
	if !bytes.Equal(got, patch) {
		t.Fatal("write not reflected")
	}
	c1.disconnect()

	// Second connection: should see original data.
	c2 := newTestClient(t, addr)
	defer c2.disconnect()
	got = c2.read(0, pageSize)
	if !bytes.Equal(got, original) {
		t.Fatal("writes should be lost after disconnect")
	}
}

func TestTrim(t *testing.T) {
	const pageSize = 4096
	original := make([]byte, pageSize)
	for i := range original {
		original[i] = byte(i % 256)
	}

	addr, _, cleanup := startTestServer(t, original, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Write new data.
	writeData := bytes.Repeat([]byte{0xFF}, pageSize)
	c.write(0, writeData)
	got := c.read(0, pageSize)
	if !bytes.Equal(got, writeData) {
		t.Fatal("write not reflected")
	}

	// Trim the page.
	c.trim(0, pageSize)

	// Should revert to original data.
	got = c.read(0, pageSize)
	if !bytes.Equal(got, original) {
		t.Fatal("trim should revert to original")
	}
}

func TestFlush(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	c.write(0, bytes.Repeat([]byte{1}, pageSize))
	c.flush() // should not error
}

func TestConcurrentConnections(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*2)
	for i := range data {
		data[i] = byte(i % 256)
	}

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	// Multiple concurrent connections, each with independent writes.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := newTestClient(t, addr)
			defer c.disconnect()

			// Each connection writes a different byte pattern.
			pattern := bytes.Repeat([]byte{byte(idx)}, pageSize)
			c.write(0, pattern)

			got := c.read(0, pageSize)
			if !bytes.Equal(got, pattern) {
				t.Errorf("conn %d: write/read mismatch", idx)
			}

			// Second page should still be original.
			got = c.read(pageSize, pageSize)
			if !bytes.Equal(got, data[pageSize:]) {
				t.Errorf("conn %d: second page should be original", idx)
			}
		}(i)
	}
	wg.Wait()
}

func TestContentDedup(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*4)

	addr, srv, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Write the same content to two different pages.
	pattern := make([]byte, pageSize)
	rand.Read(pattern)

	c.write(0, pattern)
	c.write(pageSize, pattern)

	// Both pages should read back correctly.
	got1 := c.read(0, pageSize)
	got2 := c.read(pageSize, pageSize)
	if !bytes.Equal(got1, pattern) || !bytes.Equal(got2, pattern) {
		t.Fatal("dedup read mismatch")
	}

	// The cache should have only one entry for this content
	// (plus potentially zero-page entries for the other pages).
	h := hashPage(pattern)
	if _, ok := srv.cache.Get(h); !ok {
		t.Fatal("expected pattern to be in cache")
	}
}

func TestZeroPage(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	got := c.read(0, pageSize)
	if !bytes.Equal(got, make([]byte, pageSize)) {
		t.Fatal("zero page should be all zeros")
	}
}

func TestLargeFile(t *testing.T) {
	const pageSize = 4096
	const numPages = 100
	data := make([]byte, pageSize*numPages)
	rand.Read(data)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	if c.exportSize != uint64(len(data)) {
		t.Fatalf("export size: got %d, want %d", c.exportSize, len(data))
	}

	// Read a few random pages and verify.
	for _, pageIdx := range []int{0, 1, 50, 99} {
		off := uint64(pageIdx * pageSize)
		got := c.read(off, pageSize)
		if !bytes.Equal(got, data[off:off+pageSize]) {
			t.Fatalf("page %d mismatch", pageIdx)
		}
	}
}

func TestNonPageAlignedFile(t *testing.T) {
	const pageSize = 4096
	// File size not a multiple of page size.
	data := make([]byte, pageSize+500)
	for i := range data {
		data[i] = byte(i % 256)
	}

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	c := newTestClient(t, addr)
	defer c.disconnect()

	// Read past the file's actual data (within the last page).
	// The remainder should be zero-filled.
	got := c.read(pageSize, pageSize)
	expected := make([]byte, pageSize)
	copy(expected, data[pageSize:])
	if !bytes.Equal(got, expected) {
		t.Fatal("non-aligned last page mismatch")
	}
}

func TestHandshakeExportName(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	// Manual handshake using NBD_OPT_EXPORT_NAME instead of NBD_OPT_GO.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var handshake [18]byte
	if _, err := io.ReadFull(conn, handshake[:]); err != nil {
		t.Fatalf("read handshake: %v", err)
	}

	// Send client flags (fixed newstyle, no zeroes).
	var clientFlags [4]byte
	binary.BigEndian.PutUint32(clientFlags[:], uint32(nbdFlagCFixedNewstyle|nbdFlagCNoZeroes))
	if _, err := conn.Write(clientFlags[:]); err != nil {
		t.Fatalf("write client flags: %v", err)
	}

	// Send NBD_OPT_EXPORT_NAME.
	var optBuf [16]byte
	binary.BigEndian.PutUint64(optBuf[0:8], nbdOptsMagic)
	binary.BigEndian.PutUint32(optBuf[8:12], nbdOptExportName)
	binary.BigEndian.PutUint32(optBuf[12:16], 0) // empty name
	if _, err := conn.Write(optBuf[:]); err != nil {
		t.Fatalf("write opt: %v", err)
	}

	// Read reply: size(8) + flags(2), no zeroes since we set the flag.
	var reply [10]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatalf("read export reply: %v", err)
	}
	exportSize := binary.BigEndian.Uint64(reply[0:8])
	if exportSize != uint64(pageSize) {
		t.Fatalf("export size: got %d, want %d", exportSize, pageSize)
	}
}

func TestInodeSharing(t *testing.T) {
	const pageSize = 4096
	data := make([]byte, pageSize*2)
	rand.Read(data)

	addr, srv, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	// Two connections to the same file should share the same readonlyFile.
	c1 := newTestClient(t, addr)
	defer c1.disconnect()
	c2 := newTestClient(t, addr)
	defer c2.disconnect()

	// Read same page from both; second should hit cache.
	c1.read(0, pageSize)
	c2.read(0, pageSize)

	srv.mu.Lock()
	numRO := len(srv.readonlyFiles)
	srv.mu.Unlock()

	if numRO != 1 {
		t.Fatalf("expected 1 readonlyFile, got %d", numRO)
	}
}

func TestReconnectHitsCache(t *testing.T) {
	const pageSize = 4096
	const numPages = 20
	data := make([]byte, pageSize*numPages)
	rand.Read(data)

	addr, srv, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	// First connection: read all pages, populating the cache.
	c1 := newTestClient(t, addr)
	for i := 0; i < numPages; i++ {
		c1.read(uint64(i*pageSize), pageSize)
	}
	c1.disconnect()

	// Snapshot read path counters.
	diskColdBefore := srv.readPath.Get("base_disk_cold").Value()
	diskMissBefore := srv.readPath.Get("base_disk_miss").Value()
	baseMemBefore := srv.readPath.Get("base_mem").Value()

	// Second connection: same inode, all pages should come from cache.
	c2 := newTestClient(t, addr)
	defer c2.disconnect()
	for i := 0; i < numPages; i++ {
		got := c2.read(uint64(i*pageSize), pageSize)
		want := data[i*pageSize : (i+1)*pageSize]
		if !bytes.Equal(got, want) {
			t.Fatalf("page %d mismatch on reconnect", i)
		}
	}

	diskColdAfter := srv.readPath.Get("base_disk_cold").Value()
	diskMissAfter := srv.readPath.Get("base_disk_miss").Value()
	baseMemAfter := srv.readPath.Get("base_mem").Value()

	newCold := diskColdAfter - diskColdBefore
	newMiss := diskMissAfter - diskMissBefore
	newMem := baseMemAfter - baseMemBefore

	if newCold != 0 {
		t.Errorf("expected 0 base_disk_cold reads after reconnect, got %d", newCold)
	}
	if newMiss != 0 {
		t.Errorf("expected 0 base_disk_miss reads after reconnect, got %d", newMiss)
	}
	if newMem != int64(numPages) {
		t.Errorf("expected %d base_mem reads after reconnect, got %d", numPages, newMem)
	}
}

func BenchmarkRead(b *testing.B) {
	const pageSize = 4096
	data := make([]byte, pageSize*100)
	rand.Read(data)

	tmpFile, err := os.CreateTemp("", "guestbd-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	tmpFile.Write(data)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	srv := NewServer(tmpFile.Name(), pageSize, int64(pageSize)*256)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.HandleConn(conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	c := &testNBDClient{t: nil, conn: conn}
	c.t = &testing.T{} // unused, but handshake needs it
	// Skip handshake in benchmark... actually let's do it properly
	// Just inline the handshake since testNBDClient needs *testing.T

	// For benchmark, do the handshake manually.
	var handshake [18]byte
	io.ReadFull(conn, handshake[:])
	var clientFlags [4]byte
	binary.BigEndian.PutUint32(clientFlags[:], uint32(nbdFlagCFixedNewstyle|nbdFlagCNoZeroes))
	conn.Write(clientFlags[:])
	var optBuf [16]byte
	binary.BigEndian.PutUint64(optBuf[0:8], nbdOptsMagic)
	binary.BigEndian.PutUint32(optBuf[8:12], nbdOptExportName)
	binary.BigEndian.PutUint32(optBuf[12:16], 0)
	conn.Write(optBuf[:])
	var exportReply [10]byte
	io.ReadFull(conn, exportReply[:])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handle := uint64(i)
		var req [28]byte
		binary.BigEndian.PutUint32(req[0:4], nbdRequestMagic)
		binary.BigEndian.PutUint16(req[6:8], uint16(nbdCmdRead))
		binary.BigEndian.PutUint64(req[8:16], handle)
		offset := uint64((i % 100) * pageSize)
		binary.BigEndian.PutUint64(req[16:24], offset)
		binary.BigEndian.PutUint32(req[24:28], pageSize)
		if _, err := conn.Write(req[:]); err != nil {
			b.Fatal(err)
		}

		var reply [16]byte
		if _, err := io.ReadFull(conn, reply[:]); err != nil {
			b.Fatal(err)
		}
		replyData := make([]byte, pageSize)
		if _, err := io.ReadFull(conn, replyData); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(pageSize)

	// Send disconnect.
	var disc [28]byte
	binary.BigEndian.PutUint32(disc[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint16(disc[6:8], uint16(nbdCmdDisc))
	conn.Write(disc[:])
	conn.Close()
}

func TestPageCacheLRU(t *testing.T) {
	cache := newPageCache(3, 4096)

	pages := make([][]byte, 5)
	hashes := make([]pageHash, 5)
	for i := range pages {
		pages[i] = make([]byte, 4096)
		pages[i][0] = byte(i + 1)
		hashes[i] = hashPage(pages[i])
	}

	// Fill cache.
	cache.Put(hashes[0], pages[0])
	cache.Put(hashes[1], pages[1])
	cache.Put(hashes[2], pages[2])

	// All three should be present.
	for i := 0; i < 3; i++ {
		if _, ok := cache.Get(hashes[i]); !ok {
			t.Fatalf("page %d should be in cache", i)
		}
	}

	// Add a 4th; oldest (page 0) should be evicted.
	// But first access page 0 to make it recent.
	cache.Get(hashes[0])
	cache.Put(hashes[3], pages[3])
	// Now page 1 should be evicted (it's the LRU).
	if _, ok := cache.Get(hashes[1]); ok {
		t.Fatal("page 1 should have been evicted")
	}
	if _, ok := cache.Get(hashes[0]); !ok {
		t.Fatal("page 0 should still be in cache (was recently accessed)")
	}
}

func TestMultiplePageSizes(t *testing.T) {
	for _, pageSize := range []int{512, 1024, 4096, 8192} {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			data := make([]byte, pageSize*4)
			for i := range data {
				data[i] = byte(i % 251)
			}

			addr, _, cleanup := startTestServer(t, data, pageSize)
			defer cleanup()

			c := newTestClient(t, addr)
			defer c.disconnect()

			got := c.read(0, uint32(pageSize))
			if !bytes.Equal(got, data[:pageSize]) {
				t.Fatal("page mismatch")
			}

			// Sub-page write.
			patch := bytes.Repeat([]byte{0xCC}, 16)
			c.write(100, patch)
			got = c.read(0, uint32(pageSize))
			expected := make([]byte, pageSize)
			copy(expected, data[:pageSize])
			copy(expected[100:], patch)
			if !bytes.Equal(got, expected) {
				t.Fatal("sub-page write mismatch")
			}
		})
	}
}
