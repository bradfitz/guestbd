package guestbd

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteDirtyTo checks that writing a shared snapshot's dirty pages onto
// a copy of the base image reproduces what clients see.
func TestWriteDirtyTo(t *testing.T) {
	const pageSize = 4096
	base := make([]byte, 3*pageSize+100) // not a page multiple
	for i := range base {
		base[i] = byte(i % 251)
	}
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.img")
	if err := os.WriteFile(basePath, base, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(FileSource(basePath), WithPageSize(pageSize), WithSharedSnapshot(), WithMaxMem(0))
	defer srv.Close()
	if srv.SharedSnapshot() != nil {
		t.Fatal("SharedSnapshot non-nil before any connection")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.Serve(ln)

	c := newTestClient(t, ln.Addr().String())
	c.write(10, bytes.Repeat([]byte{0xaa}, 20))               // partial first page
	c.write(pageSize-5, bytes.Repeat([]byte{0xbb}, 10))       // straddles pages 0 and 1
	c.write(3*pageSize+50, bytes.Repeat([]byte{0xcc}, 50))    // partial last page, to EOF
	c.write(2*pageSize, bytes.Repeat([]byte{0xdd}, pageSize)) // whole page 2...
	c.trim(2*pageSize, pageSize)                              // ...then trimmed back to base
	want := c.read(0, uint32(len(base)))
	c.disconnect()

	snap := srv.SharedSnapshot()
	if snap == nil {
		t.Fatal("SharedSnapshot nil after a connection")
	}
	outPath := filepath.Join(dir, "persisted.img")
	if err := os.WriteFile(outPath, base, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(outPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	pages, err := snap.WriteDirtyTo(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if pages != 3 {
		t.Errorf("wrote %d pages, want 3 (pages 0, 1 and 3; page 2 was trimmed)", pages)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(base) {
		t.Fatalf("persisted image is %d bytes, want %d", len(got), len(base))
	}
	if !bytes.Equal(got, want) {
		t.Fatal("persisted image differs from what the client read")
	}
}
