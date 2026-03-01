package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

// skipUnlessCanNBD skips the test unless we're running on Linux
// as root or with passwordless sudo, nbd-client is installed,
// and the nbd kernel module can be loaded. It returns the sudo
// prefix ("sudo" or "").
func skipUnlessCanNBD(t *testing.T) string {
	t.Helper()

	var sudo string
	if os.Getuid() != 0 {
		out, err := exec.Command("sudo", "-n", "true").CombinedOutput()
		if err != nil {
			t.Skipf("skipping: not root and no passwordless sudo: %s", out)
		}
		sudo = "sudo"
	}

	if _, err := exec.LookPath("nbd-client"); err != nil {
		t.Skip("skipping: nbd-client not found in PATH")
	}

	out, err := sudoExec(sudo, "modprobe", "nbd")
	if err != nil {
		t.Skipf("skipping: cannot load nbd module: %s %v", out, err)
	}

	return sudo
}

// sudoExec runs a command, optionally prefixed with sudo.
func sudoExec(sudo string, args ...string) ([]byte, error) {
	if sudo != "" {
		args = append([]string{sudo}, args...)
	}
	return exec.Command(args[0], args[1:]...).CombinedOutput()
}

// findFreeNBD finds an unused /dev/nbdN device.
func findFreeNBD(t *testing.T, sudo string) string {
	t.Helper()
	for i := 0; i < 16; i++ {
		dev := fmt.Sprintf("/dev/nbd%d", i)
		if _, err := os.Stat(dev); err != nil {
			continue
		}
		// nbd-client -c exits non-zero if the device is not connected.
		var cmd *exec.Cmd
		if sudo != "" {
			cmd = exec.Command(sudo, "nbd-client", "-c", dev)
		} else {
			cmd = exec.Command("nbd-client", "-c", dev)
		}
		if err := cmd.Run(); err != nil {
			return dev
		}
	}
	t.Skip("skipping: no free /dev/nbdX device found")
	return ""
}

// nbdClientConnect connects the kernel NBD device to our server.
func nbdClientConnect(t *testing.T, sudo, addr, dev string, blockSize int) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bad addr %q: %v", addr, err)
	}
	out, err := sudoExec(sudo,
		"nbd-client", "-N", "", "-b", fmt.Sprint(blockSize),
		host, port, dev)
	if err != nil {
		t.Fatalf("nbd-client connect %s to %s: %s %v", dev, addr, out, err)
	}
}

// nbdClientDisconnect disconnects the kernel NBD device.
func nbdClientDisconnect(sudo, dev string) {
	sudoExec(sudo, "nbd-client", "-d", dev)
}

// devRead reads length bytes at the given byte offset from a block device
// using dd, bypassing any page cache with iflag=direct.
func devRead(t *testing.T, sudo, dev string, offset, length int) []byte {
	t.Helper()
	args := []string{
		"dd",
		"if=" + dev,
		fmt.Sprintf("bs=%d", length),
		"count=1",
		fmt.Sprintf("skip=%d", offset),
		"iflag=skip_bytes,direct",
		"status=none",
	}
	if sudo != "" {
		args = append([]string{sudo}, args...)
	}
	cmd := exec.Command(args[0], args[1:]...)
	out, err := cmd.Output()
	if err != nil {
		ee, _ := err.(*exec.ExitError)
		t.Fatalf("dd read at offset %d: %v (stderr: %s)", offset, err, ee.Stderr)
	}
	return out
}

// devWrite writes data at the given byte offset to a block device
// using dd, bypassing any page cache with oflag=direct.
func devWrite(t *testing.T, sudo, dev string, offset int, data []byte) {
	t.Helper()
	args := []string{
		"dd",
		"of=" + dev,
		fmt.Sprintf("bs=%d", len(data)),
		"count=1",
		fmt.Sprintf("seek=%d", offset),
		"oflag=seek_bytes,direct",
		"conv=notrunc",
		"status=none",
	}
	if sudo != "" {
		args = append([]string{sudo}, args...)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = bytes.NewReader(data)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dd write at offset %d: %s %v", offset, out, err)
	}
}

func TestLinuxNBDRead(t *testing.T) {
	sudo := skipUnlessCanNBD(t)
	dev := findFreeNBD(t, sudo)

	const pageSize = 4096
	data := make([]byte, pageSize*10)
	rand.Read(data)

	addr, _, cleanup := startTestServer(t, data, pageSize)
	defer cleanup()

	nbdClientConnect(t, sudo, addr, dev, pageSize)
	t.Cleanup(func() { nbdClientDisconnect(sudo, dev) })
	time.Sleep(200 * time.Millisecond)

	// Read first page.
	got := devRead(t, sudo, dev, 0, pageSize)
	if !bytes.Equal(got, data[:pageSize]) {
		t.Fatalf("first page mismatch: got %x... want %x...", got[:16], data[:16])
	}

	// Read a page in the middle.
	off := 5 * pageSize
	got = devRead(t, sudo, dev, off, pageSize)
	if !bytes.Equal(got, data[off:off+pageSize]) {
		t.Fatalf("middle page mismatch at offset %d", off)
	}

	// Read last page.
	off = 9 * pageSize
	got = devRead(t, sudo, dev, off, pageSize)
	if !bytes.Equal(got, data[off:off+pageSize]) {
		t.Fatalf("last page mismatch at offset %d", off)
	}
}

func TestLinuxNBDWriteAndReadBack(t *testing.T) {
	sudo := skipUnlessCanNBD(t)
	dev := findFreeNBD(t, sudo)

	const pageSize = 4096
	original := make([]byte, pageSize*4)
	rand.Read(original)

	addr, _, cleanup := startTestServer(t, original, pageSize)
	defer cleanup()

	nbdClientConnect(t, sudo, addr, dev, pageSize)
	t.Cleanup(func() { nbdClientDisconnect(sudo, dev) })
	time.Sleep(200 * time.Millisecond)

	// Write a full page of 0xBE.
	writeData := bytes.Repeat([]byte{0xBE}, pageSize)
	devWrite(t, sudo, dev, 0, writeData)

	// Read it back.
	got := devRead(t, sudo, dev, 0, pageSize)
	if !bytes.Equal(got, writeData) {
		t.Fatalf("written page read-back mismatch: got %x... want %x...", got[:16], writeData[:16])
	}

	// Verify an unmodified page is still the original data.
	got = devRead(t, sudo, dev, pageSize, pageSize)
	if !bytes.Equal(got, original[pageSize:2*pageSize]) {
		t.Fatal("unmodified page should match original")
	}

	// Write a second distinct pattern to page 2.
	writeData2 := bytes.Repeat([]byte{0xEF}, pageSize)
	devWrite(t, sudo, dev, 2*pageSize, writeData2)

	got = devRead(t, sudo, dev, 2*pageSize, pageSize)
	if !bytes.Equal(got, writeData2) {
		t.Fatal("second write read-back mismatch")
	}
}

func TestLinuxNBDWritesLostOnReconnect(t *testing.T) {
	sudo := skipUnlessCanNBD(t)
	dev := findFreeNBD(t, sudo)

	const pageSize = 4096
	original := make([]byte, pageSize*4)
	rand.Read(original)

	addr, _, cleanup := startTestServer(t, original, pageSize)
	defer cleanup()

	// First connection: write data via the kernel.
	nbdClientConnect(t, sudo, addr, dev, pageSize)
	time.Sleep(200 * time.Millisecond)

	writeData := bytes.Repeat([]byte{0xAA}, pageSize)
	devWrite(t, sudo, dev, 0, writeData)

	// Confirm the write is visible.
	got := devRead(t, sudo, dev, 0, pageSize)
	if !bytes.Equal(got, writeData) {
		t.Fatalf("write not visible: got %x... want %x...", got[:16], writeData[:16])
	}

	// Disconnect.
	nbdClientDisconnect(sudo, dev)
	time.Sleep(200 * time.Millisecond)

	// Reconnect: the server's per-connection writes should be gone.
	nbdClientConnect(t, sudo, addr, dev, pageSize)
	t.Cleanup(func() { nbdClientDisconnect(sudo, dev) })
	time.Sleep(200 * time.Millisecond)

	got = devRead(t, sudo, dev, 0, pageSize)
	if !bytes.Equal(got, original[:pageSize]) {
		t.Fatalf("writes should be lost after reconnect: got %x... want %x...",
			got[:16], original[:16])
	}
}

func TestLinuxNBDMultipleConnections(t *testing.T) {
	sudo := skipUnlessCanNBD(t)

	// Find two free devices.
	dev1 := findFreeNBD(t, sudo)
	dev2 := ""
	for i := 0; i < 16; i++ {
		d := fmt.Sprintf("/dev/nbd%d", i)
		if d == dev1 {
			continue
		}
		if _, err := os.Stat(d); err != nil {
			continue
		}
		var cmd *exec.Cmd
		if sudo != "" {
			cmd = exec.Command(sudo, "nbd-client", "-c", d)
		} else {
			cmd = exec.Command("nbd-client", "-c", d)
		}
		if err := cmd.Run(); err != nil {
			dev2 = d
			break
		}
	}
	if dev2 == "" {
		t.Skip("skipping: need two free /dev/nbdX devices")
	}

	const pageSize = 4096
	original := make([]byte, pageSize*4)
	rand.Read(original)

	addr, _, cleanup := startTestServer(t, original, pageSize)
	defer cleanup()

	nbdClientConnect(t, sudo, addr, dev1, pageSize)
	t.Cleanup(func() { nbdClientDisconnect(sudo, dev1) })
	nbdClientConnect(t, sudo, addr, dev2, pageSize)
	t.Cleanup(func() { nbdClientDisconnect(sudo, dev2) })
	time.Sleep(200 * time.Millisecond)

	// Both should see the same original data.
	got1 := devRead(t, sudo, dev1, 0, pageSize)
	got2 := devRead(t, sudo, dev2, 0, pageSize)
	if !bytes.Equal(got1, original[:pageSize]) || !bytes.Equal(got2, original[:pageSize]) {
		t.Fatal("both connections should see original data")
	}

	// Write on dev1 should not be visible on dev2.
	writeData := bytes.Repeat([]byte{0xCC}, pageSize)
	devWrite(t, sudo, dev1, 0, writeData)

	got1 = devRead(t, sudo, dev1, 0, pageSize)
	if !bytes.Equal(got1, writeData) {
		t.Fatal("dev1 should see its own write")
	}

	got2 = devRead(t, sudo, dev2, 0, pageSize)
	if !bytes.Equal(got2, original[:pageSize]) {
		t.Fatalf("dev2 should still see original: got %x... want %x...",
			got2[:16], original[:16])
	}
}
