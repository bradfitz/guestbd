# guestbd

guestbd is a userspace NBD server (meant primarily for Linux) that
gives each TCP connection its own virtual read/write namespace on top
of a given file on disk that's only read-only. Then the TCP connection
breaks, any writes that were made by that client are lost.

Any written data exists only in memory (for the first configured N
gigabytes) and only spills to disk as needed as a cache.

Everything is content-addressable and de-duped. (use type pageHash
[sha256.Size]byte as a value)

When a new TCP connection is accepted, the named file (given by a flag
to the binary) is opened, and its *os.File is compared against
existing open TCP connections (with os.SameFile) to see if it maps to
the same inode on disk. That inode than effectively maps to a "type
readonlyFile struct { f *os.File, size int64, refcount ... }"
singleton (for that inode) that contains lazily-computed state,
including a map of all 4K (configurable) pages in the file, and what
their sha256 checksum is. If a page is entirely zero bytes, the
checksum is skipped and a pre-computed sha256-of-zero is used
instead. This lets the checksum table double as a bitmap of whether a
readonlyFile page has been read: if the checksum is zero, it hasn't
been read yet and needs to be read from disk.

Then, each page's 4KB data is stored just once for the whole process
as a function of its hash. This means if the file on disk is replaced
(mv newversion replacedfile) and gets a new inode, but there are
clients still open on the old version, and the new version has 80% of
the same contents overall (e.g. the ext4 filesystem was rebuilt with
mostly identical contents, but different inode dentry metadata), then
the cache will be mostly shared.

That hash=>contents is stored in a configurably sized LRU cache
tracking hit rates, and used for all connections. No reference
counting is done on it; things simply age out if they no longer exist
in the base image or any old versions of the base image.

Likewise, all writes update the underlying block device in 4KB units,
so a small 1KB write from a client ends up reading the existing 4KB,
mutating the 1KB in the middle of it, and then hashing the whole 4KB.

It's expected that many connections will end up doing writes with the
same 4KB page contents (at different offsets in the block device), so
we also share that memory. But because written data needs to always be
re-readable later, we need to guarantee it either exists in memory or
on disk. To start simple, we don't try to be clever with how data is
lazily written to disk. Each connection maintains a table:

   pageNum => *struct{ hash pageHash, dirtyPage int }

Where dirtyPage is the 4KB page number on disk (a per TCP connection
temp file that's open for read/write and then unlinked immediately)
where the page was written. Whenever a new dirty page is written, it
gets a monotonically increasing dirtyPageNum. All writes to the
connDirtyFile are in 4KB units, or whatever the flag-configured page
size is (which can be limited to a power of two)

### Testing

Tests cover all the cases. The tests try to avoid requiring root or
Linux for most tests, preferring a pure Go NDB client for testing, but
there are also tests that, when run on Linux as root or with
password-less sudo access, set up ndb mount for testing.

### Observability

The server uses tailscale.com/tsweb (including its DebugHandler) to
expose pprof, expvar, and a Prometheus-compatible `/debug/varz` endpoint
on the `--debug-addr` (default `:8080`).

Metrics use normal expvar metrics (which tsweb Prometheus-ifies) and
tailscale.com/metrics's LabelMap and Histogram types.

#### Counters

| Metric | Description |
|--------|-------------|
| `guestbd_total_conns` | Total TCP connections accepted |
| `guestbd_nbd_ops{type=read\|write\|disconnect\|flush\|trim}` | NBD operations by type |
| `guestbd_read_bytes` | Total bytes read by clients |
| `guestbd_read_pages` | Total page reads (dirty + base layer) |
| `guestbd_read_path{type=base_mem\|base_disk_cold\|base_disk_miss\|from_write}` | Read source breakdown |
| `guestbd_write_bytes` | Total bytes written by clients |
| `guestbd_write_pages` | Total dirty pages written |
| `guestbd_cache{path=hits\|misses\|evictions}` | Page cache operations |

#### Gauges

| Metric | Description |
|--------|-------------|
| `guestbd_active_conns` | Currently connected clients |
| `guestbd_cache_entries` | Pages in the LRU cache |
| `guestbd_cache_bytes` | Bytes used by the LRU cache |
| `guestbd_base_images_active` | readonlyFile entries with active connections |
| `guestbd_base_images_cached` | readonlyFile entries in memory (including idle) |
| `guestbd_page_size` | Configured page size |

#### Histograms

| Metric | Description |
|--------|-------------|
| `guestbd_read_size_bytes` | Distribution of NBD read request sizes |
| `guestbd_write_size_bytes` | Distribution of NBD write request sizes |

The `read_path` metric is particularly useful for diagnosing cache effectiveness:

- **`base_mem`** — page hash was known and data was in the LRU cache (ideal)
- **`base_disk_cold`** — page hash was unknown (first read ever, or readonlyFile was recreated due to inode change); had to read from disk
- **`base_disk_miss`** — page hash was known but data was evicted from the LRU cache; had to re-read from disk (indicates cache is too small)
- **`from_write`** — read of a page that was previously written on this connection

## Imports

The server uses tailscale.com/tsweb (including its DebugHandler),
tailscale.com/util/set.{Set,HandleSet} etc as needed, but doesn't
depend on tsnet.
