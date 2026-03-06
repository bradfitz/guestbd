package guestbd

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"expvar"
	"sync"

	"tailscale.com/metrics"
)

// pageHash is the sha256 hash of a page's contents.
// A zero value means the page has not been read from disk yet.
type pageHash [sha256.Size]byte

// hashPage returns the SHA-256 hash of data.
func hashPage(data []byte) pageHash {
	return sha256.Sum256(data)
}

// pageCache is a content-addressable LRU cache of page data,
// shared across all connections. Pages are keyed by their sha256 hash.
type pageCache struct {
	mu       sync.Mutex
	maxPages int
	pageSize int

	items map[pageHash]*list.Element
	lru   *list.List // front = most recently used

	path    metrics.LabelMap // counter_guestbd_cache{path="hits|misses|evictions"}
	entries expvar.Int       // gauge_guestbd_cache_entries
	bytes   expvar.Int       // gauge_guestbd_cache_bytes
}

// cacheEntry is a single element in the LRU list, pairing a page hash with
// its data.
type cacheEntry struct {
	hash pageHash
	data []byte
}

// newPageCache returns a new page cache that holds at most maxPages pages of
// the given pageSize.
func newPageCache(maxPages, pageSize int) *pageCache {
	return &pageCache{
		maxPages: maxPages,
		pageSize: pageSize,
		items:    make(map[pageHash]*list.Element),
		lru:      list.New(),
		path:     metrics.LabelMap{Label: "path"},
	}
}

// Get returns the page data for the given hash, if present in the cache.
func (c *pageCache) Get(h pageHash) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[h]; ok {
		c.lru.MoveToFront(elem)
		c.path.Add("hits", 1)
		return elem.Value.(*cacheEntry).data, true
	}
	c.path.Add("misses", 1)
	return nil, false
}

// Put adds page data to the cache, keyed by its hash.
// If the hash already exists, it's moved to the front of the LRU.
func (c *pageCache) Put(h pageHash, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[h]; ok {
		c.lru.MoveToFront(elem)
		return
	}

	entry := &cacheEntry{hash: h, data: bytes.Clone(data)}
	elem := c.lru.PushFront(entry)
	c.items[h] = elem
	c.entries.Add(1)
	c.bytes.Add(int64(c.pageSize))

	for c.lru.Len() > c.maxPages {
		c.evict()
	}
}

// evict removes the least recently used entry from the cache.
func (c *pageCache) evict() {
	elem := c.lru.Back()
	if elem == nil {
		return
	}
	c.lru.Remove(elem)
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.hash)
	c.path.Add("evictions", 1)
	c.entries.Add(-1)
	c.bytes.Add(-int64(c.pageSize))
}
