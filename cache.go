package main

import (
	"container/list"
	"expvar"
	"sync"

	"tailscale.com/metrics"
)

// pageCache is a content-addressable LRU cache of page data,
// shared across all connections. Pages are keyed by their sha256 hash.
type pageCache struct {
	mu       sync.Mutex
	maxPages int
	pageSize int

	items map[pageHash]*list.Element
	lru   *list.List // front = most recently used

	// counter_guestbd_cache{path="hits|misses|evictions"}
	path metrics.LabelMap

	// gauge_guestbd_cache_entries
	entries expvar.Int

	// gauge_guestbd_cache_bytes
	bytes expvar.Int
}

type cacheEntry struct {
	hash pageHash
	data []byte
}

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

	d := make([]byte, len(data))
	copy(d, data)

	entry := &cacheEntry{hash: h, data: d}
	elem := c.lru.PushFront(entry)
	c.items[h] = elem
	c.entries.Add(1)
	c.bytes.Add(int64(c.pageSize))

	for c.lru.Len() > c.maxPages {
		c.evict()
	}
}

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
