package main

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// pageCache is a content-addressable LRU cache of page data,
// shared across all connections. Pages are keyed by their sha256 hash.
type pageCache struct {
	mu       sync.Mutex
	maxPages int
	pageSize int

	items map[pageHash]*list.Element
	lru   *list.List // front = most recently used

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	size      atomic.Int64
}

type cacheEntry struct {
	hash pageHash
	data []byte
}

func newPageCache(maxPages, pageSize int) *pageCache {
	c := &pageCache{
		maxPages: maxPages,
		pageSize: pageSize,
		items:    make(map[pageHash]*list.Element),
		lru:      list.New(),
	}
	return c
}

// Get returns the page data for the given hash, if present in the cache.
func (c *pageCache) Get(h pageHash) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[h]; ok {
		c.lru.MoveToFront(elem)
		c.hits.Add(1)
		return elem.Value.(*cacheEntry).data, true
	}
	c.misses.Add(1)
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
	c.size.Add(1)

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
	c.evictions.Add(1)
	c.size.Add(-1)
}
