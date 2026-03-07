// Package guestbd implements a userspace NBD (Network Block Device) server
// designed for ephemeral CI workload VMs.
//
// Each Snapshot provides a read/write namespace layered on top of a shared
// read-only base image. By default each TCP connection gets its own Snapshot
// whose writes are discarded on disconnect, but a Snapshot can also be shared
// across multiple connections so that reconnecting clients see previous writes.
// The base image may be a raw disk file or a qcow2 image (auto-detected by
// file extension).
//
// Pages are content-addressed by their SHA-256 hash and de-duplicated in a
// global LRU cache shared across all snapshots. Multiple snapshots backed by
// the same file (same inode) share the same page hash table, so
// reconnecting clients benefit from previously computed hashes without
// re-reading from disk.
package guestbd
