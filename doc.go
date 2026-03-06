// Package guestbd implements a userspace NBD (Network Block Device) server
// designed for ephemeral CI workload VMs.
//
// Each TCP connection gets its own read/write namespace layered on top of a
// shared read-only base image. Writes are per-connection and ephemeral: they
// are lost when the connection disconnects. The base image may be a raw disk
// file or a qcow2 image (auto-detected by file extension).
//
// Pages are content-addressed by their SHA-256 hash and de-duplicated in a
// global LRU cache shared across all connections. Multiple connections to the
// same backing file (same inode) share the same page hash table, so
// reconnecting clients benefit from previously computed hashes without
// re-reading from disk.
package guestbd
