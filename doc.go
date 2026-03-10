// Package guestbd implements a userspace NBD (Network Block Device) server
// designed for ephemeral CI workload VMs.
//
// Each Snapshot provides a read/write namespace layered on top of a shared
// read-only base image. By default each TCP connection gets its own Snapshot
// whose writes are discarded on disconnect, but a Snapshot can also be shared
// across multiple connections so that reconnecting clients see previous writes.
//
// The base image is provided as a [BaseImageSource] — a function returning a
// [BaseImage] — so callers can serve images from files, object stores,
// or memory. [BaseImage.BaseImageKey] enables equivalence-keyed caching of
// idle baseImageStates so that reconnecting clients reuse the page hash table.
// The [FileSource] helper provides the common file-based workflow, with
// automatic qcow2 detection by file extension.
//
// Pages are content-addressed by their SHA-256 hash and de-duplicated in a
// global LRU cache shared across all snapshots.
package guestbd
