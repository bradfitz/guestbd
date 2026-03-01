package main

import "crypto/sha256"

// pageHash is the sha256 hash of a page's contents.
// A zero value means the page has not been read from disk yet.
type pageHash [sha256.Size]byte

// zeroPageHash is the sha256 of a page filled entirely with zero bytes.
// This is different from the zero value of pageHash, which means "not yet computed".
var zeroPageHash pageHash

func initZeroPageHash(pageSize int) {
	zeroPageHash = sha256.Sum256(make([]byte, pageSize))
}

func hashPage(data []byte) pageHash {
	return sha256.Sum256(data)
}
