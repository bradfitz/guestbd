package main

import "crypto/sha256"

// pageHash is the sha256 hash of a page's contents.
// A zero value means the page has not been read from disk yet.
type pageHash [sha256.Size]byte

func hashPage(data []byte) pageHash {
	return sha256.Sum256(data)
}
