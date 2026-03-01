package main

// NBD protocol constants.
// See https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

const (
	// Handshake magic values
	nbdMagic         uint64 = 0x4e42444d41474943 // "NBDMAGIC"
	nbdOptsMagic     uint64 = 0x49484156454F5054 // "IHAVEOPT"
	nbdRequestMagic  uint32 = 0x25609513
	nbdReplyMagic    uint32 = 0x67446698
	nbdOptReplyMagic uint64 = 0x3e889045565a9

	// Handshake flags (server to client)
	nbdFlagFixedNewstyle uint16 = 1 << 0
	nbdFlagNoZeroes      uint16 = 1 << 1

	// Client flags
	nbdFlagCFixedNewstyle uint32 = 1 << 0
	nbdFlagCNoZeroes      uint32 = 1 << 1

	// Transmission flags
	nbdFlagHasFlags  uint16 = 1 << 0
	nbdFlagSendFlush uint16 = 1 << 2
	nbdFlagSendTrim  uint16 = 1 << 5

	// Option codes
	nbdOptExportName uint32 = 1
	nbdOptAbort      uint32 = 2
	nbdOptList       uint32 = 3
	nbdOptGo         uint32 = 7

	// Option reply types
	nbdRepAck      uint32 = 1
	nbdRepServer   uint32 = 2
	nbdRepInfo     uint32 = 3
	nbdRepErrUnsup uint32 = (1 << 31) + 1

	// Info types
	nbdInfoExport    uint16 = 0
	nbdInfoBlockSize uint16 = 3

	// Command types
	nbdCmdRead  uint16 = 0
	nbdCmdWrite uint16 = 1
	nbdCmdDisc  uint16 = 2
	nbdCmdFlush uint16 = 3
	nbdCmdTrim  uint16 = 4

	// Maximum payload size per the NBD spec.
	nbdMaxPayload uint32 = 32 * 1024 * 1024 // 32 MB

	// Error codes (Linux errno values)
	nbdEIO    uint32 = 5
	nbdEINVAL uint32 = 22
)

// nbdRequest is the wire format for an NBD transmission request.
type nbdRequest struct {
	Magic  uint32
	Flags  uint16
	Type   uint16
	Handle uint64
	Offset uint64
	Length uint32
}
