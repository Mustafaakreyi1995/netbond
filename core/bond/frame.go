// Package bond implements a multipath stream multiplexer: one logical session
// carried over several TCP/TLS links at once. Streams are cut into chunks,
// chunks are scheduled across whichever links have capacity, and the receiver
// puts them back in order. Unacknowledged chunks are re-sent over the surviving
// links when a link dies, so streams outlive individual networks.
package bond

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	tData = 1 // stream chunk (seq, payload); flagFin marks the end of the stream
	tAck  = 2 // seq = number of chunks the receiver has fully consumed
	tRst  = 3 // abort stream
	tUDP  = 4 // datagram; stream field = association id
	tPing = 5
	tPong = 6

	flagFin = 1

	hdrLen = 12

	// MaxChunk is the largest stream payload carried in one frame.
	MaxChunk = 16 * 1024
	// Window is how many chunks a sender may have outstanding (sent but not
	// yet consumed by the remote application) per stream.
	Window = 256

	maxPayload = MaxChunk + 256
)

var errProto = errors.New("bond: protocol error")

type frame struct {
	typ     byte
	flags   byte
	stream  uint32
	seq     uint32
	payload []byte
	chunk   *chunk // set for data frames that must be tracked for retransmit
}

func putHeader(b []byte, f *frame) {
	b[0] = f.typ
	b[1] = f.flags
	binary.BigEndian.PutUint32(b[2:], f.stream)
	binary.BigEndian.PutUint32(b[6:], f.seq)
	binary.BigEndian.PutUint16(b[10:], uint16(len(f.payload)))
}

func readFrame(r io.Reader, hdr []byte) (frame, error) {
	var f frame
	if _, err := io.ReadFull(r, hdr[:hdrLen]); err != nil {
		return f, err
	}
	f.typ = hdr[0]
	f.flags = hdr[1]
	f.stream = binary.BigEndian.Uint32(hdr[2:])
	f.seq = binary.BigEndian.Uint32(hdr[6:])
	n := int(binary.BigEndian.Uint16(hdr[10:]))
	if n > maxPayload {
		return f, errProto
	}
	if n > 0 {
		f.payload = make([]byte, n)
		if _, err := io.ReadFull(r, f.payload); err != nil {
			return f, err
		}
	}
	return f, nil
}
