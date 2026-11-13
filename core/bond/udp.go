package bond

import (
	"encoding/binary"
	"errors"
	"net/netip"
)

func encodeUDP(addr netip.AddrPort, data []byte) []byte {
	ip := addr.Addr().Unmap().AsSlice()
	b := make([]byte, 0, 1+len(ip)+2+len(data))
	b = append(b, byte(len(ip)))
	b = append(b, ip...)
	b = binary.BigEndian.AppendUint16(b, addr.Port())
	return append(b, data...)
}

func decodeUDP(p []byte) (netip.AddrPort, []byte, error) {
	if len(p) < 1 {
		return netip.AddrPort{}, nil, errors.New("short")
	}
	n := int(p[0])
	if (n != 4 && n != 16) || len(p) < 1+n+2 {
		return netip.AddrPort{}, nil, errors.New("bad address")
	}
	ip, _ := netip.AddrFromSlice(p[1 : 1+n])
	port := binary.BigEndian.Uint16(p[1+n:])
	return netip.AddrPortFrom(ip, port), p[1+n+2:], nil
}

// MaxDatagram is the largest UDP payload the tunnel carries.
const MaxDatagram = MaxChunk
