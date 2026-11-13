//go:build linux

package bond

import (
	"net"
	"syscall"
)

const tcpNotSentLowat = 25 // TCP_NOTSENT_LOWAT

// tuneTCP keeps the kernel's unsent backlog on a link small. Without this a
// link buffers megabytes before it pushes back, which hides its real speed
// from the scheduler and adds latency.
func tuneTCP(c net.Conn, lowat int) {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	tc.SetNoDelay(true)
	tc.SetKeepAlive(true)
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, lowat)
	})
}
