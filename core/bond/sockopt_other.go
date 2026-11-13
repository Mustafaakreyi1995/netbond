//go:build !linux

package bond

import "net"

func tuneTCP(c net.Conn, lowat int) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
	}
}
