package mobile

import (
	"context"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// TestDownloadSpeed measures a single download from a local server through
// the VPN path in no-server mode, at several interface MTUs. It reports
// numbers rather than failing: absolute speed depends on the machine.
func TestDownloadSpeed(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ip := localIP(t)
	ln, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	chunk := make([]byte, 256*1024)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					if _, err := c.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
	}()
	var ip4 [4]byte
	copy(ip4[:], ip)
	port := uint16(ln.Addr().(*net.TCPAddr).Port)

	// direct, no VPN, for reference
	c, _ := net.Dial("tcp", ln.Addr().String())
	t.Logf("no VPN:      %6.0f MB/s", measure(c))

	for _, mtu := range []int{1500, 9000, 32000} {
		cb := &fakeCB{binds: map[string]int{}}
		d := newDirect(cb)
		d.addLink("wifi", "Wi-Fi")
		sp := pair(t)
		ts, err := newTunStack(sp[1], mtu, d, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		syscall.SetNonblock(sp[0], true)
		ep, _ := fdbased.New(&fdbased.Options{FDs: []int{sp[0]}, MTU: uint32(mtu), RXChecksumOffload: true})
		app := stack.New(stack.Options{
			NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
			TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
		})
		// the phone kernel's TCP has large windows; give the stand-in the same
		rcv := tcpip.TCPReceiveBufferSizeRangeOption{Min: 4096, Default: 4 << 20, Max: 8 << 20}
		app.SetTransportProtocolOption(tcp.ProtocolNumber, &rcv)
		app.CreateNIC(1, ep)
		app.AddProtocolAddress(1, tcpip.ProtocolAddress{
			Protocol:          ipv4.ProtocolNumber,
			AddressWithPrefix: tcpip.AddrFrom4([4]byte{10, 111, 0, 2}).WithPrefix(),
		}, stack.AddressProperties{})
		app.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := gonet.DialContextTCP(ctx, app, tcpip.FullAddress{Addr: tcpip.AddrFrom4(ip4), Port: port}, ipv4.ProtocolNumber)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("VPN mtu %-5d %6.0f MB/s", mtu, measure(conn))
		conn.Close()
		app.Close()
		ts.close()
		d.close()
	}
}

func measure(c net.Conn) float64 {
	buf := make([]byte, 256*1024)
	io.ReadFull(c, buf) // warm up
	start := time.Now()
	var n int64
	for time.Since(start) < 2*time.Second {
		k, err := c.Read(buf)
		n += int64(k)
		if err != nil {
			break
		}
	}
	c.Close()
	return float64(n) / time.Since(start).Seconds() / (1 << 20)
}

var _ = fmt.Sprint
