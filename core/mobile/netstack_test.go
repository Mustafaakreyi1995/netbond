package mobile

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"

	"netbond.app/core/bond"
)

// pair returns a datagram socket pair standing in for the VPN interface. The
// buffers are enlarged because a full buffer drops packets, which the two
// userspace TCP stacks then recover from only after long retransmit timers.
func pair(t *testing.T) [2]int {
	sp, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range sp {
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUFFORCE, 16<<20)
		syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUFFORCE, 16<<20)
	}
	return sp
}

func localIP(t *testing.T) net.IP {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && !n.IP.IsLoopback() {
			return n.IP.To4()
		}
	}
	return net.IPv4(127, 0, 0, 1)
}

// TestTunEndToEnd stands a second userspace stack in for the phone's apps,
// joined to tunStack by a datagram socket pair in place of the VPN interface.
func TestTunEndToEnd(t *testing.T) {
	ip := localIP(t)
	t.Logf("target ip %v", ip)

	eln, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer eln.Close()
	go func() {
		for {
			c, err := eln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	upc, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
	if err != nil {
		t.Fatal(err)
	}
	defer upc.Close()
	go func() {
		buf := make([]byte, 2000)
		for {
			n, from, err := upc.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			upc.WriteToUDPAddrPort(buf[:n], from)
		}
	}()

	secret := bond.NewSecret()
	sv, _ := bond.NewServer(secret, nil)
	sv.AllowLocal = true
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go sv.Serve(ln)
	cl := bond.NewClient(secret, nil)
	defer cl.Close()
	for _, id := range []string{"a", "b"} {
		cl.AddLink(id, id, func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", ln.Addr().String())
		})
	}

	sp := pair(t)
	ts, err := newTunStack(sp[1], 1500, bonded{cl}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.close()

	// the "phone apps" stack
	syscall.SetNonblock(sp[0], true)
	ep, err := fdbased.New(&fdbased.Options{FDs: []int{sp[0]}, MTU: 1500, RXChecksumOffload: true})
	if err != nil {
		t.Fatal(err)
	}
	app := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})
	if e := app.CreateNIC(1, ep); e != nil {
		t.Fatal(e)
	}
	app.AddProtocolAddress(1, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom4([4]byte{10, 111, 0, 2}).WithPrefix(),
	}, stack.AddressProperties{})
	app.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})

	var ip4 [4]byte
	copy(ip4[:], ip)
	port := eln.Addr().(*net.TCPAddr).Port
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := gonet.DialContextTCP(ctx, app, tcpip.FullAddress{Addr: tcpip.AddrFrom4(ip4), Port: uint16(port)}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatalf("dial through tun: %v", err)
	}
	data := make([]byte, 3<<20)
	rand.Read(data)
	go func() { conn.Write(data); conn.CloseWrite() }()
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("tcp echo through tun failed: err=%v got=%d want=%d", err, len(got), len(data))
	}
	conn.Close()

	uport := upc.LocalAddr().(*net.UDPAddr).Port
	uc, err := gonet.DialUDP(app, nil, &tcpip.FullAddress{Addr: tcpip.AddrFrom4(ip4), Port: uint16(uport)}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	uc.Write([]byte("ping"))
	uc.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 100)
	n, err := uc.Read(buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("udp echo through tun failed: %v %q", err, buf[:n])
	}
}

type fakeCB struct{ binds map[string]int }

func (f *fakeCB) BindSocket(id string, fd int32) bool { f.binds[id]++; return true }
func (f *fakeCB) Log(string)                          {}

// TestDirectMode checks the no-server path: flows leave directly and are
// spread across the available links.
func TestDirectMode(t *testing.T) {
	ip := localIP(t)
	eln, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer eln.Close()
	go func() {
		for {
			c, err := eln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	upc, _ := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
	defer upc.Close()
	go func() {
		buf := make([]byte, 2000)
		for {
			n, from, err := upc.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			upc.WriteToUDPAddrPort(buf[:n], from)
		}
	}()

	cb := &fakeCB{binds: map[string]int{}}
	d := newDirect(cb)
	d.addLink("wifi", "Wi-Fi")
	d.addLink("cell", "Mobile data")
	sp := pair(t)
	ts, err := newTunStack(sp[1], 1500, d, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.close()
	defer d.close()

	syscall.SetNonblock(sp[0], true)
	ep, _ := fdbased.New(&fdbased.Options{FDs: []int{sp[0]}, MTU: 1500, RXChecksumOffload: true})
	app := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})
	app.CreateNIC(1, ep)
	app.AddProtocolAddress(1, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom4([4]byte{10, 111, 0, 2}).WithPrefix(),
	}, stack.AddressProperties{})
	app.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})

	var ip4 [4]byte
	copy(ip4[:], ip)
	port := uint16(eln.Addr().(*net.TCPAddr).Port)
	for i := 0; i < 6; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := gonet.DialContextTCP(ctx, app, tcpip.FullAddress{Addr: tcpip.AddrFrom4(ip4), Port: port}, ipv4.ProtocolNumber)
		cancel()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		data := make([]byte, 300*1024)
		rand.Read(data)
		go func() { conn.Write(data); conn.CloseWrite() }()
		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		got, err := io.ReadAll(conn)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("direct echo %d failed: err=%v got=%d", i, err, len(got))
		}
		conn.Close()
	}
	if cb.binds["wifi"] < 2 || cb.binds["cell"] < 2 {
		t.Fatalf("connections were not spread: %v", cb.binds)
	}
	st, _ := d.stats()
	for _, s := range st {
		if s.Rx == 0 || s.Tx == 0 {
			t.Fatalf("no traffic counted on %s: %+v", s.ID, s)
		}
	}

	uport := uint16(upc.LocalAddr().(*net.UDPAddr).Port)
	uc, err := gonet.DialUDP(app, nil, &tcpip.FullAddress{Addr: tcpip.AddrFrom4(ip4), Port: uport}, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	uc.Write([]byte("ping"))
	uc.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 100)
	n, err := uc.Read(buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("direct udp failed: %v %q", err, buf[:n])
	}
}
