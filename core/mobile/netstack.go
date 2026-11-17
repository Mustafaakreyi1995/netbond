package mobile

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	"netbond.app/core/bond"
)

const nicID = 1

// tunStack is a userspace TCP/IP stack sitting on the VPN interface. Every
// TCP connection and UDP flow the phone's apps open ends here and is carried
// to the server through the bonded client.
type tunStack struct {
	fd   int
	s    *stack.Stack
	tr   transport
	logf func(string, ...any)
}

// transport carries the flows that arrive on the VPN interface: either
// through the bonded server session or straight out over one network.
type transport interface {
	serveTCP(conn *gonet.TCPConn, dst netip.AddrPort)
	serveUDP(conn *gonet.UDPConn, dst netip.AddrPort)
}

func newTunStack(fd, mtu int, tr transport, logf func(string, ...any)) (*tunStack, error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		return nil, err
	}
	ep, err := fdbased.New(&fdbased.Options{
		FDs:               []int{fd},
		MTU:               uint32(mtu),
		RXChecksumOffload: true,
	})
	if err != nil {
		return nil, err
	}
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol4, icmp.NewProtocol6},
	})
	t := &tunStack{fd: fd, s: s, tr: tr, logf: logf}

	sack := tcpip.TCPSACKEnabled(true)
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &sack)
	mod := tcpip.TCPModerateReceiveBufferOption(true)
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &mod)
	cc := tcpip.CongestionControlOption("cubic")
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &cc)
	snd := tcpip.TCPSendBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: 4 << 20, Max: 8 << 20}
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &snd)
	rcv := tcpip.TCPReceiveBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: 1 << 20, Max: 8 << 20}
	s.SetTransportProtocolOption(tcp.ProtocolNumber, &rcv)

	tcpFwd := tcp.NewForwarder(s, 0, 2048, t.handleTCP)
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)
	udpFwd := udp.NewForwarder(s, t.handleUDP)
	s.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	if e := s.CreateNIC(nicID, ep); e != nil {
		return nil, errors.New(e.String())
	}
	// accept packets for any destination and answer from that address
	if e := s.SetPromiscuousMode(nicID, true); e != nil {
		return nil, errors.New(e.String())
	}
	if e := s.SetSpoofing(nicID, true); e != nil {
		return nil, errors.New(e.String())
	}
	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})
	return t, nil
}

func (t *tunStack) close() {
	t.s.RemoveNIC(nicID)
	t.s.Close()
	syscall.Close(t.fd)
}

func hostPort(a tcpip.Address, port uint16) (netip.AddrPort, bool) {
	ip, ok := netip.AddrFromSlice(a.AsSlice())
	if !ok {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(ip.Unmap(), port), true
}

func (t *tunStack) handleTCP(r *tcp.ForwarderRequest) {
	id := r.ID()
	dst, ok := hostPort(id.LocalAddress, id.LocalPort)
	if !ok {
		r.Complete(true)
		return
	}
	var wq waiter.Queue
	ep, e := r.CreateEndpoint(&wq)
	if e != nil {
		r.Complete(true)
		return
	}
	r.Complete(false)
	ep.SocketOptions().SetKeepAlive(true)
	idle := tcpip.KeepaliveIdleOption(60 * time.Second)
	ep.SetSockOpt(&idle)
	intvl := tcpip.KeepaliveIntervalOption(30 * time.Second)
	ep.SetSockOpt(&intvl)
	ep.SetSockOptInt(tcpip.KeepaliveCountOption, 9)
	go t.tr.serveTCP(gonet.NewTCPConn(&wq, ep), dst)
}

func (t *tunStack) handleUDP(r *udp.ForwarderRequest) {
	id := r.ID()
	dst, ok := hostPort(id.LocalAddress, id.LocalPort)
	if !ok {
		return
	}
	var wq waiter.Queue
	ep, e := r.CreateEndpoint(&wq)
	if e != nil {
		return
	}
	go t.tr.serveUDP(gonet.NewUDPConn(&wq, ep), dst)
}

func udpIdle(dst netip.AddrPort) time.Duration {
	if dst.Port() == 53 {
		return 10 * time.Second
	}
	return 60 * time.Second
}

// bonded sends everything through the server session.
type bonded struct{ c *bond.Client }

func (b bonded) serveTCP(conn *gonet.TCPConn, dst netip.AddrPort) {
	st, err := b.c.DialTCP(net.JoinHostPort(dst.Addr().String(), strconv.Itoa(int(dst.Port()))))
	if err != nil {
		conn.Close()
		return
	}
	bond.Relay(st, conn)
}

func (b bonded) serveUDP(conn *gonet.UDPConn, dst netip.AddrPort) {
	u := b.c.DialUDP()
	idle := udpIdle(dst)
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	since := func() time.Duration { return time.Duration(time.Now().UnixNano() - last.Load()) }
	done := make(chan struct{})

	go func() { // app -> server
		defer close(done)
		buf := make([]byte, 65535)
		for {
			conn.SetReadDeadline(time.Now().Add(idle))
			n, err := conn.Read(buf)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() && since() < idle {
					continue
				}
				return
			}
			last.Store(time.Now().UnixNano())
			if n <= bond.MaxDatagram {
				u.WriteTo(append([]byte(nil), buf[:n]...), dst)
			}
		}
	}()
	// server -> app
	defer conn.Close()
	defer u.Close()
	for {
		select {
		case <-done:
			return
		default:
		}
		data, _, err := u.ReadFrom(2 * time.Second)
		if err == bond.ErrTimeout {
			continue
		}
		if err != nil {
			return
		}
		last.Store(time.Now().UnixNano())
		conn.Write(data)
	}
}
