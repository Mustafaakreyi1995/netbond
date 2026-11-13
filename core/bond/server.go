package bond

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxUDPAssoc = 2048

// Server terminates bonded sessions and connects their streams to the
// internet.
type Server struct {
	token  []byte
	tlsCfg *tls.Config
	logf   func(string, ...any)
	dialer net.Dialer

	// AllowLocal permits connections to the server's own loopback and
	// link-local addresses. Off by default.
	AllowLocal bool

	mu       sync.Mutex
	sessions map[[16]byte]*srvSession
}

type srvSession struct {
	sv    *Server
	sess  *Session
	epoch uint64

	mu  sync.Mutex
	udp map[uint32]*srvUDP
}

type srvUDP struct {
	conn *net.UDPConn
	last atomic.Int64
}

// NewServer creates a server for the given shared secret.
func NewServer(secret string, logf func(string, ...any)) (*Server, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	cfg, err := serverTLS(secret)
	if err != nil {
		return nil, err
	}
	sv := &Server{
		token:    authToken(secret),
		tlsCfg:   cfg,
		logf:     logf,
		sessions: make(map[[16]byte]*srvSession),
	}
	sv.dialer = net.Dialer{Timeout: 10 * time.Second, Control: sv.controlDial}
	return sv, nil
}

func (sv *Server) allowed(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	if !sv.AllowLocal && (ip.IsLoopback() || ip.IsLinkLocalUnicast()) {
		return false
	}
	return true
}

func (sv *Server) controlDial(network, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return err
	}
	if !sv.allowed(ap.Addr()) {
		return errors.New("destination not allowed")
	}
	return nil
}

// Serve accepts links on ln until it is closed.
func (sv *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go sv.handleConn(conn)
	}
}

func (sv *Server) handleConn(raw net.Conn) {
	tuneTCP(raw, 128*1024)
	tc := tls.Server(raw, sv.tlsCfg)
	tc.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tc.Handshake(); err != nil {
		tc.Close()
		return
	}
	head := make([]byte, len(helloMagic)+32+16+1)
	if _, err := io.ReadFull(tc, head); err != nil {
		tc.Close()
		return
	}
	if string(head[:5]) != helloMagic || subtle.ConstantTimeCompare(head[5:37], sv.token) != 1 {
		tc.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})
		tc.Close()
		return
	}
	var sid [16]byte
	copy(sid[:], head[37:53])
	name := make([]byte, head[53])
	if _, err := io.ReadFull(tc, name); err != nil {
		tc.Close()
		return
	}

	ss := sv.session(sid)
	var reply [9]byte
	binary.BigEndian.PutUint64(reply[1:], ss.epoch)
	if _, err := tc.Write(reply[:]); err != nil {
		tc.Close()
		return
	}
	tc.SetDeadline(time.Time{})
	sv.logf("link up: %s via %s (%d streams)", raw.RemoteAddr(), name, ss.sess.NumStreams())
	ss.sess.runLink(tc, raw.RemoteAddr().String(), string(name), nil)
	sv.logf("link down: %s via %s", raw.RemoteAddr(), name)
}

func (sv *Server) session(sid [16]byte) *srvSession {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if ss := sv.sessions[sid]; ss != nil {
		return ss
	}
	ss := &srvSession{sv: sv, udp: make(map[uint32]*srvUDP)}
	var e [8]byte
	rand.Read(e[:])
	ss.epoch = binary.BigEndian.Uint64(e[:])
	ss.sess = newSession(true, sv.logf)
	ss.sess.accept = ss.handleStream
	ss.sess.onUDP = ss.handleUDP
	ss.sess.onDead = func() {
		sv.mu.Lock()
		if sv.sessions[sid] == ss {
			delete(sv.sessions, sid)
		}
		sv.mu.Unlock()
		ss.mu.Lock()
		for id, u := range ss.udp {
			u.conn.Close()
			delete(ss.udp, id)
		}
		ss.mu.Unlock()
	}
	sv.sessions[sid] = ss
	return ss
}

func (ss *srvSession) handleStream(st *Stream) {
	defer st.Close()
	var l [2]byte
	if _, err := io.ReadFull(st, l[:]); err != nil {
		return
	}
	n := int(binary.BigEndian.Uint16(l[:]))
	if n == 0 || n > 300 {
		return
	}
	addr := make([]byte, n)
	if _, err := io.ReadFull(st, addr); err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	go func() {
		select {
		case <-st.Done():
			cancel()
		case <-stop:
		}
	}()
	conn, err := ss.sv.dialer.DialContext(ctx, "tcp", string(addr))
	close(stop)
	cancel()
	if err != nil {
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	Relay(st, conn)
}

func (ss *srvSession) handleUDP(id uint32, p []byte) {
	dst, data, err := decodeUDP(p)
	if err != nil || !ss.sv.allowed(dst.Addr()) {
		return
	}
	ss.mu.Lock()
	u := ss.udp[id]
	if u == nil {
		if len(ss.udp) >= maxUDPAssoc {
			ss.mu.Unlock()
			return
		}
		conn, err := net.ListenUDP("udp", nil)
		if err != nil {
			ss.mu.Unlock()
			return
		}
		u = &srvUDP{conn: conn}
		ss.udp[id] = u
		idle := 2 * time.Minute
		if dst.Port() == 53 {
			idle = 15 * time.Second
		}
		go ss.udpReader(id, u, idle)
	}
	ss.mu.Unlock()
	u.last.Store(time.Now().UnixNano())
	if dst.Addr().Is4() {
		// the socket is dual-stack; address IPv4 peers in mapped form
		dst = netip.AddrPortFrom(netip.AddrFrom16(dst.Addr().As16()), dst.Port())
	}
	u.conn.WriteToUDPAddrPort(data, dst)
}

func (ss *srvSession) udpReader(id uint32, u *srvUDP, idle time.Duration) {
	defer func() {
		u.conn.Close()
		ss.mu.Lock()
		if ss.udp[id] == u {
			delete(ss.udp, id)
		}
		ss.mu.Unlock()
	}()
	buf := make([]byte, 65535)
	for {
		u.conn.SetReadDeadline(time.Now().Add(idle))
		n, from, err := u.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if time.Now().UnixNano()-u.last.Load() < int64(idle) {
					continue
				}
			}
			return
		}
		if n > MaxDatagram {
			continue
		}
		u.last.Store(time.Now().UnixNano())
		ss.sess.dgram.push(frame{typ: tUDP, stream: id, payload: encodeUDP(from, buf[:n])})
	}
}
