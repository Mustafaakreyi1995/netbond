package bond

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sort"
	"sync"
	"time"
)

const helloMagic = "NBND1"

// DialFunc opens a raw TCP connection to the server over one particular
// network.
type DialFunc func(ctx context.Context) (net.Conn, error)

type clink struct {
	id, name string
	dial     DialFunc
	cancel   context.CancelFunc
	cnt      Counters
	up       bool
	lastErr  string
}

// Client is the phone side of a bonded session.
type Client struct {
	sess   *Session
	secret string
	sid    [16]byte
	logf   func(string, ...any)

	mu        sync.Mutex
	epoch     uint64
	haveEpoch bool
	links     map[string]*clink
	udp       map[uint32]*UDPConn
	nextUDP   uint32
	closed    bool
}

// NewClient creates a client; add links with AddLink.
func NewClient(secret string, logf func(string, ...any)) *Client {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	c := &Client{
		secret: secret,
		logf:   logf,
		links:  make(map[string]*clink),
		udp:    make(map[uint32]*UDPConn),
	}
	rand.Read(c.sid[:])
	c.sess = newSession(false, logf)
	c.sess.onUDP = c.handleUDP
	return c
}

// Close stops all links and resets all streams.
func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	for _, l := range c.links {
		l.cancel()
	}
	c.links = map[string]*clink{}
	c.mu.Unlock()
	c.sess.Close()
}

// AddLink starts maintaining a link over the network reached by dial. The
// link reconnects by itself until RemoveLink is called.
func (c *Client) AddLink(id, name string, dial DialFunc) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if old := c.links[id]; old != nil {
		old.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &clink{id: id, name: name, dial: dial, cancel: cancel}
	c.links[id] = l
	c.mu.Unlock()
	go c.maintain(ctx, l)
}

// RemoveLink stops a link.
func (c *Client) RemoveLink(id string) {
	c.mu.Lock()
	l := c.links[id]
	delete(c.links, id)
	c.mu.Unlock()
	if l != nil {
		l.cancel()
	}
}

func (c *Client) maintain(ctx context.Context, l *clink) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := c.runOnce(ctx, l)
		c.mu.Lock()
		l.up = false
		if err != nil {
			l.lastErr = err.Error()
		}
		c.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logf("%s: %v", l.name, err)
		}
		if time.Since(start) > 20*time.Second {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) runOnce(ctx context.Context, l *clink) error {
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := l.dial(dctx)
	if err != nil {
		return err
	}
	tuneTCP(raw, 64*1024)
	tc := tls.Client(raw, clientTLS(c.secret))
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	if err := tc.HandshakeContext(dctx); err != nil {
		raw.Close()
		return err
	}
	tc.SetDeadline(time.Now().Add(10 * time.Second))
	name := l.name
	if len(name) > 200 {
		name = name[:200]
	}
	hello := make([]byte, 0, 64+len(name))
	hello = append(hello, helloMagic...)
	hello = append(hello, authToken(c.secret)...)
	hello = append(hello, c.sid[:]...)
	hello = append(hello, byte(len(name)))
	hello = append(hello, name...)
	if _, err := tc.Write(hello); err != nil {
		tc.Close()
		return err
	}
	var reply [9]byte
	if _, err := io.ReadFull(tc, reply[:]); err != nil {
		tc.Close()
		return errors.New("server rejected the connection")
	}
	if reply[0] != 0 {
		tc.Close()
		return errors.New("server refused: bad secret")
	}
	tc.SetDeadline(time.Time{})
	epoch := binary.BigEndian.Uint64(reply[1:])

	c.mu.Lock()
	changed := c.haveEpoch && c.epoch != epoch
	c.epoch, c.haveEpoch = epoch, true
	l.up = true
	l.lastErr = ""
	c.mu.Unlock()
	if changed {
		// the server lost our session (restart or long outage)
		c.sess.resetAll()
	}
	c.logf("%s: connected", l.name)
	c.sess.runLink(tc, l.id, l.name, &l.cnt)
	if ctx.Err() != nil {
		return nil
	}
	return errors.New("link lost")
}

// DialTCP opens a stream that the server connects to addr ("host:port").
func (c *Client) DialTCP(addr string) (*Stream, error) {
	if len(addr) == 0 || len(addr) > 300 {
		return nil, errors.New("bad address")
	}
	st, err := c.sess.OpenStream()
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, 2+len(addr))
	binary.BigEndian.PutUint16(hdr, uint16(len(addr)))
	copy(hdr[2:], addr)
	if _, err := st.Write(hdr); err != nil {
		st.Close()
		return nil, err
	}
	return st, nil
}

// Stats returns per-link state, sorted by id.
func (c *Client) Stats() []LinkStat {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]LinkStat, 0, len(c.links))
	for _, l := range c.links {
		ls := LinkStat{ID: l.id, Name: l.name, Up: l.up, Tx: l.cnt.Tx.Load(), Rx: l.cnt.Rx.Load()}
		if l.up {
			ls.RTTms = l.cnt.RTT.Load() / 1000
		}
		out = append(out, ls)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NumStreams returns the number of open streams.
func (c *Client) NumStreams() int { return c.sess.NumStreams() }

// UDPConn is a datagram association relayed through the server.
type UDPConn struct {
	c    *Client
	id   uint32
	in   chan udpMsg
	done chan struct{}
	once sync.Once
}

type udpMsg struct {
	from netip.AddrPort
	data []byte
}

// DialUDP creates a datagram association.
func (c *Client) DialUDP() *UDPConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextUDP++
	u := &UDPConn{c: c, id: c.nextUDP, in: make(chan udpMsg, 128), done: make(chan struct{})}
	c.udp[u.id] = u
	return u
}

func (c *Client) handleUDP(id uint32, p []byte) {
	c.mu.Lock()
	u := c.udp[id]
	c.mu.Unlock()
	if u == nil {
		return
	}
	from, data, err := decodeUDP(p)
	if err != nil {
		return
	}
	select {
	case u.in <- udpMsg{from, data}:
	default:
	}
}

// WriteTo sends one datagram to addr via the server. Datagrams are dropped
// when the tunnel is congested, like UDP anywhere.
func (u *UDPConn) WriteTo(data []byte, addr netip.AddrPort) error {
	if len(data) > MaxDatagram {
		return errors.New("datagram too large")
	}
	u.c.sess.dgram.push(frame{typ: tUDP, stream: u.id, payload: encodeUDP(addr, data)})
	return nil
}

// ErrTimeout is returned by ReadFrom when nothing arrives in time.
var ErrTimeout = errors.New("timeout")

// ReadFrom waits up to timeout for a datagram.
func (u *UDPConn) ReadFrom(timeout time.Duration) ([]byte, netip.AddrPort, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case m := <-u.in:
		return m.data, m.from, nil
	case <-u.done:
		return nil, netip.AddrPort{}, net.ErrClosed
	case <-t.C:
		return nil, netip.AddrPort{}, ErrTimeout
	}
}

// Close releases the association.
func (u *UDPConn) Close() {
	u.once.Do(func() {
		close(u.done)
		u.c.mu.Lock()
		delete(u.c.udp, u.id)
		u.c.mu.Unlock()
	})
}
