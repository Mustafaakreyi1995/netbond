package mobile

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"

	"netbond.app/core/bond"
)

// direct is the no-server mode. There is nothing on the far side to put a
// split stream back together, so a connection stays on one network for its
// whole life; the gain comes from spreading connections across networks.
type direct struct {
	cb Callbacks

	mu     sync.Mutex
	links  map[string]*dlink
	rr     uint64
	closed bool
	conns  map[io.Closer]struct{}
}

type dlink struct {
	id, name string
	active   atomic.Int64 // open connections
	tx, rx   atomic.Uint64
	fails    atomic.Int64 // consecutive dial failures
}

func newDirect(cb Callbacks) *direct {
	return &direct{cb: cb, links: map[string]*dlink{}, conns: map[io.Closer]struct{}{}}
}

func (d *direct) addLink(id, name string) {
	d.mu.Lock()
	if !d.closed {
		d.links[id] = &dlink{id: id, name: name}
	}
	d.mu.Unlock()
}

func (d *direct) removeLink(id string) {
	d.mu.Lock()
	delete(d.links, id)
	d.mu.Unlock()
}

func (d *direct) close() {
	d.mu.Lock()
	d.closed = true
	d.links = map[string]*dlink{}
	cs := make([]io.Closer, 0, len(d.conns))
	for c := range d.conns {
		cs = append(cs, c)
	}
	d.conns = map[io.Closer]struct{}{}
	d.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

func (d *direct) track(c io.Closer) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	d.conns[c] = struct{}{}
	return true
}

func (d *direct) untrack(c io.Closer) {
	d.mu.Lock()
	delete(d.conns, c)
	d.mu.Unlock()
}

func (d *direct) stats() ([]bond.LinkStat, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]bond.LinkStat, 0, len(d.links))
	n := 0
	for _, l := range d.links {
		out = append(out, bond.LinkStat{ID: l.id, Name: l.name, Up: true, Tx: l.tx.Load(), Rx: l.rx.Load()})
		n += int(l.active.Load())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, n
}

// order returns the links to try for a new flow: fewest open connections
// first, rotating among equals, with recently failing links last.
func (d *direct) order() []*dlink {
	d.mu.Lock()
	ls := make([]*dlink, 0, len(d.links))
	for _, l := range d.links {
		ls = append(ls, l)
	}
	d.rr++
	rr := d.rr
	d.mu.Unlock()
	sort.Slice(ls, func(i, j int) bool { return ls[i].id < ls[j].id })
	if n := len(ls); n > 1 {
		k := int(rr % uint64(n))
		ls = append(ls[k:], ls[:k]...)
	}
	sort.SliceStable(ls, func(i, j int) bool {
		fi, fj := ls[i].fails.Load() > 2, ls[j].fails.Load() > 2
		if fi != fj {
			return !fi
		}
		return ls[i].active.Load() < ls[j].active.Load()
	})
	return ls
}

func (d *direct) control(l *dlink) func(string, string, syscall.RawConn) error {
	return func(_, _ string, rc syscall.RawConn) error {
		ok := false
		if err := rc.Control(func(fd uintptr) { ok = d.cb.BindSocket(l.id, int32(fd)) }); err != nil {
			return err
		}
		if !ok {
			return errors.New("could not bind to network")
		}
		return nil
	}
}

func (d *direct) serveTCP(conn *gonet.TCPConn, dst netip.AddrPort) {
	defer conn.Close()
	var out net.Conn
	var via *dlink
	for _, l := range d.order() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		dl := net.Dialer{Control: d.control(l)}
		c, err := dl.DialContext(ctx, "tcp", dst.String())
		cancel()
		if err == nil {
			l.fails.Store(0)
			out, via = c, l
			break
		}
		l.fails.Add(1)
	}
	if out == nil {
		return
	}
	defer out.Close()
	if tc, ok := out.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	if !d.track(out) {
		return
	}
	defer d.untrack(out)
	via.active.Add(1)
	defer via.active.Add(-1)

	done := make(chan struct{}, 2)
	go func() { // app -> internet
		copyCount(out, conn, &via.tx)
		if tc, ok := out.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() { // internet -> app
		copyCount(conn, out, &via.rx)
		conn.CloseWrite()
		done <- struct{}{}
	}()
	<-done
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
	}
}

func copyCount(dst io.Writer, src io.Reader, n *atomic.Uint64) {
	buf := make([]byte, 32*1024)
	for {
		k, err := src.Read(buf)
		if k > 0 {
			n.Add(uint64(k))
			if _, werr := dst.Write(buf[:k]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (d *direct) serveUDP(conn *gonet.UDPConn, dst netip.AddrPort) {
	defer conn.Close()
	network := "udp4"
	if dst.Addr().Is6() {
		network = "udp6"
	}
	var pc net.PacketConn
	var via *dlink
	for _, l := range d.order() {
		lc := net.ListenConfig{Control: d.control(l)}
		p, err := lc.ListenPacket(context.Background(), network, ":0")
		if err == nil {
			pc, via = p, l
			break
		}
	}
	if pc == nil {
		return
	}
	defer pc.Close()
	if !d.track(pc) {
		return
	}
	defer d.untrack(pc)
	uc := pc.(*net.UDPConn)
	idle := udpIdle(dst)
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	since := func() time.Duration { return time.Duration(time.Now().UnixNano() - last.Load()) }

	go func() { // internet -> app
		defer conn.Close()
		buf := make([]byte, 65535)
		for {
			uc.SetReadDeadline(time.Now().Add(idle))
			n, _, err := uc.ReadFromUDPAddrPort(buf)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() && since() < idle {
					continue
				}
				return
			}
			last.Store(time.Now().UnixNano())
			via.rx.Add(uint64(n))
			conn.Write(buf[:n])
		}
	}()
	buf := make([]byte, 65535)
	for { // app -> internet
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
		via.tx.Add(uint64(n))
		uc.WriteToUDPAddrPort(buf[:n], dst)
	}
}
