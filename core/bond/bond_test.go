package bond

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// shaper is a TCP forwarder that limits throughput, standing in for a
// network of a given speed.
type shaper struct {
	ln    net.Listener
	rate  int // bytes per second each way
	mu    sync.Mutex
	conns []net.Conn
	dead  bool
}

func newShaper(t *testing.T, upstream string, rate int) *shaper {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sh := &shaper{ln: ln, rate: rate}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", upstream)
			if err != nil {
				c.Close()
				continue
			}
			sh.mu.Lock()
			if sh.dead {
				sh.mu.Unlock()
				c.Close()
				u.Close()
				continue
			}
			sh.conns = append(sh.conns, c, u)
			sh.mu.Unlock()
			for _, x := range []net.Conn{c, u} {
				x.(*net.TCPConn).SetReadBuffer(32 * 1024)
				x.(*net.TCPConn).SetWriteBuffer(32 * 1024)
			}
			go sh.pipe(c, u)
			go sh.pipe(u, c)
		}
	}()
	return sh
}

func (sh *shaper) pipe(a, b net.Conn) {
	defer a.Close()
	defer b.Close()
	buf := make([]byte, 4096)
	for {
		n, err := a.Read(buf)
		if n > 0 {
			time.Sleep(time.Duration(float64(n) / float64(sh.rate) * float64(time.Second)))
			if _, werr := b.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (sh *shaper) kill() {
	sh.mu.Lock()
	sh.dead = true
	for _, c := range sh.conns {
		c.Close()
	}
	sh.mu.Unlock()
}

func (sh *shaper) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", sh.ln.Addr().String())
}

type env struct {
	t       *testing.T
	srvAddr string
	echo    string
	sink    string
	client  *Client
}

func setup(t *testing.T) *env {
	secret := NewSecret()
	sv, err := NewServer(secret, nil)
	if err != nil {
		t.Fatal(err)
	}
	sv.AllowLocal = true
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go sv.Serve(ln)
	t.Cleanup(func() { ln.Close() })

	// echo target
	eln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			c, err := eln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.(*net.TCPConn).CloseWrite() }()
		}
	}()
	t.Cleanup(func() { eln.Close() })

	// source target: sends N bytes requested in an 8-byte header... kept simple:
	// reads everything until EOF, then replies with its SHA-256.
	sln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			c, err := sln.Accept()
			if err != nil {
				return
			}
			go func() {
				h := sha256.New()
				io.Copy(h, c)
				c.Write(h.Sum(nil))
				c.Close()
			}()
		}
	}()
	t.Cleanup(func() { sln.Close() })

	cl := NewClient(secret, nil)
	t.Cleanup(cl.Close)
	return &env{t: t, srvAddr: ln.Addr().String(), echo: eln.Addr().String(), sink: sln.Addr().String(), client: cl}
}

func waitLinks(t *testing.T, c *Client, n int) {
	for i := 0; i < 100; i++ {
		if c.sess.NumLinks() >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("links did not come up")
}

// upload sends size random bytes to the hashing target and checks the hash.
func upload(t *testing.T, c *Client, addr string, size int) time.Duration {
	data := make([]byte, size)
	rand.Read(data)
	want := sha256.Sum256(data)
	st, err := c.DialTCP(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	start := time.Now()
	if _, err := st.Write(data); err != nil {
		t.Fatal(err)
	}
	st.CloseWrite()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("hash mismatch (%d bytes back)", len(got))
	}
	return time.Since(start)
}

func TestAggregation(t *testing.T) {
	e := setup(t)
	const mb = 1 << 20
	a := newShaper(t, e.srvAddr, 2*mb)
	b := newShaper(t, e.srvAddr, 1*mb)
	e.client.AddLink("a", "fast", a.dial)
	waitLinks(t, e.client, 1)
	one := upload(t, e.client, e.sink, 6*mb)
	e.client.AddLink("b", "slow", b.dial)
	waitLinks(t, e.client, 2)
	two := upload(t, e.client, e.sink, 6*mb)
	r1 := 6 / one.Seconds()
	r2 := 6 / two.Seconds()
	t.Logf("one link: %.2f MB/s, two links: %.2f MB/s", r1, r2)
	if r2 < r1*1.15 {
		t.Fatalf("no aggregation: %.2f vs %.2f MB/s", r2, r1)
	}
	for _, s := range e.client.Stats() {
		t.Logf("%+v", s)
	}
}

func TestEchoDownload(t *testing.T) {
	e := setup(t)
	const mb = 1 << 20
	a := newShaper(t, e.srvAddr, 3*mb)
	b := newShaper(t, e.srvAddr, 1*mb)
	e.client.AddLink("a", "a", a.dial)
	e.client.AddLink("b", "b", b.dial)
	waitLinks(t, e.client, 2)
	data := make([]byte, 5*mb)
	rand.Read(data)
	st, err := e.client.DialTCP(e.echo)
	if err != nil {
		t.Fatal(err)
	}
	go func() { st.Write(data); st.CloseWrite() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch: err=%v len=%d", err, len(got))
	}
	time.Sleep(200 * time.Millisecond)
	if n := e.client.NumStreams(); n != 0 {
		t.Fatalf("stream leak: %d", n)
	}
}

func TestFailover(t *testing.T) {
	e := setup(t)
	const mb = 1 << 20
	a := newShaper(t, e.srvAddr, 2*mb)
	b := newShaper(t, e.srvAddr, 2*mb)
	e.client.AddLink("a", "a", a.dial)
	e.client.AddLink("b", "b", b.dial)
	waitLinks(t, e.client, 2)
	go func() { time.Sleep(700 * time.Millisecond); a.kill() }()
	// echo exercises both directions across the link death
	data := make([]byte, 6*mb)
	rand.Read(data)
	st, err := e.client.DialTCP(e.echo)
	if err != nil {
		t.Fatal(err)
	}
	go func() { st.Write(data); st.CloseWrite() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("failover corrupted stream: err=%v len=%d want=%d", err, len(got), len(data))
	}
}

func TestManyStreams(t *testing.T) {
	e := setup(t)
	const mb = 1 << 20
	a := newShaper(t, e.srvAddr, 4*mb)
	b := newShaper(t, e.srvAddr, 4*mb)
	e.client.AddLink("a", "a", a.dial)
	e.client.AddLink("b", "b", b.dial)
	waitLinks(t, e.client, 2)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			upload(t, e.client, e.sink, 100*1024+i*777)
		}(i)
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	if n := e.client.NumStreams(); n != 0 {
		t.Fatalf("stream leak: %d", n)
	}
}

func TestDialFailure(t *testing.T) {
	e := setup(t)
	a := newShaper(t, e.srvAddr, 1<<20)
	e.client.AddLink("a", "a", a.dial)
	waitLinks(t, e.client, 1)
	st, err := e.client.DialTCP("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 10)
	if _, err := st.Read(buf); err != ErrReset {
		t.Fatalf("want reset, got %v", err)
	}
}

func TestBadSecret(t *testing.T) {
	e := setup(t)
	bad := NewClient(NewSecret(), nil)
	defer bad.Close()
	bad.AddLink("a", "a", func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", e.srvAddr)
	})
	time.Sleep(500 * time.Millisecond)
	if bad.sess.NumLinks() != 0 {
		t.Fatal("link came up with the wrong secret")
	}
}

func TestUDP(t *testing.T) {
	e := setup(t)
	pc, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer pc.Close()
	go func() {
		buf := make([]byte, 2000)
		for {
			n, from, err := pc.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			pc.WriteToUDPAddrPort(buf[:n], from)
		}
	}()
	a := newShaper(t, e.srvAddr, 1<<20)
	e.client.AddLink("a", "a", a.dial)
	waitLinks(t, e.client, 1)
	u := e.client.DialUDP()
	defer u.Close()
	dst := pc.LocalAddr().(*net.UDPAddr).AddrPort()
	msg := []byte("hello over udp")
	u.WriteTo(msg, dst)
	got, from, err := u.ReadFrom(3 * time.Second)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("udp echo failed: %v %q", err, got)
	}
	if from != netip.AddrPortFrom(dst.Addr().Unmap(), dst.Port()) {
		t.Fatalf("wrong source %v, want %v", from, dst)
	}
}
