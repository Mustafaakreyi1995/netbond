package bond

import (
	"bufio"
	"encoding/binary"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pingInterval  = 3 * time.Second
	linkTimeout   = 12 * time.Second
	writeTimeout  = 15 * time.Second
	noLinkGrace   = 30 * time.Second
	tombstoneTTL  = 60 * time.Second
	maxStreams    = 8192
	udpQueueLimit = 1024
)

// LinkStat describes one link for display.
type LinkStat struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Up    bool   `json:"up"`
	RTTms int64  `json:"rtt"`
	Tx    uint64 `json:"tx"`
	Rx    uint64 `json:"rx"`
}

type link struct {
	s    *Session
	conn net.Conn
	id   string
	name string

	slot chan *chunk
	ctl  chan frame
	done chan struct{}
	once sync.Once

	srtt     atomic.Int64 // microseconds, 0 = unknown
	lastRecv atomic.Int64 // unix nano
	cnt      *Counters
}

// Counters holds byte totals for a link; the caller may keep one across
// reconnects.
type Counters struct {
	Tx, Rx atomic.Uint64
	RTT    atomic.Int64 // microseconds
}

func (l *link) close() {
	l.once.Do(func() {
		close(l.done)
		l.conn.Close()
	})
}

// Session is one logical connection between a client and the server,
// carried over any number of links.
type Session struct {
	isServer bool
	logf     func(string, ...any)

	mu       sync.Mutex
	links    map[*link]struct{}
	streams  map[uint32]*Stream
	tombs    map[uint32]time.Time
	nextID   uint32
	noLinkAt time.Time
	isClosed bool

	ctl   *fq // reliable control frames (ACK, RST)
	dgram *fq // datagrams, dropped when the queue is full
	rtx   *fq // chunks needing retransmission
	dataQ chan *chunk
	ready chan struct{}

	closed chan struct{}

	accept func(*Stream)             // server: new inbound stream
	onUDP  func(id uint32, p []byte) // datagram handler
	onDead func()                    // called once when the session closes

	tx, rx atomic.Uint64
}

func newSession(isServer bool, logf func(string, ...any)) *Session {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	s := &Session{
		isServer: isServer,
		logf:     logf,
		links:    make(map[*link]struct{}),
		streams:  make(map[uint32]*Stream),
		tombs:    make(map[uint32]time.Time),
		nextID:   1,
		noLinkAt: time.Now(),
		ctl:      newFQ(0),
		dgram:    newFQ(udpQueueLimit),
		rtx:      newFQ(0),
		dataQ:    make(chan *chunk),
		ready:    make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	go s.dispatch()
	go s.janitor()
	return s
}

// Close shuts the session down and resets every stream.
func (s *Session) Close() {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return
	}
	s.isClosed = true
	close(s.closed)
	links := make([]*link, 0, len(s.links))
	for l := range s.links {
		links = append(links, l)
	}
	s.mu.Unlock()
	for _, l := range links {
		l.close()
	}
	s.resetAll()
	if s.onDead != nil {
		s.onDead()
	}
}

// resetAll aborts every stream locally (no RST is sent; used when the peer
// has lost the session or all links have been gone too long).
func (s *Session) resetAll() {
	s.mu.Lock()
	sts := make([]*Stream, 0, len(s.streams))
	for _, st := range s.streams {
		sts = append(sts, st)
	}
	s.mu.Unlock()
	for _, st := range sts {
		st.finalize(ErrReset)
	}
}

func (s *Session) signalReady() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// OpenStream creates a new outbound stream.
func (s *Session) OpenStream() (*Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed {
		return nil, ErrReset
	}
	if len(s.streams) >= maxStreams {
		return nil, ErrReset
	}
	id := s.nextID
	s.nextID++
	if s.nextID == 0 {
		s.nextID = 1
	}
	st := newStream(s, id)
	s.streams[id] = st
	return st, nil
}

func (s *Session) removeStream(id uint32) {
	s.mu.Lock()
	if _, ok := s.streams[id]; ok {
		delete(s.streams, id)
		s.tombs[id] = time.Now()
	}
	s.mu.Unlock()
}

// NumLinks returns the number of live links.
func (s *Session) NumLinks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.links)
}

// NumStreams returns the number of open streams.
func (s *Session) NumStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) linkStats() []LinkStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LinkStat, 0, len(s.links))
	for l := range s.links {
		out = append(out, LinkStat{ID: l.id, Name: l.name, Up: true,
			RTTms: l.srtt.Load() / 1000, Tx: l.cnt.Tx.Load(), Rx: l.cnt.Rx.Load()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// dispatch hands fresh chunks to the best link that has room: the link with
// the lowest round-trip time whose slot is free. A link's slot frees up only
// when its writer is not blocked by the kernel, so faster links naturally
// take more chunks and traffic spills onto slower links under load.
func (s *Session) dispatch() {
	for {
		var c *chunk
		select {
		case c = <-s.dataQ:
		case <-s.closed:
			return
		}
		for {
			if c.st.dead.Load() {
				break
			}
			if s.assign(c) {
				break
			}
			select {
			case <-s.ready:
			case <-c.st.done:
			case <-s.closed:
				return
			}
		}
	}
}

// assign places c in the slot of the best ready link. It runs under the
// session lock so a link cannot be detached between being chosen and used.
func (s *Session) assign(c *chunk) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *link
	var bestRTT int64
	for l := range s.links {
		if len(l.slot) != 0 {
			continue
		}
		rtt := l.srtt.Load()
		if rtt == 0 {
			rtt = 150000
		}
		if best == nil || rtt < bestRTT {
			best, bestRTT = l, rtt
		}
	}
	if best == nil {
		return false
	}
	best.slot <- c
	return true
}

func (s *Session) kick() {
	for _, q := range []*fq{s.ctl, s.rtx, s.dgram} {
		select {
		case q.sig <- struct{}{}:
		default:
		}
	}
}

func (s *Session) janitor() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
		}
		now := time.Now()
		s.mu.Lock()
		for id, at := range s.tombs {
			if now.Sub(at) > tombstoneTTL {
				delete(s.tombs, id)
			}
		}
		expired := len(s.links) == 0 && now.Sub(s.noLinkAt) > noLinkGrace
		if expired {
			s.noLinkAt = now
		}
		s.mu.Unlock()
		if expired {
			if s.isServer {
				s.Close()
				return
			}
			s.resetAll()
		}
	}
}

// runLink attaches conn as a link and blocks until the link dies.
func (s *Session) runLink(conn net.Conn, id, name string, cnt *Counters) {
	if cnt == nil {
		cnt = &Counters{}
	}
	l := &link{
		s: s, conn: conn, id: id, name: name, cnt: cnt,
		slot: make(chan *chunk, 1),
		ctl:  make(chan frame, 16),
		done: make(chan struct{}),
	}
	l.lastRecv.Store(time.Now().UnixNano())

	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		conn.Close()
		return
	}
	s.links[l] = struct{}{}
	s.mu.Unlock()
	s.signalReady()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); l.readLoop(); l.close() }()
	go func() { defer wg.Done(); l.writeLoop(); l.close() }()
	go func() { defer wg.Done(); l.pingLoop(); l.close() }()
	wg.Wait()
	s.detach(l)
}

// detach removes a dead link and re-queues everything that might have been
// lost with it.
func (s *Session) detach(l *link) {
	s.mu.Lock()
	delete(s.links, l)
	if len(s.links) == 0 {
		s.noLinkAt = time.Now()
	}
	sts := make([]*Stream, 0, len(s.streams))
	for _, st := range s.streams {
		sts = append(sts, st)
	}
	now := time.Now()
	var recent []uint32
	for id, at := range s.tombs {
		if now.Sub(at) < noLinkGrace {
			recent = append(recent, id)
		}
	}
	closed := s.isClosed
	s.mu.Unlock()
	if closed {
		return
	}

	// chunk parked in the slot but never written
	select {
	case c := <-l.slot:
		s.rtx.push(frame{typ: tData, chunk: c})
	default:
	}

	for _, st := range sts {
		st.mu.Lock()
		var lost []*chunk
		for _, c := range st.unacked {
			if c.link.Load() == l {
				lost = append(lost, c)
			}
		}
		consumed := st.consumed
		st.mu.Unlock()
		sort.Slice(lost, func(i, j int) bool { return lost[i].seq < lost[j].seq })
		for _, c := range lost {
			c.link.Store(nil)
			s.rtx.push(frame{typ: tData, chunk: c})
		}
		// our last ACK may have been on the dead link
		s.ctl.push(frame{typ: tAck, stream: st.id, seq: consumed})
	}
	for _, id := range recent {
		s.ctl.push(frame{typ: tRst, stream: id})
	}
	s.signalReady()
}

func (l *link) pingLoop() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	l.sendPing()
	for {
		select {
		case <-l.done:
			return
		case <-l.s.closed:
			return
		case now := <-t.C:
			if now.UnixNano()-l.lastRecv.Load() > int64(linkTimeout) {
				l.s.logf("link %s: timed out", l.name)
				return
			}
			l.sendPing()
		}
	}
}

func (l *link) sendPing() {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(time.Now().UnixNano()))
	select {
	case l.ctl <- frame{typ: tPing, payload: p}:
	default:
	}
}

func (l *link) readLoop() {
	br := bufio.NewReaderSize(l.conn, 64*1024)
	hdr := make([]byte, hdrLen)
	for {
		f, err := readFrame(br, hdr)
		if err != nil {
			return
		}
		n := uint64(hdrLen + len(f.payload))
		l.cnt.Rx.Add(n)
		l.s.rx.Add(n)
		l.lastRecv.Store(time.Now().UnixNano())
		switch f.typ {
		case tPing:
			select {
			case l.ctl <- frame{typ: tPong, payload: f.payload}:
			default:
			}
		case tPong:
			if len(f.payload) == 8 {
				sent := int64(binary.BigEndian.Uint64(f.payload))
				rtt := (time.Now().UnixNano() - sent) / 1000
				if rtt < 1 {
					rtt = 1
				}
				old := l.srtt.Load()
				if old == 0 {
					l.srtt.Store(rtt)
				} else {
					l.srtt.Store((old*7 + rtt) / 8)
				}
				l.cnt.RTT.Store(l.srtt.Load())
			}
		default:
			l.s.handleFrame(f)
		}
	}
}

func (s *Session) handleFrame(f frame) {
	switch f.typ {
	case tData:
		s.mu.Lock()
		st := s.streams[f.stream]
		var isNew bool
		if st == nil {
			_, dead := s.tombs[f.stream]
			if dead || s.isClosed {
				s.mu.Unlock()
				return
			}
			if !s.isServer || s.accept == nil || len(s.streams) >= maxStreams {
				s.mu.Unlock()
				s.ctl.push(frame{typ: tRst, stream: f.stream})
				return
			}
			st = newStream(s, f.stream)
			s.streams[f.stream] = st
			isNew = true
		}
		s.mu.Unlock()
		if isNew {
			go s.accept(st)
		}
		st.recvData(f.seq, f.flags&flagFin != 0, f.payload)
	case tAck:
		if st := s.lookup(f.stream); st != nil {
			st.onAck(f.seq)
		}
	case tRst:
		if st := s.lookup(f.stream); st != nil {
			st.onRst()
		}
	case tUDP:
		if s.onUDP != nil {
			s.onUDP(f.stream, f.payload)
		}
	}
}

func (s *Session) lookup(id uint32) *Stream {
	s.mu.Lock()
	st := s.streams[id]
	s.mu.Unlock()
	return st
}

func (l *link) writeLoop() {
	s := l.s
	bw := bufio.NewWriterSize(l.conn, 32*1024)
	hdr := make([]byte, hdrLen)
	defer s.kick() // a wake-up this writer swallowed must not be lost
	for {
		var f frame
		got := false
		// control traffic first
		select {
		case f = <-l.ctl:
			got = true
		default:
		}
		if !got {
			if f, got = s.ctl.pop(); !got {
				if f, got = s.rtx.pop(); !got {
					f, got = s.dgram.pop()
				}
			}
		}
		if !got {
			select {
			case c := <-l.slot:
				f, got = frame{typ: tData, chunk: c}, true
				s.signalReady()
			default:
			}
		}
		if !got {
			if bw.Buffered() > 0 {
				l.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if err := bw.Flush(); err != nil {
					return
				}
			}
			select {
			case f = <-l.ctl:
			case <-s.ctl.sig:
				continue
			case <-s.rtx.sig:
				continue
			case <-s.dgram.sig:
				continue
			case c := <-l.slot:
				f = frame{typ: tData, chunk: c}
				s.signalReady()
			case <-l.done:
				return
			case <-s.closed:
				return
			}
		}
		if c := f.chunk; c != nil {
			if c.st.dead.Load() || c.seq < c.st.ackedA.Load() {
				continue
			}
			c.link.Store(l)
			f.stream = c.st.id
			f.seq = c.seq
			f.payload = c.data
			if c.fin {
				f.flags = flagFin
			}
		}
		putHeader(hdr, &f)
		l.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := bw.Write(hdr); err != nil {
			l.requeue(f)
			return
		}
		if len(f.payload) > 0 {
			if _, err := bw.Write(f.payload); err != nil {
				l.requeue(f)
				return
			}
		}
		n := uint64(hdrLen + len(f.payload))
		l.cnt.Tx.Add(n)
		s.tx.Add(n)
	}
}

// requeue puts a reliable control frame back when the link failed mid-write.
// Data chunks are recovered by detach via their link pointer.
func (l *link) requeue(f frame) {
	if f.chunk == nil && (f.typ == tAck || f.typ == tRst) {
		l.s.ctl.push(f)
	}
}
