package bond

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ErrReset is returned by stream operations after the stream was aborted.
var ErrReset = errors.New("bond: stream reset")

// chunk is one piece of a stream held by the sender until the peer has
// consumed it, so it can be re-sent if the link that carried it dies.
type chunk struct {
	st   *Stream
	seq  uint32
	fin  bool
	data []byte
	link atomic.Pointer[link] // link it was last written to
}

type rchunk struct {
	data []byte
	fin  bool
}

// Stream is a reliable, ordered byte stream inside a Session.
type Stream struct {
	s  *Session
	id uint32

	mu    sync.Mutex
	rcond *sync.Cond
	wcond *sync.Cond

	// send side
	sendSeq uint32
	acked   uint32
	ackedA  atomic.Uint32
	unacked map[uint32]*chunk
	finSent bool

	// receive side
	next     uint32
	reorder  map[uint32]rchunk
	q        [][]byte
	qoff     int
	consumed uint32
	lastAck  uint32
	eof      bool
	eofRead  bool

	err  error
	dead atomic.Bool
	done chan struct{}
}

func newStream(s *Session, id uint32) *Stream {
	st := &Stream{
		s:       s,
		id:      id,
		unacked: make(map[uint32]*chunk),
		reorder: make(map[uint32]rchunk),
		done:    make(chan struct{}),
	}
	st.rcond = sync.NewCond(&st.mu)
	st.wcond = sync.NewCond(&st.mu)
	return st
}

// ID returns the stream identifier.
func (st *Stream) ID() uint32 { return st.id }

func (st *Stream) sendChunk(data []byte, fin bool) error {
	st.mu.Lock()
	for st.err == nil && st.sendSeq-st.acked >= Window {
		st.wcond.Wait()
	}
	if st.err != nil {
		err := st.err
		st.mu.Unlock()
		return err
	}
	if st.finSent {
		st.mu.Unlock()
		return io.ErrClosedPipe
	}
	c := &chunk{st: st, seq: st.sendSeq, fin: fin, data: data}
	st.sendSeq++
	st.unacked[c.seq] = c
	if fin {
		st.finSent = true
	}
	st.mu.Unlock()

	select {
	case st.s.dataQ <- c:
		return nil
	case <-st.done:
		return st.errOr(ErrReset)
	case <-st.s.closed:
		return ErrReset
	}
}

func (st *Stream) errOr(def error) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.err != nil {
		return st.err
	}
	return def
}

// Write sends p. It blocks while the flow-control window is full. Only one
// goroutine may write at a time.
func (st *Stream) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		k := len(p)
		if k > MaxChunk {
			k = MaxChunk
		}
		buf := make([]byte, k)
		copy(buf, p)
		if err := st.sendChunk(buf, false); err != nil {
			return n, err
		}
		n += k
		p = p[k:]
	}
	return n, nil
}

// CloseWrite signals end-of-stream to the peer; reads remain possible.
func (st *Stream) CloseWrite() error {
	st.mu.Lock()
	if st.finSent || st.err != nil {
		st.mu.Unlock()
		return nil
	}
	st.mu.Unlock()
	return st.sendChunk(nil, true)
}

// Read reads the next bytes in order.
func (st *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	st.mu.Lock()
	for len(st.q) == 0 && !st.eof && st.err == nil {
		st.rcond.Wait()
	}
	if len(st.q) > 0 {
		n := copy(p, st.q[0][st.qoff:])
		st.qoff += n
		ack := uint32(0)
		sendAck := false
		if st.qoff >= len(st.q[0]) {
			st.q[0] = nil
			st.q = st.q[1:]
			if len(st.q) == 0 {
				st.q = nil
			}
			st.qoff = 0
			st.consumed++
			if len(st.q) == 0 || st.consumed-st.lastAck >= Window/8 {
				st.lastAck = st.consumed
				ack = st.consumed
				sendAck = true
			}
		}
		st.mu.Unlock()
		if sendAck {
			st.s.ctl.push(frame{typ: tAck, stream: st.id, seq: ack})
		}
		return n, nil
	}
	if st.err != nil {
		err := st.err
		st.mu.Unlock()
		return 0, err
	}
	// eof
	sendAck := false
	ack := uint32(0)
	if !st.eofRead {
		st.eofRead = true
		st.consumed++
		st.lastAck = st.consumed
		ack = st.consumed
		sendAck = true
	}
	fin := st.finishedLocked()
	st.mu.Unlock()
	if sendAck {
		st.s.ctl.push(frame{typ: tAck, stream: st.id, seq: ack})
	}
	if fin {
		st.finalize(nil)
	}
	return 0, io.EOF
}

// finishedLocked reports whether both directions completed cleanly.
func (st *Stream) finishedLocked() bool {
	return st.eofRead && st.finSent && st.acked == st.sendSeq
}

func (st *Stream) recvData(seq uint32, fin bool, payload []byte) {
	st.mu.Lock()
	if st.err != nil || seq < st.next {
		st.mu.Unlock()
		return
	}
	if _, dup := st.reorder[seq]; dup {
		st.mu.Unlock()
		return
	}
	if seq-st.consumed > 2*Window {
		st.mu.Unlock()
		st.Reset()
		return
	}
	st.reorder[seq] = rchunk{data: payload, fin: fin}
	advanced := false
	for {
		rc, ok := st.reorder[st.next]
		if !ok {
			break
		}
		delete(st.reorder, st.next)
		st.next++
		advanced = true
		if rc.fin {
			st.eof = true
			break
		}
		if len(rc.data) > 0 {
			st.q = append(st.q, rc.data)
		} else {
			// empty non-fin chunk: nothing to read, count it consumed
			st.consumed++
		}
	}
	if advanced {
		st.rcond.Broadcast()
	}
	st.mu.Unlock()
}

func (st *Stream) onAck(n uint32) {
	st.mu.Lock()
	if n > st.acked && n <= st.sendSeq {
		for i := st.acked; i < n; i++ {
			delete(st.unacked, i)
		}
		st.acked = n
		st.ackedA.Store(n)
		st.wcond.Broadcast()
	}
	fin := st.finishedLocked()
	st.mu.Unlock()
	if fin {
		st.finalize(nil)
	}
}

// finalize tears the stream down locally. err == nil means a clean finish.
func (st *Stream) finalize(err error) {
	st.mu.Lock()
	if st.dead.Load() {
		st.mu.Unlock()
		return
	}
	st.dead.Store(true)
	if st.err == nil {
		if err == nil {
			err = io.ErrClosedPipe
		}
		st.err = err
	}
	st.unacked = nil
	st.reorder = nil
	if err != nil && err != io.ErrClosedPipe {
		st.q = nil
	}
	close(st.done)
	st.rcond.Broadcast()
	st.wcond.Broadcast()
	st.mu.Unlock()
	st.s.removeStream(st.id)
}

// Reset aborts the stream and tells the peer.
func (st *Stream) Reset() {
	if st.dead.Load() {
		return
	}
	st.finalize(ErrReset)
	st.s.ctl.push(frame{typ: tRst, stream: st.id})
}

func (st *Stream) onRst() { st.finalize(ErrReset) }

// Close releases the stream. If both directions already ended cleanly it
// lingers until the peer confirms it has read everything; otherwise the peer
// is told to abort.
func (st *Stream) Close() error {
	st.mu.Lock()
	graceful := st.err == nil && st.finSent && st.eofRead
	st.mu.Unlock()
	if graceful {
		time.AfterFunc(lingerTimeout, st.Reset)
		return nil
	}
	st.Reset()
	return nil
}

const lingerTimeout = 30 * time.Second

// Done is closed when the stream is finished or reset.
func (st *Stream) Done() <-chan struct{} { return st.done }
