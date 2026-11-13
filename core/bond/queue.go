package bond

import "sync"

// fq is an unbounded FIFO of frames that several goroutines can wait on.
// Waiters select on sig and then call pop.
type fq struct {
	mu    sync.Mutex
	items []frame
	sig   chan struct{}
	limit int // 0 = unbounded; otherwise push drops when full
}

func newFQ(limit int) *fq { return &fq{sig: make(chan struct{}, 1), limit: limit} }

func (q *fq) push(f frame) bool {
	q.mu.Lock()
	if q.limit > 0 && len(q.items) >= q.limit {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, f)
	q.mu.Unlock()
	select {
	case q.sig <- struct{}{}:
	default:
	}
	return true
}

func (q *fq) pop() (frame, bool) {
	q.mu.Lock()
	if len(q.items) == 0 {
		q.mu.Unlock()
		return frame{}, false
	}
	f := q.items[0]
	q.items[0] = frame{}
	q.items = q.items[1:]
	more := len(q.items) > 0
	if !more {
		q.items = nil
	}
	q.mu.Unlock()
	if more {
		select {
		case q.sig <- struct{}{}:
		default:
		}
	}
	return f, true
}
