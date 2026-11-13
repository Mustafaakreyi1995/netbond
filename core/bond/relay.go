package bond

import (
	"io"
	"net"
	"sync/atomic"
	"time"
)

const halfCloseIdle = 2 * time.Minute

type closeWriter interface{ CloseWrite() error }

// Relay copies data both ways between a stream and a connection, honouring
// half-close, and returns once both directions are finished.
func Relay(st *Stream, conn net.Conn) {
	var last atomic.Int64
	touch := func() { last.Store(time.Now().UnixNano()) }
	touch()
	done := make(chan struct{}, 2)

	go func() { // conn -> stream
		defer func() { done <- struct{}{} }()
		buf := make([]byte, MaxChunk)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				touch()
				if _, werr := st.Write(buf[:n]); werr != nil {
					conn.Close()
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					st.CloseWrite()
				} else {
					st.Reset()
				}
				return
			}
		}
	}()

	go func() { // stream -> conn
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		for {
			n, err := st.Read(buf)
			if n > 0 {
				touch()
				if _, werr := conn.Write(buf[:n]); werr != nil {
					st.Reset()
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					if cw, ok := conn.(closeWriter); ok {
						cw.CloseWrite()
					} else {
						conn.Close()
					}
				} else {
					conn.Close()
				}
				return
			}
		}
	}()

	<-done
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
wait:
	for {
		select {
		case <-done:
			break wait
		case <-t.C:
			if time.Now().UnixNano()-last.Load() > int64(halfCloseIdle) {
				break wait
			}
		}
	}
	conn.Close()
	st.Close()
}
