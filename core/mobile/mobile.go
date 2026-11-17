// Package mobile is the API the Android app calls (bound with gomobile).
package mobile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"

	"netbond.app/core/bond"
)

// Callbacks is implemented by the Android service.
type Callbacks interface {
	// BindSocket must bind the socket to the Android network that backs
	// linkID (Network.bindSocket) and report whether that worked.
	BindSocket(linkID string, fd int32) bool
	// Log receives human-readable status lines.
	Log(msg string)
}

var (
	mu     sync.Mutex
	client *bond.Client // set in server mode
	dir    *direct      // set in no-server mode
	tun    *tunStack
	cbs    Callbacks
	port   string
)

// ServerHost returns the host part of a connection code, or "" if the code
// is not valid. The app resolves it once per network.
func ServerHost(code string) string {
	_, host, _, err := bond.ParseCode(code)
	if err != nil {
		return ""
	}
	return host
}

// Start begins tunnelling. tunFd is the VPN interface descriptor; ownership
// passes to this library. An empty code selects no-server mode: every new
// connection goes straight out over one of the networks, in turn.
func Start(tunFd int32, mtu int32, code string, cb Callbacks) error {
	mu.Lock()
	defer mu.Unlock()
	if client != nil || dir != nil {
		return errors.New("already running")
	}
	if code == "" {
		d := newDirect(cb)
		t, err := newTunStack(int(tunFd), int(mtu), d, func(f string, a ...any) { cb.Log(fmt.Sprintf(f, a...)) })
		if err != nil {
			syscall.Close(int(tunFd))
			return err
		}
		dir, tun, cbs = d, t, cb
		return nil
	}
	secret, _, p, err := bond.ParseCode(code)
	if err != nil {
		syscall.Close(int(tunFd))
		return err
	}
	logf := func(f string, a ...any) { cb.Log(fmt.Sprintf(f, a...)) }
	c := bond.NewClient(secret, logf)
	t, err := newTunStack(int(tunFd), int(mtu), bonded{c}, logf)
	if err != nil {
		c.Close()
		syscall.Close(int(tunFd))
		return err
	}
	client, tun, cbs, port = c, t, cb, p
	return nil
}

// AddLink starts a link over one Android network. serverIP is the server's
// address as resolved on that network.
func AddLink(id, name, serverIP string) {
	mu.Lock()
	c, d, cb, p := client, dir, cbs, port
	mu.Unlock()
	if d != nil {
		d.addLink(id, name)
		return
	}
	if c == nil {
		return
	}
	addr := net.JoinHostPort(serverIP, p)
	c.AddLink(id, name, func(ctx context.Context) (net.Conn, error) {
		d := net.Dialer{Control: func(network, address string, rc syscall.RawConn) error {
			ok := false
			if err := rc.Control(func(fd uintptr) { ok = cb.BindSocket(id, int32(fd)) }); err != nil {
				return err
			}
			if !ok {
				return errors.New("could not bind to network")
			}
			return nil
		}}
		return d.DialContext(ctx, "tcp", addr)
	})
}

// SetLinkHint passes Android's bandwidth estimate for a network (kbps), used
// to place connections before the link's real speed has been measured.
func SetLinkHint(id string, kbps int64) {
	mu.Lock()
	d := dir
	mu.Unlock()
	if d != nil {
		d.setHint(id, kbps)
	}
}

// RemoveLink stops the link for a network that went away.
func RemoveLink(id string) {
	mu.Lock()
	c, d := client, dir
	mu.Unlock()
	if d != nil {
		d.removeLink(id)
	}
	if c != nil {
		c.RemoveLink(id)
	}
}

// Stats returns JSON: {"links":[{id,name,up,rtt,tx,rx}],"streams":n}.
func Stats() string {
	mu.Lock()
	c, d := client, dir
	mu.Unlock()
	out := struct {
		Links   []bond.LinkStat `json:"links"`
		Streams int             `json:"streams"`
	}{Links: []bond.LinkStat{}}
	if c != nil {
		out.Links = c.Stats()
		out.Streams = c.NumStreams()
	}
	if d != nil {
		out.Links, out.Streams = d.stats()
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// Stop shuts everything down.
func Stop() {
	mu.Lock()
	c, d, t := client, dir, tun
	client, dir, tun, cbs = nil, nil, nil, nil
	mu.Unlock()
	if d != nil {
		d.close()
	}
	if c != nil {
		c.Close()
	}
	if t != nil {
		t.close()
	}
}
