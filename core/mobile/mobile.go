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
	client *bond.Client
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
// passes to this library.
func Start(tunFd int32, mtu int32, code string, cb Callbacks) error {
	mu.Lock()
	defer mu.Unlock()
	if client != nil {
		return errors.New("already running")
	}
	secret, _, p, err := bond.ParseCode(code)
	if err != nil {
		syscall.Close(int(tunFd))
		return err
	}
	logf := func(f string, a ...any) { cb.Log(fmt.Sprintf(f, a...)) }
	c := bond.NewClient(secret, logf)
	t, err := newTunStack(int(tunFd), int(mtu), c, logf)
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
	c, cb, p := client, cbs, port
	mu.Unlock()
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

// RemoveLink stops the link for a network that went away.
func RemoveLink(id string) {
	mu.Lock()
	c := client
	mu.Unlock()
	if c != nil {
		c.RemoveLink(id)
	}
}

// Stats returns JSON: {"links":[{id,name,up,rtt,tx,rx}],"streams":n}.
func Stats() string {
	mu.Lock()
	c := client
	mu.Unlock()
	out := struct {
		Links   []bond.LinkStat `json:"links"`
		Streams int             `json:"streams"`
	}{Links: []bond.LinkStat{}}
	if c != nil {
		out.Links = c.Stats()
		out.Streams = c.NumStreams()
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// Stop shuts everything down.
func Stop() {
	mu.Lock()
	c, t := client, tun
	client, tun, cbs = nil, nil, nil
	mu.Unlock()
	if c != nil {
		c.Close()
	}
	if t != nil {
		t.close()
	}
}
