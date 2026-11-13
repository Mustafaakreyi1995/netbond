// netbond-server is the internet-side half of NetBond. Run it on a VPS; the
// phone app connects to it over every network it has.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"netbond.app/core/bond"
)

func main() {
	listen := flag.String("listen", ":443", "address to listen on")
	secretFile := flag.String("secret-file", "/etc/netbond/secret", "file holding the shared secret (created if missing)")
	host := flag.String("host", "", "public IP or hostname, used only to print the connection code")
	printCode := flag.Bool("print-code", false, "print the connection code and exit")
	quiet := flag.Bool("quiet", false, "do not log link events")
	flag.Parse()

	secret, err := loadSecret(*secretFile)
	if err != nil {
		log.Fatalf("secret: %v", err)
	}
	_, port, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatalf("bad -listen: %v", err)
	}
	h := *host
	if h == "" {
		h = "YOUR_SERVER_IP"
	}
	code := fmt.Sprintf("netbond://%s@%s", secret, net.JoinHostPort(h, port))
	if *printCode {
		fmt.Println(code)
		return
	}

	logf := log.Printf
	if *quiet {
		logf = nil
	}
	sv, err := bond.NewServer(secret, logf)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("netbond-server listening on %s", ln.Addr())
	log.Fatal(sv.Serve(ln))
}

func loadSecret(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if s := strings.TrimSpace(string(b)); len(s) >= 20 {
			return s, nil
		}
	}
	s := bond.NewSecret()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(s+"\n"), 0o600); err != nil {
		return "", err
	}
	return s, nil
}
