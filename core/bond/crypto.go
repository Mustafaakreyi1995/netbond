package bond

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"
)

// Both ends hold one shared secret. From it they derive:
//   - an Ed25519 key the server uses for its TLS certificate; the client
//     accepts only a certificate carrying that exact public key, so TLS 1.3
//     authenticates the server without any certificate authority;
//   - a token the client sends inside the TLS tunnel to authenticate itself.

// NewSecret returns a fresh random secret in text form.
func NewSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func derive(secret, label string, n int) []byte {
	out, err := hkdf.Key(sha256.New, []byte(secret), []byte("netbond v1"), label, n)
	if err != nil {
		panic(err)
	}
	return out
}

func tlsKey(secret string) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(derive(secret, "tls key", ed25519.SeedSize))
}

func authToken(secret string) []byte { return derive(secret, "auth token", 32) }

func serverTLS(secret string) (*tls.Config, error) {
	key := tlsKey(secret)
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "netbond"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(20 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}, nil
}

func clientTLS(secret string) *tls.Config {
	want := tlsKey(secret).Public().(ed25519.PublicKey)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// The usual CA check is replaced by the exact public-key check below.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errors.New("bond: no server certificate")
			}
			cert, err := x509.ParseCertificate(raw[0])
			if err != nil {
				return err
			}
			got, ok := cert.PublicKey.(ed25519.PublicKey)
			if !ok || !bytes.Equal(got, want) {
				return errors.New("bond: server key mismatch (wrong secret?)")
			}
			return nil
		},
	}
}

// Code builds the connection code the app accepts.
func Code(secret, host string, port int) string {
	return fmt.Sprintf("netbond://%s@%s", secret, net.JoinHostPort(host, fmt.Sprint(port)))
}

// ParseCode splits a connection code into secret, host and port.
func ParseCode(code string) (secret, host, port string, err error) {
	code = strings.TrimSpace(code)
	u, err := url.Parse(code)
	if err != nil || u.Scheme != "netbond" || u.User == nil || u.Hostname() == "" {
		return "", "", "", errors.New("invalid connection code")
	}
	port = u.Port()
	if port == "" {
		port = "443"
	}
	return u.User.Username(), u.Hostname(), port, nil
}
