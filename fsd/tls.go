package fsd

import (
	"crypto/tls"
	"fmt"
	"net"
)

// FSD-over-TLS uses a separate listener; the protocol has no STARTTLS upgrade.
// TLS-aware clients connect directly to that listener and must not silently
// fall back to plaintext. Setting plaintext_policy to "off" disables the
// server's plaintext listeners.
//
// The greeting identifies the FSD protocol version on both transports.
// Keeping its format unchanged preserves compatibility with older clients.

// Plaintext listener policies.
const (
	// PlaintextOn enables the configured plaintext listeners.
	PlaintextOn = "on"
	// PlaintextOff disables plaintext and requires an active TLS listener.
	PlaintextOff = "off"
)

// TLSConfig builds the server TLS configuration from a certificate pair.
//
// Connections require TLS 1.2 or TLS 1.3.
func TLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("fsd: a TLS listener needs both a certificate and a key")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("fsd: loading the TLS certificate: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
	}, nil
}

// WrapTLS turns a plain listener into a TLS one.
//
// Separate from TLSConfig so a caller — including a test — can bind an
// ephemeral port first and wrap it, which is what makes the no-downgrade
// behaviour testable without a fixed port.
func WrapTLS(listener net.Listener, config *tls.Config) net.Listener {
	return tls.NewListener(listener, config)
}
