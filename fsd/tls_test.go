package fsd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTestCert generates a throwaway self-signed pair.
//
// Generated rather than committed: a certificate and key in the repository is
// a credential in the repository, however clearly it is labelled a test one,
// and the first person to copy this directory as a starting point inherits it.
func writeTestCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fsd.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"fsd.test"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating a certificate: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("writing the certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling the key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("writing the key: %v", err)
	}
	return certFile, keyFile
}

// The transport policy permits TLS 1.2 and TLS 1.3.
func TestTLSConfigMatchesTheClientContract(t *testing.T) {
	certFile, keyFile := writeTestCert(t)

	cfg, err := TLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
	}
	if cfg.MaxVersion != tls.VersionTLS13 {
		t.Errorf("MaxVersion = %x, want TLS 1.3", cfg.MaxVersion)
	}
}

// A TLS listener with no certificate is not a TLS listener, and the refusal
// has to name what is missing.
func TestTLSConfigRefusesAnIncompletePair(t *testing.T) {
	certFile, keyFile := writeTestCert(t)
	for _, c := range []struct{ cert, key string }{
		{"", ""}, {certFile, ""}, {"", keyFile},
		{certFile, filepath.Join(t.TempDir(), "missing.pem")},
	} {
		if _, err := TLSConfig(c.cert, c.key); err == nil {
			t.Errorf("cert=%q key=%q should have been refused", c.cert, c.key)
		}
	}
}

// A wrapped listener really does speak TLS: a plaintext dial must not get a
// usable FSD session out of it. This is the server half of "no downgrade".
func TestWrappedListenerRefusesPlaintext(t *testing.T) {
	certFile, keyFile := writeTestCert(t)
	cfg, err := TLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener := WrapTLS(raw, cfg)
	defer listener.Close()

	accepted := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer conn.Close()
		// The handshake is deferred until the first read, which is where a
		// plaintext peer fails.
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		accepted <- err
	}()

	plain, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer plain.Close()
	// A bare FSD login packet, as a plaintext client would send.
	plain.Write([]byte("#AAZBAA_TWR:SERVER:Name:1001:secret:5:9\r\n"))

	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("a plaintext client completed a read against the TLS listener")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the TLS listener neither failed nor completed the handshake")
	}
}

// The other half: a real TLS client does complete a handshake, so the refusal
// above is about the transport and not about a listener that is simply broken.
func TestWrappedListenerAcceptsTLS(t *testing.T) {
	certFile, keyFile := writeTestCert(t)
	cfg, err := TLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener := WrapTLS(raw, cfg)
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Read once to drive the handshake, then let the client go.
			go func(c net.Conn) {
				defer c.Close()
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 1)
				c.Read(buf)
			}(conn)
		}
	}()

	// This test uses a generated self-signed certificate. Deployed clients
	// must verify the server certificate and hostname.
	secure, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 3 * time.Second}, "tcp", listener.Addr().String(),
		&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("a TLS client should complete the handshake: %v", err)
	}
	defer secure.Close()

	if state := secure.ConnectionState(); state.Version < tls.VersionTLS12 {
		t.Errorf("negotiated TLS %x, want at least 1.2", state.Version)
	}
}
