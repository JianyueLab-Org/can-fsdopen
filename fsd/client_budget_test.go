package fsd

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestAcceptInboundEnforcesPacketBudget(t *testing.T) {
	client := newClient(context.Background(), nopConn{}, nil, LoginData{Callsign: "CCA101"})
	client.inboundPackets = newTokenBucket(1, time.Hour)
	client.inboundBytes = newTokenBucket(1024, time.Hour)

	if !client.acceptInbound([]byte("one")) {
		t.Fatal("first packet rejected")
	}
	if client.acceptInbound([]byte("two")) {
		t.Fatal("second packet accepted after packet budget was exhausted")
	}
	if client.DisconnectReason() != "inbound rate limit exceeded" {
		t.Fatalf("disconnect reason = %q", client.DisconnectReason())
	}
}

func TestAcceptInboundEnforcesByteBudget(t *testing.T) {
	client := newClient(context.Background(), nopConn{}, nil, LoginData{Callsign: "CCA101"})
	client.inboundPackets = newTokenBucket(10, time.Hour)
	client.inboundBytes = newTokenBucket(3, time.Hour)

	if !client.acceptInbound([]byte("one")) {
		t.Fatal("packet within byte budget rejected")
	}
	if client.acceptInbound([]byte("two")) {
		t.Fatal("packet over byte budget accepted")
	}
}

type nopConn struct{}

func (nopConn) Read([]byte) (int, error)         { return 0, context.Canceled }
func (nopConn) Write(p []byte) (int, error)      { return len(p), nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return nopAddr{} }
func (nopConn) RemoteAddr() net.Addr             { return nopAddr{} }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

type nopAddr struct{}

func (nopAddr) Network() string { return "test" }
func (nopAddr) String() string  { return "test" }
