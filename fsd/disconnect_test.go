package fsd

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
)

// The disconnect reason is the only account of why a connection ended, and
// three very different situations used to share one sentence. Chasing a real
// drop (CES10061, 14:47) turned on being able to tell them apart:
//
//	idle timeout      the peer went quiet for readTimeout
//	closed by server  we took it down; the peer may have been fine
//	peer unreachable  the kernel gave up retransmitting
//
// The middle one is the trap: cancelling a client trips the read deadline on
// purpose (eventLoop's watcher), so a server-initiated close arrives at
// readErrorReason wearing the same error as a genuine idle timeout.

// deadlineErr is what a net.Conn returns when a deadline we set expires.
type deadlineErr struct{}

func (deadlineErr) Error() string   { return "i/o timeout" }
func (deadlineErr) Timeout() bool   { return true }
func (deadlineErr) Temporary() bool { return true }
func (deadlineErr) Unwrap() error   { return os.ErrDeadlineExceeded }

func TestReadErrorReasonIdleTimeout(t *testing.T) {
	got := readErrorReason(deadlineErr{}, false)
	if got != "idle timeout" {
		t.Fatalf("want idle timeout, got %q", got)
	}
}

func TestReadErrorReasonServerInitiatedCloseIsNotAnIdleTimeout(t *testing.T) {
	// Same error as above — only the cancelled flag differs. Reporting this
	// as "idle timeout" sends whoever reads the log after a client that had
	// done nothing wrong.
	got := readErrorReason(deadlineErr{}, true)
	if got != "closed by server" {
		t.Fatalf("want closed by server, got %q", got)
	}
}

func TestReadErrorReasonPeerUnreachable(t *testing.T) {
	// ETIMEDOUT is the kernel giving up on retransmits, and syscall.Errno
	// reports it as Timeout() — so testing netErr.Timeout() first would file
	// an unreachable peer under "idle timeout", which says the opposite: that
	// the peer was reachable and simply silent.
	err := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ETIMEDOUT}
	if !err.Timeout() {
		t.Skip("this platform does not report ETIMEDOUT as a timeout")
	}
	got := readErrorReason(err, false)
	if got != "peer unreachable" {
		t.Fatalf("want peer unreachable, got %q", got)
	}
}

func TestReadErrorReasonReset(t *testing.T) {
	// The one that started all this: an inbound RST. It must stay a plain
	// read error, because "the socket was reset from outside" is exactly what
	// distinguishes it from every server-initiated teardown above.
	err := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	got := readErrorReason(err, false)
	if got == "idle timeout" || got == "closed by server" || got == "peer unreachable" {
		t.Fatalf("a reset must not be reported as %q", got)
	}
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal("test error lost its cause")
	}
}

func TestReadErrorReasonOversizedPacket(t *testing.T) {
	if got := readErrorReason(bufio.ErrTooLong, false); got != "packet exceeded maximum size" {
		t.Fatalf("want packet exceeded maximum size, got %q", got)
	}
}

func TestWriteErrorReasonDeadlineNamesTheStalledClient(t *testing.T) {
	// A write deadline means the peer stopped reading, not that it vanished.
	got := writeErrorReason(deadlineErr{})
	if got != "write timeout: client not reading" {
		t.Fatalf("want write timeout, got %q", got)
	}
}

func TestWriteErrorReasonKeepsTheCause(t *testing.T) {
	got := writeErrorReason(&net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE})
	if got == "write timeout: client not reading" {
		t.Fatalf("a broken pipe is not a stalled reader: %q", got)
	}
	if len(got) == 0 || got[:12] != "write error:" {
		t.Fatalf("want the underlying error carried through, got %q", got)
	}
}

// A write failure must leave a reason behind. Before it did not: writerLoop
// returned, its deferred cancel tripped the reader's deadline, and the whole
// disconnect was logged as a read-side timeout — pointing at a client that had
// in fact stopped reading.
func TestWriteFailureIsRecordedAsTheDisconnectReason(t *testing.T) {
	client := &Client{}
	client.ctx, client.cancel = context.WithCancel(context.Background())

	client.Disconnect(writeErrorReason(deadlineErr{}))

	if got := client.DisconnectReason(); got != "write timeout: client not reading" {
		t.Fatalf("want the write reason, got %q", got)
	}
}

// Disconnect is first-wins, which is what lets writerLoop fill in a blank
// without ever overwriting a real reason (a kick, an idle sweep).
func TestTheFirstReasonWins(t *testing.T) {
	client := &Client{}
	client.ctx, client.cancel = context.WithCancel(context.Background())

	client.Disconnect("kicked by ZBAA_CTR")
	client.Disconnect(writeErrorReason(deadlineErr{}))

	if got := client.DisconnectReason(); got != "kicked by ZBAA_CTR" {
		t.Fatalf("the first reason must win, got %q", got)
	}
}
