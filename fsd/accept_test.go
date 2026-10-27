package fsd

import (
	"context"
	"errors"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type temporaryAcceptError struct{}

func (temporaryAcceptError) Error() string   { return "temporary accept failure" }
func (temporaryAcceptError) Timeout() bool   { return false }
func (temporaryAcceptError) Temporary() bool { return true }

type observedTerminalAcceptError struct{ classified chan struct{} }

func (observedTerminalAcceptError) Error() string { return "terminal accept failure" }
func (observedTerminalAcceptError) Timeout() bool { return false }
func (e observedTerminalAcceptError) Temporary() bool {
	close(e.classified)
	return false
}

type observedDoneContext struct {
	context.Context
	calls chan struct{}
}

func (c observedDoneContext) Done() <-chan struct{} {
	c.calls <- struct{}{}
	return c.Context.Done()
}

type scriptedListener struct {
	mu        sync.Mutex
	errors    []error
	times     []time.Time
	attempts  atomic.Int32
	attempted chan struct{}
	accepted  chan struct{}
	closed    chan struct{}
	once      sync.Once
}

func newScriptedListener(errors ...error) *scriptedListener {
	return &scriptedListener{
		errors:    errors,
		attempted: make(chan struct{}, 32),
		accepted:  make(chan struct{}),
		closed:    make(chan struct{}),
	}
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.attempts.Add(1)
	select {
	case l.attempted <- struct{}{}:
	default:
	}
	l.mu.Lock()
	l.times = append(l.times, time.Now())
	l.mu.Unlock()
	l.mu.Lock()
	if len(l.errors) > 0 {
		err := l.errors[0]
		l.errors = l.errors[1:]
		l.mu.Unlock()
		if err != nil {
			return nil, err
		}
		server, peer := net.Pipe()
		closeConnSoon(peer)
		select {
		case <-l.accepted:
		default:
			close(l.accepted)
		}
		return server, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}

func (l *scriptedListener) attemptTimes() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Time(nil), l.times...)
}

func closeConnSoon(conn net.Conn) {
	go func() { _ = conn.Close() }()
}

func (l *scriptedListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *scriptedListener) Addr() net.Addr { return scriptedAddr("test") }

type scriptedAddr string

func (a scriptedAddr) Network() string { return "scripted" }
func (a scriptedAddr) String() string  { return string(a) }

func testServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Defaults()
	s, err := NewServer(Options{
		Config: config.NewStore("config.json", &cfg),
		Users:  &fakeAuth{users: map[string]*testIdentity{}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s
}

func TestServeRetriesTemporaryAcceptErrors(t *testing.T) {
	listener := newScriptedListener(temporaryAcceptError{}, temporaryAcceptError{}, nil)
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, listener, errCh)
		close(done)
	}()

	select {
	case <-listener.accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not recover from temporary accept errors")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after context cancellation")
	}
	select {
	case err := <-errCh:
		t.Fatalf("temporary accept error reached errCh: %v", err)
	default:
	}
	if got := listener.attempts.Load(); got != 4 {
		t.Fatalf("accept attempts = %d, want 4 including shutdown", got)
	}
}

func TestServeReportsTerminalAcceptError(t *testing.T) {
	want := errors.New("listener permanently failed")
	listener := newScriptedListener(want)
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, listener, errCh)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after terminal accept error")
	}
	select {
	case got := <-errCh:
		if !errors.Is(got, want) {
			t.Fatalf("errCh = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not report terminal accept error")
	}
}

func TestServeResetsAcceptBackoffAfterSuccess(t *testing.T) {
	listener := newScriptedListener(
		temporaryAcceptError{}, temporaryAcceptError{}, temporaryAcceptError{},
		temporaryAcceptError{}, temporaryAcceptError{}, nil,
		temporaryAcceptError{}, nil,
	)
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, listener, nil)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for listener.attempts.Load() < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if listener.attempts.Load() < 8 {
		cancel()
		<-done
		t.Fatal("Serve did not complete the scripted accept sequence")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after context cancellation")
	}

	times := listener.attemptTimes()
	if len(times) < 8 {
		t.Fatalf("recorded %d accept attempts, want at least 8", len(times))
	}
	if delay := times[1].Sub(times[0]); delay < 4*time.Millisecond {
		t.Fatalf("first retry delay = %s, want at least 4ms", delay)
	}
	if delay := times[2].Sub(times[1]); delay < 8*time.Millisecond {
		t.Fatalf("second retry delay = %s, want at least 8ms", delay)
	}
	if delay := times[5].Sub(times[4]); delay < 64*time.Millisecond {
		t.Fatalf("retry delay before successful accept = %s, want at least 64ms", delay)
	}
	if delay := times[7].Sub(times[6]); delay >= 40*time.Millisecond {
		t.Fatalf("retry delay after successful accept = %s, want reset below 40ms", delay)
	}
}

func TestServeRecoversFromResourceAcceptErrors(t *testing.T) {
	for _, resourceErr := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM} {
		t.Run(resourceErr.Error(), func(t *testing.T) {
			acceptErr := &net.OpError{Op: "accept", Net: "tcp", Err: &os.SyscallError{Syscall: "accept4", Err: resourceErr}}
			listener := newScriptedListener(acceptErr, nil)
			server := testServer(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errCh := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				server.Serve(ctx, listener, errCh)
				close(done)
			}()
			select {
			case <-listener.accepted:
			case <-time.After(time.Second):
				t.Fatal("Serve did not recover from resource exhaustion")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Serve did not stop after cancellation")
			}
			select {
			case err := <-errCh:
				t.Fatalf("resource error reached errCh: %v", err)
			default:
			}
			if !server.WaitConnections(time.Second) {
				t.Fatal("accepted connection did not drain")
			}
		})
	}
}

func TestServeCapsAcceptBackoffAndCancelsDuringRetry(t *testing.T) {
	errs := make([]error, 12)
	for i := range errs {
		errs[i] = &net.OpError{Op: "accept", Err: syscall.ENOBUFS}
	}
	listener := newScriptedListener(errs...)
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observedCtx := observedDoneContext{Context: ctx, calls: make(chan struct{}, 32)}
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		server.Serve(observedCtx, listener, errCh)
		close(done)
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	// One Done call belongs to the listener watcher; the other eleven are
	// made after each retry timer starts. Cancel while the capped timer runs.
	for range 12 {
		select {
		case <-observedCtx.calls:
		case <-deadline.C:
			t.Fatal("Serve did not reach capped retries")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Serve waited for the one-second backoff after cancellation")
	}
	times := listener.attemptTimes()
	if len(times) != 11 {
		t.Fatalf("accept attempts = %d, want 11 before cancellation", len(times))
	}
	for _, index := range []int{9, 10} {
		if delay := times[index].Sub(times[index-1]); delay < 900*time.Millisecond || delay > 1500*time.Millisecond {
			t.Fatalf("retry delay = %s, want capped one-second backoff", delay)
		}
	}
	select {
	case err := <-errCh:
		t.Fatalf("cancellation reported a terminal error: %v", err)
	default:
	}
}

func TestServeReportsUnexpectedListenerClosure(t *testing.T) {
	listener := newScriptedListener(&net.OpError{Op: "accept", Err: net.ErrClosed})
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	server.Serve(ctx, listener, errCh)
	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("terminal error = %v, want net.ErrClosed", err)
		}
	default:
		t.Fatal("unexpected listener closure was silently ignored")
	}
}

func TestServeTerminalErrorSendCancels(t *testing.T) {
	classified := make(chan struct{})
	listener := newScriptedListener(observedTerminalAcceptError{classified: classified})
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, listener, make(chan error))
		close(done)
	}()
	select {
	case <-classified:
	case <-time.After(time.Second):
		t.Fatal("Serve did not classify the terminal accept error")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal error delivery blocked shutdown")
	}
}

func TestRunPropagatesListenerFailureAndStopsOtherListeners(t *testing.T) {
	for _, acceptErr := range []error{errors.New("listener permanently failed"), net.ErrClosed} {
		t.Run(acceptErr.Error(), func(t *testing.T) {
			server := testServer(t)
			server.config.Get().ListenAddrs = []string{"first", "second"}
			listeners := []*scriptedListener{newScriptedListener(acceptErr), newScriptedListener()}
			index := 0
			listen := func(ctx context.Context, network, address string) (net.Listener, error) {
				if network != "tcp" || address != server.config.Get().ListenAddrs[index] {
					t.Errorf("unexpected listen arguments: %s %s", network, address)
				}
				listener := listeners[index]
				index++
				return listener, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- server.run(ctx, listen) }()
			select {
			case err := <-result:
				if !errors.Is(err, acceptErr) {
					t.Fatalf("Run error = %v, want %v", err, acceptErr)
				}
			case <-time.After(time.Second):
				t.Fatal("Run stayed active after listener failure")
			}
			for _, listener := range listeners {
				select {
				case <-listener.closed:
				default:
					t.Fatal("listener remained open after terminal failure")
				}
			}
			if ctx.Err() != nil {
				t.Fatal("test parent context was unexpectedly cancelled")
			}
		})
	}
}
