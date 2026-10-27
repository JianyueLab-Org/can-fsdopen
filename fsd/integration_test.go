package fsd

import (
	"bufio"
	"context"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"net"
	"strings"
	"testing"
	"time"
)

// Integration tests drive a real listener over a real TCP connection, so the
// framing, handshake, authorisation and delivery paths are all exercised
// together.

type testIdentity struct {
	Username string
	Password string
	Rating   int
}
type fakeAuth struct{ users map[string]*testIdentity }

func (f *fakeAuth) Authenticate(_ context.Context, id, password string, requested int, _ string) (*auth.Identity, error) {
	user, ok := f.users[id]
	if !ok {
		return nil, auth.ErrUnknownIdentity
	}
	if password != user.Password {
		return nil, auth.ErrInvalidCredential
	}
	if user.Rating < int(RatingObserver) {
		return nil, auth.ErrInactive
	}
	if requested > user.Rating {
		return nil, auth.ErrRatingDenied
	}
	return &auth.Identity{ID: id, Rating: user.Rating}, nil
}

type testHarness struct {
	server *Server
	addr   string
	cancel context.CancelFunc
}
type harnessOptions struct {
	enforcement      config.FacilityEnforcement
	users            map[string]*testIdentity
	liveMode         bool
	greetImmediately bool
	welcome          string
}

func newHarness(t *testing.T, opts harnessOptions) *testHarness {
	t.Helper()
	if opts.users == nil {
		opts.users = map[string]*testIdentity{
			"1000": {Username: "1000", Password: "secret", Rating: int(RatingObserver)},
			"1001": {Username: "1001", Password: "secret", Rating: 5},
			"1011": {Username: "1011", Password: "secret", Rating: int(RatingSupervisor)},
			"1099": {Username: "1099", Password: "secret", Rating: 0},
		}
	}
	if opts.enforcement == "" {
		opts.enforcement = config.EnforcementWarn
	}
	cfg := config.Defaults()
	cfg.Welcome = "Welcome to the test network"
	if opts.welcome != "" {
		cfg.Welcome = opts.welcome
	}
	cfg.FacilityEnforcement = opts.enforcement
	cfg.FSDMode = 1
	if opts.liveMode {
		cfg.FSDMode = 0
	}
	cfg.IdentDelayMS = 60
	if opts.greetImmediately {
		cfg.IdentDelayMS = 0
	}
	cfg.ClientAuthKeys = map[uint16]string{0x88e4: "0123456789abcdef0123456789abcdef", 0x69d7: "abcdef0123456789abcdef0123456789"}
	server, err := NewServer(Options{Config: config.NewStore("unused", &cfg), Users: &fakeAuth{users: opts.users}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go server.Serve(ctx, listener, nil)
	t.Cleanup(func() { cancel(); listener.Close(); server.WaitConnections(3 * time.Second) })
	return &testHarness{server: server, addr: listener.Addr().String(), cancel: cancel}
}

// testClient is a connected FSD client for assertions.
//
// Received packets are pumped into a channel by a background goroutine rather
// than read on demand. A bufio.Scanner latches its error permanently, so
// reading with a deadline and letting it expire — which is what draining
// queued traffic amounts to — would make every later read fail.
type testClient struct {
	t        *testing.T
	conn     net.Conn
	packets  chan string
	callsign string
	ident    string // the $DI greeting this connection received
}

// dial connects and consumes the $DI greeting.
func (h *testHarness) dial(t *testing.T) *testClient {
	t.Helper()

	c := h.dialRaw(t)

	ident := c.expect("$DI")
	if !strings.HasPrefix(ident, "$DISERVER:CLIENT:") {
		t.Fatalf("unexpected server ident: %q", ident)
	}
	c.ident = ident

	return c
}

// dialRaw connects without waiting for a greeting, the way a client that
// speaks first does.
func (h *testHarness) dialRaw(t *testing.T) *testClient {
	t.Helper()

	conn, err := net.DialTimeout("tcp", h.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	c := &testClient{t: t, conn: conn, packets: make(chan string, 256)}

	go func() {
		defer close(c.packets)

		scanner := NewScanner(bufio.NewReader(conn))
		for scanner.Scan() {
			select {
			case c.packets <- scanner.Text():
			default:
				// Test is not keeping up; dropping is better than blocking
				// the reader and stalling the server's writer.
			}
		}
	}()

	return c
}

func (c *testClient) send(packet string) {
	c.t.Helper()
	c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.conn.Write([]byte(packet + "\r\n")); err != nil {
		c.t.Fatalf("write %q: %v", packet, err)
	}
}

// sendRaw writes bytes verbatim, without appending a delimiter.
func (c *testClient) sendRaw(data []byte) {
	c.t.Helper()
	c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.conn.Write(data); err != nil {
		c.t.Fatalf("write raw: %v", err)
	}
}

// expect waits for a packet with the given prefix.
func (c *testClient) expect(prefix string) string {
	c.t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case packet, ok := <-c.packets:
			if !ok {
				c.t.Fatalf("connection closed while waiting for a %s packet", prefix)
			}
			if strings.HasPrefix(packet, prefix) {
				return packet
			}
		case <-deadline:
			c.t.Fatalf("timed out waiting for a %s packet", prefix)
		}
	}
}

// expectNone asserts no packet with the given prefix arrives within the window.
func (c *testClient) expectNone(prefix string, within time.Duration) {
	c.t.Helper()

	deadline := time.After(within)
	for {
		select {
		case packet, ok := <-c.packets:
			if !ok {
				return
			}
			if strings.HasPrefix(packet, prefix) {
				c.t.Fatalf("received an unexpected %s packet: %q", prefix, packet)
			}
		case <-deadline:
			return
		}
	}
}

// count returns how many packets matching a predicate arrive in the window.
func (c *testClient) count(within time.Duration, match func(string) bool) int {
	n := 0
	deadline := time.After(within)

	for {
		select {
		case packet, ok := <-c.packets:
			if !ok {
				return n
			}
			if match(packet) {
				n++
			}
		case <-deadline:
			return n
		}
	}
}

// loginPilot performs the pilot handshake.
func (h *testHarness) loginPilot(t *testing.T, callsign, cid string, proto int) *testClient {
	t.Helper()

	c := h.dial(t)
	c.callsign = callsign
	c.send(fmt.Sprintf("$ID%s:SERVER:88e4:vPilot:3:8:%s:-582057156:6d6973746176", callsign, cid))
	c.send(fmt.Sprintf("#AP%s:SERVER:%s:secret:1:%d:10:Test Pilot", callsign, cid, proto))
	c.expect("#TMSERVER")

	return c
}

// loginATC performs the controller handshake.
func (h *testHarness) loginATC(t *testing.T, callsign, cid string, rating int) *testClient {
	t.Helper()

	c := h.dial(t)
	c.callsign = callsign
	// 69d7 is EuroScope's VATSIM-assigned client ID (27095).
	c.send(fmt.Sprintf("$ID%s:SERVER:69d7:EuroScope:3:2:%s:-582057156:63616665", callsign, cid))
	c.send(fmt.Sprintf("#AA%s:SERVER:Test Controller:%s:secret:%d:100", callsign, cid, rating))
	c.expect("#TMSERVER")

	return c
}

// waitForOnline blocks until the server reports the expected client count.
func (h *testHarness) waitForOnline(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.server.OnlineCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected %d clients online, got %d", want, h.server.OnlineCount())
}

// --- tests ---

func TestLoginAndMOTD(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dial(t)
	c.send("$IDCCA101:SERVER:88e4:vPilot:3:8:1000:-582057156:6d6973746176")
	c.send("#APCCA101:SERVER:1000:secret:1:101:10:Test Pilot")

	motd := c.expect("#TMSERVER")
	if !strings.Contains(motd, "Welcome to the test network") {
		t.Errorf("MOTD = %q, want the configured welcome message", motd)
	}

	h.waitForOnline(t, 1)
}

func TestWelcomeLinesRemainServerText(t *testing.T) {
	h := newHarness(t, harnessOptions{
		welcome: "Visit https://example.test/network\r\nBriefing at 12:00\r\n#AAINJECTED:SERVER:untrusted",
	})
	c := h.dial(t)
	c.send("#APCCA101:SERVER:1000:secret:1:101:10:Test Pilot")

	for _, want := range []string{
		"#TMSERVER:CCA101:Visit https://example.test/network",
		"#TMSERVER:CCA101:Briefing at 12:00",
		"#TMSERVER:CCA101:#AAINJECTED:SERVER:untrusted",
	} {
		select {
		case got, ok := <-c.packets:
			if !ok || got != want {
				t.Fatalf("welcome packet = %q (open=%t), want %q", got, ok, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for welcome packet %q", want)
		}
	}

	c.send("$PICCA101:SERVER:welcome-complete")
	c.expect("$POSERVER:CCA101:welcome-complete")
}

// TestLoginWithoutIdentPacket: $ID is advisory, so a client may open with
// #AP/#AA. The handshake used to read two packets unconditionally, so such a
// client sat blocked until loginTimeout closed the socket on it.
func TestLoginWithoutIdentPacket(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dial(t)
	c.send("#APCCA102:SERVER:1000:secret:1:100:10:Test Pilot")

	c.expect("#TMSERVER")
	h.waitForOnline(t, 1)
}

// TestATCLoginWithoutIdentPacket is the controller half: EuroScope connecting
// to a private (non-VATSIM) server has no server challenge to answer.
func TestATCLoginWithoutIdentPacket(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dial(t)
	c.send("#AAZSSS_TWR:SERVER:Test Controller:1001:secret:2:100")

	c.expect("#TMSERVER")
	h.waitForOnline(t, 1)
}

// --- the deferred greeting ---

// TestClientOpeningWithoutIdentIsNotGreeted covers EuroScope against a
// non-VATSIM server: it opens with #AA and no $ID at all, and aborts with
// "VATSIM server found while non-VATSIM server was requested" if a $DI ever
// arrives.
func TestClientOpeningWithoutIdentIsNotGreeted(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dialRaw(t)
	c.send("#AAZSSS_TWR:SERVER:Test Controller:1001:secret:2:100")

	c.expect("#TMSERVER")
	c.expectNone("$DI", 300*time.Millisecond)
	h.waitForOnline(t, 1)
}

// TestEuroScopePrivateLogin replays what EuroScope 3.2.13 actually sends to a
// server it was told is not VATSIM, captured off the wire:
//
//	#AARJFU_I_APP:SERVER:1000:1000:<password>:5:9:1:0:32.91694:129.91361:100
//
// No $ID, protocol revision 9 rather than 100, and four trailing fields the
// VATSIM layout does not have. Revision 9 alone used to fail the login with
// "Unsupported protocol revision".
func TestEuroScopePrivateLogin(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dialRaw(t)
	c.send("#AARJFU_I_APP:SERVER:1001:1001:secret:5:9:1:0:32.91694:129.91361:100")

	c.expect("#TMSERVER")
	c.expectNone("$DI", 300*time.Millisecond)
	h.waitForOnline(t, 1)

	// The position update that follows immediately must be accepted too.
	c.send("%RJFU_I_APP:99998:5:150:5:32.91694:129.91361:0")
	c.expectNone("$ER", 300*time.Millisecond)
}

// TestClientOpeningWithIdentIsStillGreeted covers EuroScope with "Connect to
// VATSIM" ticked. It wants the greeting — the $DI carries the challenge key —
// but it may give up waiting and send its $ID first. Opening with $ID is the
// signal that the greeting is still wanted; the non-VATSIM EuroScope sends no
// $ID at all.
func TestClientOpeningWithIdentIsStillGreeted(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dialRaw(t)
	c.send("$IDZSSS_TWR:SERVER:69d7:EuroScope 3.2:3:2:1001:-582057156")

	if ident := c.expect("$DI"); !strings.HasPrefix(ident, "$DISERVER:CLIENT:") {
		t.Fatalf("unexpected server ident: %q", ident)
	}

	c.send("#AAZSSS_TWR:SERVER:Test Controller:1001:secret:2:100")
	c.expect("#TMSERVER")
	h.waitForOnline(t, 1)
}

// TestSilentClientIsGreeted is the other half: vPilot and the rest say nothing
// until the greeting arrives, so it still has to be sent.
func TestSilentClientIsGreeted(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.dialRaw(t)
	if ident := c.expect("$DI"); !strings.HasPrefix(ident, "$DISERVER:CLIENT:") {
		t.Fatalf("unexpected server ident: %q", ident)
	}
}

// TestGreetImmediatelyIgnoresTheClient keeps the escape hatch working: a
// network with no EuroScope on it can greet unconditionally as before.
func TestGreetImmediatelyIgnoresTheClient(t *testing.T) {
	h := newHarness(t, harnessOptions{greetImmediately: true})

	c := h.dialRaw(t)
	c.send("$IDZSSS_TWR:SERVER:69d7:EuroScope 3.2:3:2:1001:-582057156")

	c.expect("$DI")
}

func TestLoginRejectionsSendErrorPackets(t *testing.T) {
	// The Python server closed the socket with only a text message, or in
	// several paths swallowed the failure entirely, so a client could not
	// tell a bad password from a network fault.
	tests := []struct {
		name     string
		add      string
		wantCode string
	}{
		{
			name:     "wrong password",
			add:      "#APCCA101:SERVER:1000:wrong:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeInvalidLogon),
		},
		{
			name:     "unknown CID",
			add:      "#APCCA101:SERVER:9999:secret:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeInvalidLogon),
		},
		{
			name:     "suspended certificate",
			add:      "#APCCA101:SERVER:1099:secret:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeCertificateSuspended),
		},
		{
			name:     "rating above entitlement",
			add:      "#APCCA101:SERVER:1000:secret:5:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeRequestedLevelTooHigh),
		},
		{
			name:     "unsupported protocol revision",
			add:      "#APCCA101:SERVER:1000:secret:1:99:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeInvalidRevision),
		},
		{
			name:     "reserved callsign",
			add:      "#APSERVER:SERVER:1000:secret:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeCallsignInvalid),
		},
		{
			name:     "lowercase callsign",
			add:      "#APcca101:SERVER:1000:secret:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeCallsignInvalid),
		},
		{
			// 13 characters, one over MaxCallsignLength. This fixture used to
			// be 11 characters, which is now legal: vATIS needs 11 for the
			// split-ATIS names ZSPD_D_ATIS and ZSPD_A_ATIS.
			name:     "callsign too long",
			add:      "#APABCDEFGHIJKLM:SERVER:1000:secret:1:101:10:Test Pilot",
			wantCode: fmt.Sprint(ErrCodeCallsignInvalid),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})

			c := h.dial(t)
			c.send("$IDCCA101:SERVER:88e4:vPilot:3:8:1000:-582057156:6d6973746176")
			c.send(tt.add)

			errPacket := c.expect("$ER")
			if !strings.Contains(errPacket, ":"+tt.wantCode+":") {
				t.Errorf("$ER = %q, want error code %s", errPacket, tt.wantCode)
			}
		})
	}
}

// TestSplitATISCallsignLogsIn covers the reason MaxCallsignLength went from 10
// to 12. vATIS names a split ATIS ZSPD_D_ATIS / ZSPD_A_ATIS — 11 characters —
// so an airport running separate departure and arrival broadcasts could not
// get either station onto the network.
func TestSplitATISCallsignLogsIn(t *testing.T) {
	for _, callsign := range []string{"ZSPD_D_ATIS", "ZSPD_A_ATIS"} {
		t.Run(callsign, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})

			c := h.loginATC(t, callsign, "1001", 5)
			h.waitForOnline(t, 1)

			c.expectNone("$ER", 300*time.Millisecond)
			if h.server.OnlineCount() != 1 {
				t.Errorf("%s did not stay online", callsign)
			}
		})
	}
}

func TestDuplicateCallsignRejected(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 1)

	second := h.dial(t)
	second.send("$IDCCA101:SERVER:88e4:vPilot:3:8:1001:-582057156:6d6973746176")
	second.send("#APCCA101:SERVER:1001:secret:1:101:10:Impostor")

	errPacket := second.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeCallsignInUse)) {
		t.Errorf("$ER = %q, want CallsignInUse (%d)", errPacket, ErrCodeCallsignInUse)
	}
}

func TestAddPacketBroadcastOmitsPassword(t *testing.T) {
	// The add packet is re-broadcast to announce a new station. Forwarding it
	// verbatim would hand every connected client the newcomer's password.
	h := newHarness(t, harnessOptions{})

	watcher := h.loginPilot(t, "CCA100", "1000", 101)
	h.waitForOnline(t, 1)

	h.loginPilot(t, "CCA101", "1001", 101)

	announcement := watcher.expect("#AP")
	if strings.Contains(announcement, "secret") {
		t.Fatalf("add broadcast leaked the password: %q", announcement)
	}
	if !strings.Contains(announcement, "CCA101") {
		t.Errorf("add broadcast = %q, want it to name CCA101", announcement)
	}
}

func TestPositionBroadcastIsRangeLimited(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	// Shanghai.
	near := h.loginPilot(t, "CCA101", "1000", 101)
	// Los Angeles, far outside any pilot's 40 NM bubble.
	far := h.loginPilot(t, "UAL201", "1001", 101)
	h.waitForOnline(t, 2)

	// Put both receivers at known positions first.
	near.send("@N:CCA101:2000:1:31.19670:121.33500:100:0:0:0")
	far.send("@N:UAL201:2000:1:33.94000:-118.40000:100:0:0:0")

	drain(near)
	drain(far)

	// A third aircraft next to CCA101 must be seen by it and not by UAL201.
	mover := h.loginPilot(t, "CES500", "1011", 101)
	h.waitForOnline(t, 3)
	drain(near)
	drain(far)

	mover.send("@N:CES500:2000:1:31.20000:121.34000:3000:250:0:0")

	if got := near.expect("@"); !strings.Contains(got, "CES500") {
		t.Errorf("in-range client received %q, want the CES500 position", got)
	}

	far.expectNone("@N:CES500", 800*time.Millisecond)
}

func TestKillRequestRequiresSupervisor(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	victim := h.loginPilot(t, "CCA101", "1000", 101)
	observer := h.loginATC(t, "ZSSS_OBS", "1000", 1)
	h.waitForOnline(t, 2)

	// The Python server ran no check at all here: any client could disconnect
	// any other.
	observer.send("$!!ZSSS_OBS:CCA101:1:testing")

	errPacket := observer.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidControl)) {
		t.Errorf("$ER = %q, want InvalidControl (%d)", errPacket, ErrCodeInvalidControl)
	}

	victim.expectNone("#TMSERVER:CCA101:You have been disconnected", 500*time.Millisecond)
	if h.server.OnlineCount() != 2 {
		t.Errorf("victim was disconnected by an unprivileged client")
	}
}

func TestKillRequestBySupervisorDisconnects(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	h.loginPilot(t, "CCA101", "1000", 101)
	supervisor := h.loginATC(t, "ZSSS_SUP", "1011", 11)
	h.waitForOnline(t, 2)

	supervisor.send("$!!ZSSS_SUP:CCA101:1:testing")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.server.OnlineCount() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Errorf("supervisor kick did not disconnect the target: %d still online",
		h.server.OnlineCount())
}

func TestBroadcastRequiresSupervisor(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	sender := h.loginPilot(t, "CCA101", "1000", 101)
	receiver := h.loginPilot(t, "UAL201", "1001", 101)
	h.waitForOnline(t, 2)
	drain(receiver)

	sender.send("#TMCCA101:*:everyone should see this")

	errPacket := sender.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidControl)) {
		t.Errorf("$ER = %q, want InvalidControl", errPacket)
	}

	receiver.expectNone("#TMCCA101:*", 500*time.Millisecond)
}

func TestSourceCallsignSpoofingRejected(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	attacker := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 1)

	// Claiming to be another station must be refused, or any client could
	// forge position reports and messages attributed to anyone.
	attacker.send("#TMUAL201:@22800:not actually from UAL201")

	errPacket := attacker.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeSourceInvalid)) {
		t.Errorf("$ER = %q, want SourceInvalid (%d)", errPacket, ErrCodeSourceInvalid)
	}
}

// TestLargeFlightPlanSurvivesFragmentation is the end-to-end version of the
// framing regression: a route far larger than the Python server's 1024-byte
// read must arrive intact even when written in small pieces.
func TestLargeFlightPlanSurvivesFragmentation(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	controller := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	h.waitForOnline(t, 2)
	drain(controller)

	route := strings.Repeat("WAYPT ", 800)
	packet := "$FPCCA101:SERVER:I:A320:420:ZSPD:1200:1200:FL350:ZBAA:0:0:0:0::/V/:" + route + "\r\n"

	if len(packet) < 4096 {
		t.Fatalf("test packet is only %d bytes; it must exceed a single read", len(packet))
	}

	// Write in 100-byte chunks so the packet spans many TCP segments.
	data := []byte(packet)
	for i := 0; i < len(data); i += 100 {
		pilot.sendRaw(data[i:min(i+100, len(data))])
		time.Sleep(time.Millisecond)
	}

	received := controller.expect("$FP")
	if !strings.Contains(received, route) {
		t.Errorf("flight plan was truncated: got %d bytes, sent %d",
			len(received), len(packet))
	}
}

func TestFlightPlanRoundTripsThroughQuery(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	controller := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	h.waitForOnline(t, 2)
	drain(controller)

	pilot.send("$FPCCA101:SERVER:I:H/B744:420:ZSPD:1200:1200:FL350:ZBAA:0:0:0:0::/V/:PIKAS W1 SASAN")
	controller.expect("$FP")

	controller.send("$CQZSSS_CTR:SERVER:FP:CCA101")

	plan := controller.expect("$FP")
	if !strings.Contains(plan, "PIKAS W1 SASAN") {
		t.Errorf("queried plan = %q, want the filed route", plan)
	}

	// The re-broadcast preserves what the pilot filed, wake-turbulence prefix
	// included: "H/B744" is valid ICAO and controller clients render it.
	//
	// The Python server rewrote the raw packet to strip it, via
	// `if "H/" or "T/" in self.aircraft` — a condition that is always true,
	// so the replacement ran on every plan.
	if !strings.Contains(plan, "H/B744") {
		t.Errorf("queried plan = %q, want the filed aircraft type preserved", plan)
	}
}

func TestParsedAircraftIsVerbatim(t *testing.T) {
	for _, input := range []string{"H/B744", "T/C172", "M/A320", "B738/L", "H/B77W/L", "A320", ""} {
		packet := []byte("$FPCCA101:SERVER:I:" + input + ":420:ZSPD:1200:1200:FL350:ZBAA:0:0:0:0::/V/:DCT")
		if got := ParseFlightPlan(packet, 0).Aircraft; got != input {
			t.Errorf("aircraft %q parsed as %q, want it unchanged", input, got)
		}
	}
}

func TestAuthChallengeAnswered(t *testing.T) {
	// The Python server only initialised this for pilots, so EuroScope,
	// vSTARS and vatSys could never complete the exchange.
	h := newHarness(t, harnessOptions{})

	controller := h.loginATC(t, "ZSSS_TWR", "1001", 5)
	h.waitForOnline(t, 1)

	// The client interrogates the server, which answers with $ZR. See the
	// $ZC/$ZR direction in openfsd's protocol reference.
	controller.send("$ZCZSSS_TWR:SERVER:de6acb8e")

	response := controller.expect("$ZR")
	parts := strings.Split(response, ":")
	if len(parts) < 3 {
		t.Fatalf("malformed $ZR: %q", response)
	}
	if len(parts[2]) != 32 {
		t.Errorf("$ZR response is %d characters, want 32: %q", len(parts[2]), parts[2])
	}
}

func TestClientQueryATCReportsActiveController(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	controller := h.loginATC(t, "ZSSS_TWR", "1001", 5)
	h.waitForOnline(t, 2)

	// Before a position packet the controller is still an observer.
	pilot.send("$CQCCA101:SERVER:ATC:ZSSS_TWR")
	if got := pilot.expect("$CRSERVER"); !strings.Contains(got, ":ATC:N:") {
		t.Errorf("unstaffed controller reported as active: %q", got)
	}

	controller.send("%ZSSS_TWR:28550:4:50:5:31.19670:121.33500:0")
	time.Sleep(200 * time.Millisecond)

	pilot.send("$CQCCA101:SERVER:ATC:ZSSS_TWR")
	if got := pilot.expect("$CRSERVER"); !strings.Contains(got, ":ATC:Y:") {
		t.Errorf("staffed controller reported as inactive: %q", got)
	}
}

func TestObserverCannotTakeControlPosition(t *testing.T) {
	t.Run("strict refuses", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementStrict})

		// CID 1000 is rated observer, and the callsign says so too — only the
		// facility field in the position packet claims otherwise.
		c := h.loginATC(t, "093_OBS", "1000", 1)
		h.waitForOnline(t, 1)
		drain(c)

		c.send("%093_OBS:28550:2:50:1:31.19670:121.33500:0")

		errPacket := c.expect("$ER")
		if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidPositionForRating)) {
			t.Errorf("$ER = %q, want InvalidPositionForRating (%d)",
				errPacket, ErrCodeInvalidPositionForRating)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if h.server.OnlineCount() == 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Error("an observer was left on a delivery position")
	})

	t.Run("warn refuses", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementWarn})

		c := h.loginATC(t, "093_OBS", "1000", 1)
		h.waitForOnline(t, 1)
		drain(c)

		c.send("%093_OBS:28550:2:50:1:31.19670:121.33500:0")

		errPacket := c.expect("$ER")
		if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidPositionForRating)) {
			t.Errorf("$ER = %q, want InvalidPositionForRating (%d)",
				errPacket, ErrCodeInvalidPositionForRating)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if h.server.OnlineCount() == 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Error("warn mode left an observer on a control position")
	})
}

// TestObserverKeepsObserverPosition is the other side of the floor: the
// position an observer is entitled to must stay untouched.
func TestObserverKeepsObserverPosition(t *testing.T) {
	h := newHarness(t, harnessOptions{enforcement: config.EnforcementWarn})

	c := h.loginATC(t, "ZSSS_OBS", "1000", 1)
	h.waitForOnline(t, 1)
	drain(c)

	c.send("%ZSSS_OBS:99998:0:50:1:31.19670:121.33500:0")
	c.expectNone("$ER", 800*time.Millisecond)

	if h.server.OnlineCount() != 1 {
		t.Error("an observer was disconnected from an observer position")
	}
}

// OBS account restrictions also apply when optional facility checks are off.
func TestObserverControlPositionRefusedWithEnforcementOff(t *testing.T) {
	h := newHarness(t, harnessOptions{enforcement: config.EnforcementOff})

	c := h.loginATC(t, "093_OBS", "1000", 1)
	h.waitForOnline(t, 1)
	drain(c)

	c.send("%093_OBS:28550:2:50:1:31.19670:121.33500:0")
	c.expect("$ER")
	h.waitForOnline(t, 0)
}

// TestATISLoginRatingFloor covers the login-time gate.
//
// An ATIS station is not obliged to send a position packet, so the check in
// handleATCPosition may never run for one. In production a single observer
// opened three ATIS stations without tripping anything.
func TestATISLoginRatingFloor(t *testing.T) {
	// loginATC waits for the MOTD, which a refused login never reaches, so the
	// handshake is written out here.
	login := func(c *testClient, callsign, cid string, rating int) {
		c.send(fmt.Sprintf("$ID%s:SERVER:69d7:EuroScope:3:2:%s:-582057156:63616665", callsign, cid))
		c.send(fmt.Sprintf("#AA%s:SERVER:Test Controller:%s:secret:%d:100", callsign, cid, rating))
	}

	t.Run("strict refuses an observer at login", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementStrict})

		c := h.dial(t)
		login(c, "ZBAD_ATIS", "1000", 1)

		errPacket := c.expect("$ER")
		if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidPositionForRating)) {
			t.Errorf("$ER = %q, want InvalidPositionForRating (%d)",
				errPacket, ErrCodeInvalidPositionForRating)
		}

		time.Sleep(200 * time.Millisecond)
		if h.server.OnlineCount() != 0 {
			t.Error("the refused ATIS station was registered anyway")
		}
	})

	t.Run("warn refuses an observer at login", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementWarn})

		c := h.dial(t)
		login(c, "ZBAD_ATIS", "1000", 1)

		c.expect("$ER")
		h.waitForOnline(t, 0)
	})

	t.Run("S1 and above is accepted", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementWarn})

		c := h.dial(t)
		login(c, "ZBAD_ATIS", "1001", 2)

		c.expect("#TMSERVER")
		h.waitForOnline(t, 1)
	})

	t.Run("off refuses an observer at login", func(t *testing.T) {
		h := newHarness(t, harnessOptions{enforcement: config.EnforcementOff})

		c := h.dial(t)
		login(c, "ZBAD_ATIS", "1000", 1)

		c.expect("$ER")
		h.waitForOnline(t, 0)
	})
}

func TestPingToServerIsAnswered(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 1)

	c.send("$PICCA101:SERVER:1736029820")

	pong := c.expect("$PO")
	if !strings.Contains(pong, "1736029820") {
		t.Errorf("$PO = %q, want the echoed timestamp", pong)
	}
}

func TestWallopReachesSupervisorsOnly(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	supervisor := h.loginATC(t, "ZSSS_SUP", "1011", 11)
	bystander := h.loginPilot(t, "UAL201", "1001", 101)
	h.waitForOnline(t, 3)

	drain(supervisor)
	drain(bystander)

	pilot.send("#TMCCA101:*S:need assistance please")

	got := supervisor.expect("#TMCCA101")
	if !strings.Contains(got, "need assistance") {
		t.Errorf("supervisor received %q, want the wallop", got)
	}

	bystander.expectNone("#TMCCA101:*S", 500*time.Millisecond)
}

func TestUnknownPacketTypeIsRejected(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 1)

	c.send("$QQCCA101:SERVER:nonsense")

	errPacket := c.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeSyntax)) {
		t.Errorf("$ER = %q, want Syntax (%d)", errPacket, ErrCodeSyntax)
	}
}

// drain discards whatever is currently queued so a later assertion is not
// confused by earlier traffic.
func drain(c *testClient) {
	// Give in-flight packets a moment to arrive before clearing, otherwise a
	// broadcast still crossing the wire lands after the drain and is mistaken
	// for the packet under test.
	time.Sleep(150 * time.Millisecond)

	for {
		select {
		case _, ok := <-c.packets:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// takeTrack has a controller claim an aircraft's radar track. The position
// packet comes first because IT is refused to an observer, and facility is
// only known once a controller has reported one.
func takeTrack(t *testing.T, controller *testClient, aircraft string) {
	t.Helper()

	controller.send("%" + controller.callsign + ":28550:4:50:5:31.19670:121.33500:0")
	controller.send("$CQ" + controller.callsign + ":@94835:IT:" + aircraft)
	time.Sleep(200 * time.Millisecond)
}

func TestAmendFlightPlanRequiresTrackOwnership(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	owner := h.loginATC(t, "ZSSS_CTR", "1001", 5)
	other := h.loginATC(t, "ZBAA_APP", "1011", 5)
	h.waitForOnline(t, 2)
	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 3)
	drain(owner)
	drain(other)
	drain(pilot)

	amend := "$AM%s:SERVER:CCA101:I:A320:100:ZPPP:1149:0:FL321:ZUUU:0:0:0:0::/V/:NODIB W137"
	other.send(fmt.Sprintf(amend, other.callsign))
	errPacket := other.expect("$ER")
	if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeInvalidControl)) {
		t.Fatalf("unauthorised amendment error = %q, want InvalidControl", errPacket)
	}

	takeTrack(t, owner, "CCA101")
	drain(owner)
	drain(other)
	other.send(fmt.Sprintf(amend, other.callsign))
	other.expect("$ER")
	owner.send(fmt.Sprintf(amend, owner.callsign))
	other.expect("$AM")
}

// TestClientQuerySVReturnsAnAddress: SV asks which server a client is on, so
// the answer is an address. It was returning the network name.
func TestClientQuerySVReturnsAnAddress(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	c := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 1)

	c.send("$CQCCA101:SERVER:SV")

	response := c.expect("$CRSERVER")
	address := FieldString([]byte(response), 3)

	if net.ParseIP(address) == nil {
		t.Errorf("SV answered %q, which is not an address", address)
	}
	if strings.Contains(response, "Cerulean Aviation Network") {
		t.Errorf("SV answered with the network name: %q", response)
	}
}

// awaitTextATIS waits for a controller's collected ATIS lines to be non-empty.
//
// The lines are appended by the server's own read goroutine, so they are not
// there the instant the write returns.
func (h *testHarness) awaitTextATIS(t *testing.T, callsign string) []string {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range h.server.postOffice.snapshot() {
			if c.Callsign == callsign && len(c.TextATIS()) > 0 {
				return c.TextATIS()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no ATIS text collected for %s", callsign)
	return nil
}

// TestTextATISRelayBetweenClientsStaysByteTransparent preserves legacy bytes.
func TestTextATISRelayBetweenClientsStaysByteTransparent(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	atis := h.loginATC(t, "ZSPD_ATIS", "1001", 5)
	pilot := h.loginPilot(t, "CCA101", "1000", 101)
	h.waitForOnline(t, 2)

	// Put both stations at the same place so the ranged paths agree.
	atis.send("%ZSPD_ATIS:22800:5:100:5:31.14:121.80:0")
	pilot.send("@N:CCA101:1200:1:31.14:121.80:1000:0:0:0")

	wire := "\xc5\xdc\xb5\xc0 synthetic ATIS"

	body := fmt.Sprintf("$CRZSPD_ATIS:CCA101:ATIS:T:%s\r\n", wire)
	atis.sendRaw([]byte(body))

	got := pilot.expect("$CRZSPD_ATIS")
	if want := strings.TrimSuffix(body, "\r\n"); got != want {
		t.Errorf("relayed ATIS = %q, want the original bytes %q", got, want)
	}
}

// TestSweatboxKeepsAccountRatingCeiling ensures sweatbox mode keeps the
// authenticated account as the login rating ceiling.
func TestSweatboxKeepsAccountRatingCeiling(t *testing.T) {
	t.Run("sweatbox accepts the account rating", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		h.loginATC(t, "ZSSS_APP", "1001", 5)
		h.waitForOnline(t, 1)
	})

	t.Run("sweatbox refuses above account rating", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		c := h.dial(t)
		c.send("$IDZSSS_APP:SERVER:69d7:EuroScope:3:2:1001:-582057156:63616665")
		c.send("#AAZSSS_APP:SERVER:Test Controller:1001:secret:6:100")

		errPacket := c.expect("$ER")
		if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeRequestedLevelTooHigh)) {
			t.Errorf("$ER = %q, want RequestedLevelTooHigh", errPacket)
		}
	})

	t.Run("live refuses one above", func(t *testing.T) {
		h := newHarness(t, harnessOptions{liveMode: true})
		c := h.dial(t)
		c.send("$IDZSSS_APP:SERVER:69d7:EuroScope:3:2:1001:-582057156:63616665")
		c.send("#AAZSSS_APP:SERVER:Test Controller:1001:secret:6:100")

		errPacket := c.expect("$ER")
		if !strings.Contains(errPacket, fmt.Sprintf(":%d:", ErrCodeRequestedLevelTooHigh)) {
			t.Errorf("$ER = %q, want RequestedLevelTooHigh", errPacket)
		}
	})
}
