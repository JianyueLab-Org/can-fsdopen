package fsd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/auth"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Handshake and idle timings.
const (
	// readTimeout is the idle limit once logged in. Clients send position
	// updates at least every 15 seconds and the server sends #DL keepalives,
	// so a minute of complete silence means the peer is gone.
	readTimeout = 90 * time.Second

	// writeTimeout bounds a single socket write.
	writeTimeout = 20 * time.Second

	tlsHandshakeTimeout = 10 * time.Second
)

// handleConn runs one connection from accept to close.
func (s *Server) handleConn(ctx, loginCtx context.Context, cancelLogin context.CancelFunc, conn net.Conn, lease *preAuthLease) {
	defer lease.release()
	loginClosed := make(chan struct{})
	loginDetached := false
	stopLoginClose := context.AfterFunc(loginCtx, func() {
		// TLS close_notify must not extend the absolute login deadline.
		if tlsConn, ok := conn.(*tls.Conn); ok {
			tlsConn.NetConn().Close()
		} else {
			conn.Close()
		}
		if errors.Is(loginCtx.Err(), context.DeadlineExceeded) {
			s.preAuth.timedOut()
		}
		close(loginClosed)
	})
	defer func() {
		if !loginDetached && !stopLoginClose() {
			<-loginClosed
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("connection goroutine panicked",
				"event", "panic", "stack", string(debug.Stack()))
			conn.Close()
		}
	}()

	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(loginCtx); err != nil {
			conn.Close()
			return
		}
	}

	reader := bufio.NewReaderSize(conn, 4096)

	if err := s.greet(conn, reader); err != nil {
		conn.Close()
		return
	}

	scanner := NewScanner(reader)

	deadline, _ := loginCtx.Deadline()
	conn.SetReadDeadline(deadline)
	login, password, err := readLoginPackets(conn, scanner)
	if err != nil {
		// readLoginPackets has already told the client why.
		conn.Close()
		return
	}

	client := newClient(ctx, conn, scanner, login)

	// **Every way out of here has to cancel, not just the successful one.**
	// newClient derives a child of the process-lifetime Serve context, which
	// registers it in that context's children map; only cancelling removes it.
	// The three rejection paths below — bad credentials, an ATIS rating
	// violation, a callsign already in use — all closed the socket and
	// returned, so each rejected login leaked a context.cancelCtx for the life
	// of the process. It is reachable before authentication with any
	// well-formed #AA/#AP. The store's limiter now combines the CID with the
	// socket peer and also caps failures from that peer across CIDs.
	//
	// Deferred here rather than repeated at each return: cancel is idempotent,
	// so the copy in the registered-client cleanup below is harmless, and a
	// rejection path added later is covered without anyone remembering to.
	defer client.cancel()

	if err := s.authenticate(loginCtx, client, password); err != nil {
		// authenticate writes the $ER synchronously, so the client sees it
		// even though no writer goroutine is running yet.
		conn.Close()
		return
	}

	// OBS accounts cannot publish ATIS in any mode. Other below-floor
	// declarations keep the configured enforcement behavior.
	atisViolation := s.atisRatingViolation(client)
	if atisViolation != nil && (client.MaxRating == RatingObserver ||
		s.config.Get().FacilityEnforcement == config.EnforcementStrict) {
		writeSync(conn, errorPacket(login.Callsign, ErrCodeInvalidPositionForRating, atisViolation.detail))

		slog.Info("ATIS login refused",
			"callsign", client.Callsign,
			"rating", int(client.NetworkRating), "reason", atisViolation.reason)

		conn.Close()
		return
	}

	if loginCtx.Err() != nil {
		conn.Close()
		return
	}
	if err := s.postOffice.register(client); err != nil {
		switch {
		case errors.Is(err, ErrCallsignInUse):
			writeSync(conn, errorPacket(login.Callsign, ErrCodeCallsignInUse, "Callsign already in use"))
		case errors.Is(err, ErrServerFull):
			writeSync(conn, errorPacket(login.Callsign, ErrCodeServerFull, "Server full"))
		}
		conn.Close()
		return
	}
	// A deadline that wins this race must finish closing before cleanup.
	// Roll registration back before any session or writer starts.
	if !stopLoginClose() {
		<-loginClosed
		s.postOffice.release(client, s.onDisconnect)
		conn.Close()
		return
	}
	loginDetached = true
	if loginCtx.Err() != nil {
		s.postOffice.release(client, s.onDisconnect)
		conn.Close()
		return
	}
	cancelLogin()
	conn.SetDeadline(time.Time{})
	lease.release()

	// From here on the writer goroutine owns the socket.
	go client.writerLoop(writeTimeout)

	slog.Info("client connected",
		"callsign", client.Callsign,
		"rating", int(client.NetworkRating),
		"atc", client.IsATC,
		"proto", client.ProtoRevision)

	defer func() {
		s.postOffice.release(client, s.onDisconnect)
		client.cancel()

		slog.Info("client disconnected",
			"callsign", client.Callsign,
			"event", "closed")
	}()

	s.sendMOTD(client)
	if atisViolation != nil {
		s.warnFacility(client, FacilityATIS, atisViolation)
	}
	s.broadcastAddPacket(client)
	defer s.broadcastDeletePacket(client)

	s.restoreFlightPlan(client)

	s.eventLoop(client)
}

// eventLoop reads and dispatches packets until the connection ends.
//
// One goroutine, in order. The Python server spawned a thread per packet,
// which meant a position update could overtake the login that preceded it.
func (s *Server) eventLoop(client *Client) {
	defer client.cancel()
	cfg := s.config.Get()
	client.inboundPackets = newTokenBucket(cfg.InboundPacketsPerSecond, time.Second)
	client.inboundBytes = newTokenBucket(cfg.InboundBytesPerSecond, time.Second)

	// Stop reading when the context is cancelled from elsewhere (a kick, a
	// shutdown). Unblocking the read requires touching the deadline.
	go func() {
		<-client.ctx.Done()
		client.conn.SetReadDeadline(time.Now())
	}()

	for {
		client.conn.SetReadDeadline(time.Now().Add(readTimeout))

		if !client.scanner.Scan() {
			if err := client.scanner.Err(); err != nil {
				client.Disconnect(readErrorReason(err, client.ctx.Err() != nil))
			} else {
				client.Disconnect("client closed connection")
			}
			return
		}

		if client.ctx.Err() != nil {
			return
		}

		packet := client.scanner.Bytes()
		if !client.inboundPackets.take() || !client.inboundBytes.takeN(len(packet)+2) {
			client.Disconnect("inbound rate limit exceeded")
			return
		}
		if len(packet) == 0 {
			continue
		}

		packetType, ok := s.verifyPacket(client, packet)
		if !ok {
			continue
		}

		// Re-append the delimiter: handlers forward packets verbatim and the
		// scanner strips it.
		full := make([]byte, 0, len(packet)+2)
		full = append(full, packet...)
		full = append(full, '\r', '\n')

		s.dispatch(client, packetType, full)
	}
}

// readErrorReason turns a failed read into the sentence that lands in the
// disconnect log.
//
// The three ways a read can time out need different answers from whoever is
// reading the log, and for a long time all three said "idle timeout":
//
//   - our own deadline fired after readTimeout — the peer really did go quiet
//   - our own deadline was tripped to *now* by the watcher in eventLoop, to
//     unblock the read after a cancel — the server took the connection down,
//     and the peer may have been perfectly healthy
//   - the kernel gave up retransmitting (ETIMEDOUT), which net.Error also
//     reports as Timeout() — the peer became unreachable mid-connection
//
// Telling the first from the second is what `cancelled` is for; telling the
// first from the third is what makes os.ErrDeadlineExceeded the right test
// rather than netErr.Timeout(). Only a deadline *we* set wraps it, so anything
// else claiming Timeout() came from the network.
//
// A disconnect chased for an afternoon turned on exactly this: the log said
// "read error: …", and only knowing that a server-initiated close would have
// said "idle timeout" instead established that the socket had genuinely been
// reset from outside.
func readErrorReason(err error, cancelled bool) string {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		if cancelled {
			return "closed by server"
		}
		return "idle timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "peer unreachable"
	}
	if errors.Is(err, bufio.ErrTooLong) {
		return "packet exceeded maximum size"
	}
	return "read error: " + err.Error()
}

// verifyPacket runs the structural checks common to every packet type.
func (s *Server) verifyPacket(client *Client, packet []byte) (PacketType, bool) {
	packetType := GetPacketType(packet)
	if packetType == PacketUnknown {
		client.SendError(ErrCodeSyntax, "Unknown packet type")
		return PacketUnknown, false
	}

	if n := minFields(packetType); n > 0 && CountFields(packet) < n {
		client.SendError(ErrCodeSyntax, "Too few fields for "+PacketPrefix(packetType))
		return PacketUnknown, false
	}

	// A client may only speak as itself. Without this check any station could
	// forge position reports or text messages attributed to another callsign.
	if !bytes.Equal(SourceCallsign(packet, packetType), []byte(client.Callsign)) {
		client.SendError(ErrCodeSourceInvalid, "Source callsign does not match")
		return PacketUnknown, false
	}

	return packetType, true
}

// greet sends the $DI, unless the client turns out not to want one.
//
// EuroScope connecting to a server it was told is not VATSIM drops the
// connection the moment a $DI arrives — measured against every variant, banner
// and challenge key alike — and reports "VATSIM server found while non-VATSIM
// server was requested". Its own bundled server (EuroScopeFsdServer v1.2j)
// sends no greeting, so after about 750ms of silence EuroScope gives up
// waiting and opens the conversation with #AA, skipping $ID entirely.
//
// That gives two signals rather than one, which matters because the same
// EuroScope with "Connect to VATSIM" ticked does want the greeting:
//
//	silent past the delay  -> greet. vPilot and the rest wait indefinitely.
//	opens with $ID         -> greet. Only a client running the VATSIM
//	                          handshake sends one, and it is still waiting
//	                          for the challenge key the $DI carries.
//	opens with anything else -> stay quiet. This is the non-VATSIM EuroScope.
//
// So a client that gives up on the greeting early is not thereby denied it.
//
// IdentDelayMS of 0 restores the unconditional greeting.
func (s *Server) greet(conn net.Conn, reader *bufio.Reader) error {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		tlsConn.SetDeadline(time.Now().Add(tlsHandshakeTimeout))
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		tlsConn.SetDeadline(time.Time{})
	}

	delay := time.Duration(s.config.Get().IdentDelayMS) * time.Millisecond
	if delay <= 0 {
		return s.sendServerIdent(conn)
	}

	// Three bytes is the whole decision. Peek leaves them buffered for the
	// scanner, and consumes the deadline error itself, so the reader is clean
	// whichever way this goes.
	conn.SetReadDeadline(time.Now().Add(delay))
	head, err := reader.Peek(3)
	conn.SetReadDeadline(time.Time{})

	switch {
	case len(head) > 0:
		if bytes.HasPrefix(head, []byte("$ID")) {
			return s.sendServerIdent(conn)
		}
		return nil
	case isTimeout(err):
		return s.sendServerIdent(conn)
	default:
		return err
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// sendServerIdent sends the $DI greeting.
func (s *Server) sendServerIdent(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := conn.Write([]byte(s.identPacket))
	return err
}

func writeSync(conn net.Conn, packet string) {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	conn.Write([]byte(packet))
}

func errorPacket(callsign string, code int, message string) string {
	if callsign == "" {
		callsign = "unknown"
	}
	return fmt.Sprintf("$ERserver:%s:%d::%s\r\n", callsign, code, message)
}

// Login parse failures.
var (
	errBadIdentPacket = errors.New("fsd: invalid $ID packet")
	errBadAddPacket   = errors.New("fsd: invalid add packet")
)

// readLoginPackets consumes the $ID and #AP/#AA packets that open a session.
//
// Every rejection sends an $ER first. The Python server closed the socket with
// only a text message, or in several paths silently swallowed the error, so a
// client could not tell a wrong password from a network fault.
func readLoginPackets(conn net.Conn, scanner *bufio.Scanner) (login LoginData, password string, err error) {
	if !scanner.Scan() {
		writeSync(conn, errorPacket("", ErrCodeSyntax, "Expected client identification"))
		return login, "", errBadIdentPacket
	}
	addPacket := append([]byte(nil), scanner.Bytes()...)

	// $ID is optional, so the handshake may open with #AP/#AA instead. Waiting
	// unconditionally for a second packet would hang such a client until
	// the login deadline, which is what happened to clients that skip $ID.
	if bytes.HasPrefix(addPacket, []byte("$ID")) {
		parseIdentPacket(addPacket, &login)

		if !scanner.Scan() {
			writeSync(conn, errorPacket("", ErrCodeSyntax, "Expected add packet"))
			return login, "", errBadAddPacket
		}
		addPacket = append([]byte(nil), scanner.Bytes()...)
	}

	if err := parseAddPacket(addPacket, &login, &password); err != nil {
		writeSync(conn, errorPacket(login.Callsign, ErrCodeSyntax, err.Error()))
		return login, "", errBadAddPacket
	}

	if !IsValidCallsign([]byte(login.Callsign)) {
		writeSync(conn, errorPacket(login.Callsign, ErrCodeCallsignInvalid, "Callsign invalid"))
		return login, "", errBadAddPacket
	}

	if !SupportedProtoRevision(login.ProtoRevision) {
		writeSync(conn, errorPacket(login.Callsign, ErrCodeInvalidRevision, "Unsupported protocol revision"))
		return login, "", errBadAddPacket
	}

	login.LoginTime = time.Now()
	login.IsATIS = login.IsATC && IsATISCallsign(login.Callsign)

	return login, password, nil
}

// parseIdentPacket reads the optional $ID fields.
//
// $ID is advisory: a client that omits it simply cannot answer auth
// challenges. Missing or malformed fields are therefore not fatal.
//
// Layout: $IDcallsign:SERVER:clientID:name:major:minor:cid:sysUID:challenge
func parseIdentPacket(packet []byte, login *LoginData) {
	if GetPacketType(packet) != PacketUnknown || !bytes.HasPrefix(packet, []byte("$ID")) {
		// $ID has no entry in the packet type table; accept it by prefix.
		if !bytes.HasPrefix(packet, []byte("$ID")) {
			return
		}
	}

	login.ClientName = FieldString(packet, 3)

	if id, err := strconv.ParseUint(FieldString(packet, 2), 16, 16); err == nil {
		login.ClientID = uint16(id)
	}

	// The challenge is the ninth field. Clients that predate the auth scheme
	// send only eight.
	if CountFields(packet) >= 9 {
		login.ClientChallenge = FieldString(packet, 8)
	}
}

// parseAddPacket reads #AP (pilot) or #AA (ATC).
//
//	#AP callsign:SERVER:cid:token:rating:proto:simulator:realname
//	#AA callsign:SERVER:realname:cid:token:rating:proto
func parseAddPacket(packet []byte, login *LoginData, password *string) error {
	if len(packet) < 4 {
		return errors.New("Add packet too short")
	}

	var prefix string
	switch string(packet[:3]) {
	case "#AA":
		login.IsATC = true
		prefix = "#AA"
	case "#AP":
		prefix = "#AP"
	default:
		return errors.New("Expected #AP or #AA")
	}

	callsign, ok := bytes.CutPrefix(Field(packet, 0), []byte(prefix))
	if !ok {
		return errors.New("Malformed callsign field")
	}
	login.Callsign = string(callsign)

	var err error
	if login.IsATC {
		if CountFields(packet) < 7 {
			return errors.New("ATC add packet needs 7 fields")
		}
		login.RealName = FieldString(packet, 2)
		login.CID = FieldString(packet, 3)
		*password = FieldString(packet, 4)

		if login.NetworkRating, err = parseRating(FieldString(packet, 5)); err != nil {
			return errors.New("Invalid network rating")
		}
		if login.ProtoRevision, err = strconv.Atoi(FieldString(packet, 6)); err != nil {
			return errors.New("Invalid protocol revision")
		}
	} else {
		if CountFields(packet) < 8 {
			return errors.New("Pilot add packet needs 8 fields")
		}
		login.CID = FieldString(packet, 2)
		*password = FieldString(packet, 3)

		if login.NetworkRating, err = parseRating(FieldString(packet, 4)); err != nil {
			return errors.New("Invalid network rating")
		}
		if login.ProtoRevision, err = strconv.Atoi(FieldString(packet, 5)); err != nil {
			return errors.New("Invalid protocol revision")
		}
		login.SimulatorType, _ = strconv.Atoi(FieldString(packet, 6))
		login.RealName = FieldString(packet, 7)
	}

	if login.CID == "" {
		return errors.New("Missing CID")
	}

	return nil
}

func parseRating(field string) (NetworkRating, error) {
	n, err := strconv.Atoi(field)
	if err != nil {
		return 0, err
	}
	return NetworkRating(n), nil
}

// authenticate verifies credentials and prepares the VATSIM auth state.
func (s *Server) authenticate(loginCtx context.Context, client *Client, password string) error {
	ctx, cancel := context.WithTimeout(loginCtx, 10*time.Second)
	defer cancel()

	if err := ctx.Err(); err != nil {
		return err
	}
	releaseAuth, err := s.preAuth.beginAuth(client.RemoteIP(), s.config.Get())
	if err != nil {
		code, msg := authErrorPacket(err)
		writeSync(client.conn, errorPacket(client.Callsign, code, msg))
		return err
	}
	defer releaseAuth()

	// Set up challenge/response first so an unsupported client is told before
	// its credentials are even checked.
	if client.ClientChallenge != "" {
		if err := client.authState.Initialize(client.ClientID, s.config.Get().ClientAuthKeys[client.ClientID], []byte(client.ClientChallenge)); err != nil {
			slog.Debug("client cannot participate in auth challenges",
				"callsign", client.Callsign, "client_id", client.ClientID, "err", err)
			// Not fatal: the client simply will not be challenged. Rejecting
			// here would lock out every client whose ID is not in the table.
		}
	}

	if s.recentlyKicked(client.CID) {
		writeSync(client.conn, errorPacket(client.Callsign, ErrCodeCertificateSuspended,
			"Recently disconnected by a supervisor; try again later"))
		return errors.New("fsd: kicked CID")
	}

	user, err := s.users.Authenticate(ctx, client.CID, password, int(client.NetworkRating), client.RemoteIP())
	if err != nil {
		code, msg := authErrorPacket(err)
		writeSync(client.conn, errorPacket(client.Callsign, code, msg))

		slog.Info("login rejected",
			"callsign", client.Callsign,
			"code", code)
		return err
	}

	if user == nil || user.ID == "" || user.Rating < int(RatingObserver) || int(client.NetworkRating) > user.Rating {
		writeSync(client.conn, errorPacket(client.Callsign, ErrCodeInvalidLogon, "Invalid identity/password"))
		return auth.ErrRatingDenied
	}
	client.CID = user.ID
	client.MaxRating = NetworkRating(user.Rating)

	return nil
}

// authErrorPacket maps a store error onto an FSD error code and message.
func authErrorPacket(err error) (int, string) {
	switch {
	case errors.Is(err, errAuthAttempts):
		return ErrCodeInvalidLogon, "Too many login attempts; try again later"
	case errors.Is(err, auth.ErrUnknownIdentity), errors.Is(err, auth.ErrInvalidCredential):
		// Deliberately identical for both: distinguishing them would let an
		// unauthenticated caller enumerate valid CIDs.
		return ErrCodeInvalidLogon, "Invalid CID/password"
	case errors.Is(err, auth.ErrInactive):
		return ErrCodeCertificateSuspended, "Certificate inactive or suspended"
	case errors.Is(err, auth.ErrRatingDenied):
		return ErrCodeRequestedLevelTooHigh, "Requested level too high"
	case errors.Is(err, auth.ErrRateLimited):
		return ErrCodeInvalidLogon, "Too many failed attempts; try again later"
	default:
		return ErrCodeInvalidLogon, "Authentication unavailable"
	}
}

// sendMOTD sends the configured welcome message, one #TM per line.
func (s *Server) sendMOTD(client *Client) {
	welcome := s.config.Get().Welcome
	if welcome == "" {
		return
	}

	for line := range strings.SplitSeq(welcome, "\n") {
		if line = strings.TrimRight(line, "\r"); line != "" {
			client.SendServerMessage(line)
		}
	}
}

// broadcastAddPacket announces pilots globally and positioned controllers locally.
//
// The token field is omitted, as the protocol requires: re-broadcasting a
// login packet verbatim would hand every connected client the newcomer's
// password.
func (s *Server) broadcastAddPacket(client *Client) {
	if client.IsATC {
		s.syncControllerVisibility(client)
		return
	}

	s.broadcastAll(client, fmt.Sprintf("#AP%s:SERVER:%s::%d:%d:%d:%s\r\n",
		client.Callsign, client.CID,
		int(client.NetworkRating), client.ProtoRevision,
		client.SimulatorType, client.RealName))
}

// broadcastDeletePacket announces a departing station.
func (s *Server) broadcastDeletePacket(client *Client) {
	prefix := "#DP"
	if client.IsATC {
		s.postOffice.all(client, func(recipient *Client) bool {
			recipient.hideController(client)
			return true
		})
		return
	}

	s.broadcastAll(client, fmt.Sprintf("%s%s:%s\r\n", prefix, client.Callsign, client.CID))
}
