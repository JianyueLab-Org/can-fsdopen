package fsd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Protocol revisions this server speaks. 101 adds the Velocity fast-position
// packets; 100 is the classic protocol.
//
// 9 is what EuroScope announces to a server it was told is not VATSIM — the
// revision its own bundled FSD server speaks. It carries no extra capability,
// so it is handled exactly like 100.
const (
	ProtoRevisionPrivate  = 9
	ProtoRevisionClassic  = 100
	ProtoRevisionVelocity = 101
)

// SupportedProtoRevision reports whether a login may proceed on a revision.
func SupportedProtoRevision(revision int) bool {
	switch revision {
	case ProtoRevisionPrivate, ProtoRevisionClassic, ProtoRevisionVelocity:
		return true
	}
	return false
}

// LoginData is everything established during the handshake. These fields are
// fixed once the connection is live and may be read without synchronisation.
type LoginData struct {
	Callsign      string
	CID           string
	RealName      string
	NetworkRating NetworkRating
	MaxRating     NetworkRating // authenticated account rating, never a client claim
	ProtoRevision int
	SimulatorType int
	IsATC         bool
	IsATIS        bool
	LoginTime     time.Time

	// ClientID and ClientChallenge come from the $ID packet and seed the
	// VATSIM auth exchange. Zero when the client did not offer a challenge.
	ClientID        uint16
	ClientChallenge string
	ClientName      string
}

type Client struct {
	LoginData

	conn    net.Conn
	scanner *bufio.Scanner

	ctx    context.Context
	cancel context.CancelFunc

	// sendCh is the only path to the socket. A single writer goroutine drains
	// it, which is what keeps concurrent broadcasts from interleaving bytes
	// mid-packet — the Python server called sendall from arbitrary threads.
	sendCh chan string

	// closeOnce guards the disconnect reason so the first cause wins.
	closeOnce sync.Once
	closeErr  atomic.Pointer[string]

	coordinates atomic.Pointer[coordinatePosition]
	visRange    atomic.Uint64 // float64 bits, metres

	facilityType atomic.Int32
	textATIS     atomic.Pointer[[]string]
	atisMu       sync.Mutex
	pendingATIS  *textATISResponse
	drainATIS    bool

	flightPlan   atomic.Pointer[FlightPlan]
	beaconCode   atomic.Pointer[string]
	lastPosition atomic.Int64 // unix nanoseconds
	positioned   atomic.Bool  // a position packet has been seen
	atcPosition  atomic.Pointer[string]

	visibleMu          sync.Mutex
	visibleControllers map[string]*Client
	inboundPackets     *tokenBucket
	inboundBytes       *tokenBucket
	wallopBudget       *tokenBucket

	sendFastEnabled atomic.Bool

	// authState is the VATSIM challenge/response machine. Only the read
	// goroutine touches it, so it needs no synchronisation.
	authState AuthState

	warnMu        sync.Mutex
	warnedReasons map[string]bool
}

func newClient(ctx context.Context, conn net.Conn, scanner *bufio.Scanner, login LoginData) *Client {
	clientCtx, cancel := context.WithCancel(ctx)

	c := &Client{
		LoginData:     login,
		conn:          conn,
		scanner:       scanner,
		ctx:           clientCtx,
		cancel:        cancel,
		sendCh:        make(chan string, 64),
		warnedReasons: make(map[string]bool, 4),
	}

	c.setVisRange(defaultVisRange(login.IsATC))
	c.setTextATIS(nil)
	c.facilityType.Store(int32(FacilityObserver))
	c.lastPosition.Store(time.Now().UnixNano())
	context.AfterFunc(clientCtx, c.discardTextATISRefresh)

	return c
}

const pilotVisRangeNM = 40

func defaultVisRange(isATC bool) float64 {
	if isATC {
		// A controller's real range arrives with its first % packet. Until
		// then keep it small so an unpositioned station at 0,0 does not
		// receive traffic from halfway around the world.
		return 20 * MetresPerNauticalMile
	}
	return pilotVisRangeNM * MetresPerNauticalMile
}

// Context returns the client's context, cancelled on disconnect.
func (c *Client) Context() context.Context { return c.ctx }

// Disconnect schedules the connection to close. The reason is recorded for
// logging; the first one wins.
func (c *Client) Disconnect(reason string) {
	c.closeOnce.Do(func() {
		c.closeErr.Store(&reason)
	})
	c.cancel()
}

// DisconnectReason returns why the client was closed, if known.
func (c *Client) DisconnectReason() string {
	if r := c.closeErr.Load(); r != nil {
		return *r
	}
	return "connection closed"
}

// RemoteIP returns the client's address without the port.
func (c *Client) RemoteIP() string {
	return hostOf(c.conn.RemoteAddr().String())
}

// LocalAddr returns the address this client reached the server on, without the
// port. Used to answer the $CQ SV query, which asks which server a client is
// connected to.
func (c *Client) LocalAddr() string {
	return hostOf(c.conn.LocalAddr().String())
}

func hostOf(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// Send queues a packet. It never blocks indefinitely: if the client's outbound
// buffer is full it has stopped reading, and the connection is dropped rather
// than allowing one stalled station to block a broadcast to everyone else.
func (c *Client) Send(packet string) error {
	select {
	case c.sendCh <- packet:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// Buffer full. Give it a brief grace period before giving up.
	select {
	case c.sendCh <- packet:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-time.After(2 * time.Second):
		c.Disconnect("outbound buffer full: client not reading")
		return fmt.Errorf("fsd: send buffer full for %s", c.Callsign)
	}
}

// SendBestEffort queues maintenance traffic without waiting behind a stalled
// client's outbound buffer. The normal Send path remains for protocol replies.
// TrySend queues a packet without disconnecting a client whose queue is full.
// Periodic maintenance uses this path so one stalled station cannot delay
// heartbeats or the ATIS poll for every other station.
func (c *Client) TrySend(packet string) bool {
	select {
	case c.sendCh <- packet:
		return true
	case <-c.ctx.Done():
		return false
	default:
		return false
	}
}

func (c *Client) SendBestEffort(packet string) error {
	select {
	case c.sendCh <- packet:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
		c.Disconnect("outbound buffer full: client not reading")
		return fmt.Errorf("fsd: maintenance buffer full for %s", c.Callsign)
	}
}

// acceptInbound consumes the per-client inbound budgets for one packet.
// Returning false disconnects the sender before its packet can reach a
// handler or be relayed to other clients.
func (c *Client) acceptInbound(packet []byte) bool {
	if c.inboundPackets == nil || c.inboundBytes == nil {
		return true
	}
	if c.inboundPackets.take() && c.inboundBytes.takeN(len(packet)) {
		return true
	}
	c.Disconnect("inbound rate limit exceeded")
	return false
}

// SendError queues an $ER packet.
func (c *Client) SendError(code int, message string) {
	var b strings.Builder
	b.Grow(64 + len(message))
	b.WriteString("$ERserver:")
	if c.Callsign != "" {
		b.WriteString(c.Callsign)
	} else {
		b.WriteString("unknown")
	}
	b.WriteByte(':')
	b.WriteString(strconv.Itoa(code))
	b.WriteString("::")
	b.WriteString(message)
	b.WriteString("\r\n")

	c.Send(b.String())
}

// SendServerMessage queues a #TM from SERVER.
func (c *Client) SendServerMessage(message string) {
	var b strings.Builder
	b.Grow(32 + len(message))
	b.WriteString("#TMSERVER:")
	b.WriteString(c.Callsign)
	b.WriteByte(':')
	b.WriteString(message)
	b.WriteString("\r\n")

	c.Send(b.String())
}

// writeErrorReason turns a failed write into a disconnect reason.
//
// The deadline case is worth its own wording: it means the client's receive
// window stayed shut for writeTimeout, i.e. the process is alive on the socket
// but has stopped draining it — a stalled or swapped-out client, which is a
// different problem from an unreachable one.
func writeErrorReason(err error) string {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "write timeout: client not reading"
	}
	return "write error: " + err.Error()
}

// writerLoop owns the socket for writing. It is the only goroutine that ever
// writes to conn after the handshake.
func (c *Client) writerLoop(writeTimeout time.Duration) {
	defer c.conn.Close()
	defer c.cancel()

	for {
		select {
		case packet := <-c.sendCh:
			if writeTimeout > 0 {
				c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			}
			if _, err := c.conn.Write([]byte(packet)); err != nil {
				// Record why. Without this the write side dies silently: the
				// deferred cancel trips the reader's deadline and the whole
				// disconnect is logged as a read timeout, pointing at a client
				// that had in fact stopped *reading*. Disconnect is
				// first-wins, so this only ever fills in a blank.
				c.Disconnect(writeErrorReason(err))
				return
			}

		case <-c.ctx.Done():
			// Drain whatever is already queued so a disconnect notice or
			// error packet still reaches the client, then close.
			c.conn.SetWriteDeadline(time.Now().Add(time.Second))
			for {
				select {
				case packet := <-c.sendCh:
					if _, err := c.conn.Write([]byte(packet)); err != nil {
						return
					}
				default:
					return
				}
			}
		}
	}
}

// --- atomic accessors ---

func float64bits(f float64) uint64 { return math.Float64bits(f) }
func float64from(u uint64) float64 { return math.Float64frombits(u) }

// LatLon returns the client's position.
func (c *Client) LatLon() [2]float64 {
	position, _ := c.LatLonTime()
	return position
}

type coordinatePosition struct {
	latLon [2]float64
	at     time.Time
}

// LatLonTime returns one coordinate snapshot and its authoritative update time.
// The time is zero until coordinates have been reported.
func (c *Client) LatLonTime() ([2]float64, time.Time) {
	if position := c.coordinates.Load(); position != nil {
		return position.latLon, position.at
	}
	return [2]float64{}, time.Time{}
}

func (c *Client) setLatLon(lat, lon float64) {
	c.coordinates.Store(&coordinatePosition{latLon: [2]float64{lat, lon}, at: time.Now()})
}

// VisRange returns the visibility radius in metres.
func (c *Client) VisRange() float64 { return float64from(c.visRange.Load()) }

func (c *Client) setVisRange(metres float64) { c.visRange.Store(float64bits(metres)) }

// FacilityType returns the controller's facility.
func (c *Client) FacilityType() int { return int(c.facilityType.Load()) }

func (c *Client) setFacilityType(f int) { c.facilityType.Store(int32(f)) }

// activeController requires an authenticated controller account and a
// non-observer position. Callsigns and position-packet ratings grant no authority.
func (c *Client) activeController() bool {
	return c.IsATC && c.MaxRating > RatingObserver && c.FacilityType() > FacilityObserver
}

// TextATIS returns the controller's complete ATIS snapshot. The returned slice
// is immutable and must not be modified by callers.
func (c *Client) TextATIS() []string {
	if p := c.textATIS.Load(); p != nil {
		return *p
	}
	return nil
}

func (c *Client) setTextATIS(lines []string) {
	snapshot := slices.Clone(lines)
	c.textATIS.Store(&snapshot)
}

// FlightPlanData returns the filed plan, or nil.
func (c *Client) FlightPlanData() *FlightPlan { return c.flightPlan.Load() }

func (c *Client) setFlightPlan(fp *FlightPlan) { c.flightPlan.Store(fp) }

// BeaconCode returns the assigned squawk, or "0" if none.
func (c *Client) BeaconCode() string {
	if p := c.beaconCode.Load(); p != nil && *p != "" {
		return *p
	}
	return "0"
}

func (c *Client) setBeaconCode(code string) { c.beaconCode.Store(&code) }

// LastPosition returns when the client last sent a position packet.
func (c *Client) LastPosition() time.Time { return time.Unix(0, c.lastPosition.Load()) }

func (c *Client) touchPosition() {
	c.lastPosition.Store(time.Now().UnixNano())
	c.positioned.Store(true)
}

func (c *Client) HasPosition() bool { return c.positioned.Load() }

// warnOnce reports whether this reason has already been reported for this
// connection, marking it as reported.
func (c *Client) warnOnce(reason string) bool {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()

	if c.warnedReasons[reason] {
		return false
	}
	c.warnedReasons[reason] = true
	return true
}
