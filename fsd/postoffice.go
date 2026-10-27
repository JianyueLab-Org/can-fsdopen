package fsd

import (
	"errors"
	"github.com/tidwall/rtree"
	"math"
	"sync"
)

// Errors returned by the post office.
var (
	ErrCallsignInUse        = errors.New("fsd: callsign in use")
	ErrCallsignDoesNotExist = errors.New("fsd: callsign does not exist")
	ErrServerFull           = errors.New("fsd: server full")
)

// postOffice is the client registry and geographic index.
//
// Range-limited delivery goes through an R-tree keyed on each client's
// visibility bounding box. Boxes crossing the antimeridian have two disjoint
// longitude pieces in [-180, 180]; polar and full-longitude boxes have one.
type postOffice struct {
	mu      sync.RWMutex
	clients map[string]*Client
	maxSize int
	treeMu  sync.RWMutex
	tree    rtree.RTreeG[*spatialEntry]
	spatial map[*Client]*spatialEntry // protected by treeMu
}

type spatialBox struct {
	min, max [2]float64
}

type spatialBounds struct {
	boxes [2]spatialBox // ascending longitude; at most two disjoint pieces
	count int
}

type spatialEntry struct {
	client *Client
	bounds spatialBounds
}

func newPostOffice(maxClients int) *postOffice {
	return &postOffice{
		clients: make(map[string]*Client, 128),
		maxSize: maxClients,
		spatial: make(map[*Client]*spatialEntry, 128),
	}
}

// register admits a client under its callsign.
func (p *postOffice) register(c *Client) error {
	p.mu.Lock()
	if len(p.clients) >= p.maxSize {
		p.mu.Unlock()
		return ErrServerFull
	}
	if _, exists := p.clients[c.Callsign]; exists {
		p.mu.Unlock()
		return ErrCallsignInUse
	}
	p.clients[c.Callsign] = c
	p.mu.Unlock()

	p.treeMu.Lock()
	entry := &spatialEntry{client: c, bounds: visibilityBounds(c.LatLon(), c.VisRange())}
	p.spatial[c] = entry
	for i := range entry.bounds.count {
		box := &entry.bounds.boxes[i]
		p.tree.Insert(box.min, box.max, entry)
	}
	p.treeMu.Unlock()

	return nil
}

// release removes a client and cleans its session state before callsign reuse.
// cleanup runs under mu and must not send packets or look up clients.
func (p *postOffice) release(c *Client, cleanup func(*Client)) {
	p.treeMu.Lock()
	if entry := p.spatial[c]; entry != nil {
		for i := range entry.bounds.count {
			box := &entry.bounds.boxes[i]
			p.tree.Delete(box.min, box.max, entry)
		}
		delete(p.spatial, c)
	}
	p.treeMu.Unlock()

	p.mu.Lock()
	// Only drop the map entry if it still points at this client. A
	// reconnecting station that won the callsign in between must not have its
	// entry removed by the outgoing connection's cleanup.
	if current, ok := p.clients[c.Callsign]; ok && current == c {
		cleanup(c)
		delete(p.clients, c.Callsign)
	}
	p.mu.Unlock()
}

// updatePosition moves a client within the index.
func (p *postOffice) updatePosition(c *Client, centre [2]float64, visRange float64) {
	p.treeMu.Lock()
	defer p.treeMu.Unlock()

	c.setLatLon(centre[0], centre[1])
	c.setVisRange(visRange)
	entry := p.spatial[c]
	if entry == nil {
		// A late position packet must not reinsert a released session.
		return
	}
	bounds := visibilityBounds(centre, visRange)
	if entry.bounds == bounds {
		return
	}
	for i := range entry.bounds.count {
		box := &entry.bounds.boxes[i]
		p.tree.Delete(box.min, box.max, entry)
	}
	entry.bounds = bounds
	for i := range entry.bounds.count {
		box := &entry.bounds.boxes[i]
		p.tree.Insert(box.min, box.max, entry)
	}
}

// searchResult carries what a range search learned about the sender in
// addition to visiting each recipient.
type searchResult struct {
	// closestVelocityPeer is the distance in metres to the nearest other
	// client that speaks protocol revision 101, or +Inf if there is none. It
	// drives the $SF send-fast toggle.
	closestVelocityPeer float64
}

// search visits every other client whose box overlaps the sender's.
//
// **The callback runs after the tree read lock is released, never inside it.**
// It used to run inside, on the reasoning that a broadcast only queues to a
// buffered channel — but Client.Send waits up to two seconds when that buffer
// is full before giving up and dropping the station. One client with a closed
// TCP receive window therefore parked the broadcasting goroutine inside
// treeMu.RLock for two seconds; updatePosition wants the write lock, and Go's
// RWMutex blocks new readers the moment a writer queues, so every position
// packet on the network stopped for as long as the stall lasted. Nothing caps
// connections per CID, so that was repeatable at will.
//
// The cost is one slice per broadcast. The traversal itself stays under the
// lock, which is what the R-tree actually needs protecting for.
func (p *postOffice) search(c *Client, visit func(*Client) bool) searchResult {
	res := searchResult{closestVelocityPeer: math.Inf(1)}
	wantVelocity := !c.IsATC && c.ProtoRevision == ProtoRevisionVelocity
	recipients := make([]*Client, 0, 32)

	p.treeMu.RLock()
	origin := c.LatLon()
	var bounds spatialBounds
	if entry := p.spatial[c]; entry != nil {
		bounds = entry.bounds
	} else {
		bounds = visibilityBounds(origin, c.VisRange())
	}
	for i := range bounds.count {
		box := &bounds.boxes[i]
		p.tree.Search(box.min, box.max, func(min, _ [2]float64, entry *spatialEntry) bool {
			found := entry.client
			if found == c {
				return true
			}
			// Only the first overlapping pair of longitude pieces emits the
			// recipient. This also deduplicates a full-longitude query against
			// a split recipient, without a per-search map.
			if (bounds.count > 1 || entry.bounds.count > 1) && !entry.bounds.firstOverlap(&bounds, i, min) {
				return true
			}
			if wantVelocity && found.ProtoRevision == ProtoRevisionVelocity && !found.IsATC {
				peer := found.LatLon()
				if d := Distance(origin[0], origin[1], peer[0], peer[1]); d < res.closestVelocityPeer {
					res.closestVelocityPeer = d
				}
			}
			recipients = append(recipients, found)
			return true
		})
	}
	p.treeMu.RUnlock()

	for _, found := range recipients {
		if !visit(found) {
			break
		}
	}

	return res
}

// find looks up a client by callsign.
func (p *postOffice) find(callsign string) (*Client, error) {
	p.mu.RLock()
	c, ok := p.clients[callsign]
	p.mu.RUnlock()

	if !ok {
		return nil, ErrCallsignDoesNotExist
	}
	return c, nil
}

// send queues a packet to a single callsign.
func (p *postOffice) send(callsign, packet string) error {
	c, err := p.find(callsign)
	if err != nil {
		return err
	}
	return c.Send(packet)
}

// all visits every registered client except the sender. Pass nil to visit
// everyone.
func (p *postOffice) all(sender *Client, visit func(*Client) bool) {
	// Snapshot under the lock, then visit outside it. Holding the map lock
	// across an arbitrary callback risks a deadlock the moment that callback
	// wants to look another client up.
	p.mu.RLock()
	snapshot := make([]*Client, 0, len(p.clients))
	for _, c := range p.clients {
		if c != sender {
			snapshot = append(snapshot, c)
		}
	}
	p.mu.RUnlock()

	for _, c := range snapshot {
		if !visit(c) {
			return
		}
	}
}

func (p *postOffice) snapshot() []*Client {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]*Client, 0, len(p.clients))
	for _, c := range p.clients {
		out = append(out, c)
	}
	return out
}

// count returns the number of registered clients.
func (p *postOffice) count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

const (
	earthRadiusMetres = 6371000.0
	degToRad          = math.Pi / 180
	metresPerDegLat   = (math.Pi * earthRadiusMetres) / 180
)

// visibilityBounds returns canonical longitude pieces covering the visibility
// box. A pole-reaching box covers all longitudes. +180 is indexed as -180;
// a piece ending at +180 also includes its equivalent -180 endpoint.
func visibilityBounds(centre [2]float64, radius float64) spatialBounds {
	deltaLat := radius / metresPerDegLat
	minLat := math.Max(-90, centre[0]-deltaLat)
	maxLat := math.Min(90, centre[0]+deltaLat)
	bounds := spatialBounds{count: 1}
	bounds.boxes[0] = spatialBox{
		min: [2]float64{minLat, -180},
		max: [2]float64{maxLat, 180},
	}
	if minLat == -90 || maxLat == 90 {
		return bounds
	}

	// The spherical cap's longitude extent exceeds the local cosine
	// approximation near a pole, even when the cap does not reach it.
	sinLon := math.Sin(radius/earthRadiusMetres) / math.Cos(centre[0]*degToRad)
	deltaLon := math.Asin(math.Min(1, sinLon)) / degToRad
	longitude := centre[1]
	if longitude < -180 || longitude >= 180 {
		longitude = math.Mod(longitude+180, 360)
		if longitude < 0 {
			longitude += 360
		}
		longitude -= 180
	}
	minLon, maxLon := longitude-deltaLon, longitude+deltaLon
	switch {
	case minLon < -180:
		bounds.boxes[0].max[1] = maxLon
		bounds.boxes[1] = spatialBox{min: [2]float64{minLat, minLon + 360}, max: [2]float64{maxLat, 180}}
		bounds.count = 2
	case maxLon >= 180:
		bounds.boxes[0].max[1] = maxLon - 360
		bounds.boxes[1] = spatialBox{min: [2]float64{minLat, minLon}, max: [2]float64{maxLat, 180}}
		bounds.count = 2
	default:
		bounds.boxes[0].min[1] = minLon
		bounds.boxes[0].max[1] = maxLon
	}
	return bounds
}

// firstOverlap selects one overlapping pair in query-piece, entry-piece order.
// Each entry has at most two disjoint pieces, so this takes at most four
// comparisons regardless of the number of clients. Ordinary unsplit searches
// do not call it.
func (bounds *spatialBounds) firstOverlap(query *spatialBounds, queryIndex int, entryMin [2]float64) bool {
	for i := range query.count {
		q := &query.boxes[i]
		for j := range bounds.count {
			box := &bounds.boxes[j]
			if q.min[0] <= box.max[0] && q.max[0] >= box.min[0] &&
				q.min[1] <= box.max[1] && q.max[1] >= box.min[1] {
				return i == queryIndex && box.min == entryMin
			}
		}
	}
	return false
}

// Distance returns the great-circle distance in metres between two points.
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := (lat2 - lat1) * degToRad
	dLon := (lon2 - lon1) * degToRad

	sinLat := math.Sin(dLat * 0.5)
	sinLon := math.Sin(dLon * 0.5)

	a := sinLat*sinLat + math.Cos(lat1*degToRad)*math.Cos(lat2*degToRad)*sinLon*sinLon
	return earthRadiusMetres * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
