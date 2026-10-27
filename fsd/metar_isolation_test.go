package fsd

import (
	"context"
	"errors"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type metarTransport func(*http.Request) (*http.Response, error)

func (f metarTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func metarResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}
}

func metarTestService(t *testing.T, transport metarTransport, workers int) *metarService {
	t.Helper()
	cfg := config.Defaults()
	cfg.Weather = "https://weather.test/?icao="
	cfg.WeatherFallback = ""
	m := newMetarService(config.NewStore("config.json", &cfg), workers)
	m.client.Transport = transport
	t.Cleanup(m.shutdown)
	return m
}

func runMetarTestService(t *testing.T, m *metarService) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.run(ctx); close(done) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("METAR workers did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func metarTestClient(t *testing.T, callsign string) *Client {
	t.Helper()
	c := newClient(context.Background(), nil, nil, LoginData{Callsign: callsign})
	t.Cleanup(c.cancel)
	return c
}

func receiveMetarPacket(t *testing.T, c *Client) string {
	t.Helper()
	select {
	case packet := <-c.sendCh:
		return packet
	case <-time.After(time.Second):
		t.Fatal("METAR client received no reply")
		return ""
	}
}

func waitMetarState(t *testing.T, m *metarService, pending, queued, lookups int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		gotPending, gotQueued, gotLookups := len(m.pending), len(m.queue), len(m.lookups)
		m.mu.Unlock()
		if gotPending == pending && gotQueued == queued && gotLookups == lookups {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("METAR state = %d pending, %d queued, %d lookups; want %d, %d, %d",
				gotPending, gotQueued, gotLookups, pending, queued, lookups)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMetarFloodCannotMonopolizeWorkers(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var floodLookups atomic.Int32
	m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
		icao := req.URL.Query().Get("icao")
		if strings.HasPrefix(icao, "AAA") {
			floodLookups.Add(1)
			started <- struct{}{}
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return metarResponse(req, http.StatusOK, icao+" 111200Z CAVOK"), nil
	}, 4)
	runMetarTestService(t, m)
	flood := metarTestClient(t, "FLOOD")
	m.request(flood, "AAAA")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first METAR request did not start")
	}
	var requests sync.WaitGroup
	for _, icao := range []string{"AAAB", "AAAC", "AAAD", "AAAE", "AAAF", "AAAG", "AAAH"} {
		requests.Add(1)
		go func() { defer requests.Done(); m.request(flood, icao) }()
	}
	requests.Wait()
	for range 7 {
		if packet := receiveMetarPacket(t, flood); !strings.Contains(packet, ":9::Weather service busy") {
			t.Fatalf("excess request reply = %q", packet)
		}
	}
	healthy := metarTestClient(t, "HEALTHY")
	m.request(healthy, "KJFK")
	if packet := receiveMetarPacket(t, healthy); packet != "$ARserver:HEALTHY:METAR:KJFK 111200Z CAVOK\r\n" {
		t.Fatalf("healthy-client reply = %q", packet)
	}
	if got := floodLookups.Load(); got != 1 {
		t.Fatalf("flooding client occupied %d workers, want one", got)
	}
	waitMetarState(t, m, 1, 0, 1)
	close(release)
	if packet := receiveMetarPacket(t, flood); !strings.HasPrefix(packet, "$ARserver:FLOOD:METAR:AAAA ") {
		t.Fatalf("original request reply = %q", packet)
	}
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarSameICAOSharesLookupAndSurvivesOneDisconnect(t *testing.T) {
	started := make(chan *http.Request, 1)
	release := make(chan struct{})
	var hits atomic.Int32
	m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
		hits.Add(1)
		started <- req
		select {
		case <-release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		return metarResponse(req, http.StatusOK, "KJFK 111200Z CAVOK"), nil
	}, 4)
	runMetarTestService(t, m)
	first := metarTestClient(t, "FIRST")
	second := metarTestClient(t, "SECOND")
	m.request(first, "KJFK")
	var upstream *http.Request
	select {
	case upstream = <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream request did not start")
	}
	m.request(second, " kjfk ")
	waitMetarState(t, m, 2, 0, 1)
	first.cancel()
	waitMetarState(t, m, 1, 0, 1)
	if err := upstream.Context().Err(); err != nil {
		t.Fatalf("one disconnect cancelled the shared lookup: %v", err)
	}
	close(release)
	if packet := receiveMetarPacket(t, second); packet != "$ARserver:SECOND:METAR:KJFK 111200Z CAVOK\r\n" {
		t.Fatalf("remaining client's reply = %q", packet)
	}
	select {
	case packet := <-first.sendCh:
		t.Fatalf("disconnected client received %q", packet)
	default:
	}
	third := metarTestClient(t, "THIRD")
	m.request(third, "KJFK")
	receiveMetarPacket(t, third)
	if got := hits.Load(); got != 1 {
		t.Fatalf("duplicate/cache requests made %d upstream lookups, want one", got)
	}
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarLastDisconnectCancelsUpstreamWithoutCachingAMiss(t *testing.T) {
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{})
	var hits atomic.Int32
	m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
		if hits.Add(1) == 1 {
			started <- struct{}{}
			<-req.Context().Done()
			close(cancelled)
			return nil, req.Context().Err()
		}
		return metarResponse(req, http.StatusOK, "KJFK 111200Z CAVOK"), nil
	}, 1)
	runMetarTestService(t, m)
	first := metarTestClient(t, "FIRST")
	m.request(first, "KJFK")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream request did not start")
	}
	first.cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("last disconnect retained the active upstream request")
	}
	waitMetarState(t, m, 0, 0, 0)
	second := metarTestClient(t, "SECOND")
	m.request(second, "KJFK")
	if packet := receiveMetarPacket(t, second); !strings.HasPrefix(packet, "$ARserver:SECOND:METAR:KJFK ") {
		t.Fatalf("cancelled request poisoned the cache: %q", packet)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("upstream hits = %d, want cancellation followed by a fresh lookup", got)
	}
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarQueueBoundsAndDisconnectedClientReleasesCapacity(t *testing.T) {
	m := metarTestService(t, nil, 4)
	clients := make([]*Client, metarQueueLimit)
	icaos := make([]string, metarQueueLimit)
	for i := range clients {
		clients[i] = metarTestClient(t, fmt.Sprintf("PILOT%03d", i))
		icaos[i] = string([]byte{'K', 'A' + byte(i/26), 'A' + byte(i%26), 'A'})
		m.request(clients[i], icaos[i])
	}
	waitMetarState(t, m, metarQueueLimit, metarQueueLimit, metarQueueLimit)
	m.request(clients[0], "ZBAA")
	if packet := receiveMetarPacket(t, clients[0]); !strings.Contains(packet, ":9::Weather service busy") {
		t.Fatalf("client queued multiple requests: %q", packet)
	}
	duplicate := metarTestClient(t, "DUPLICATE")
	m.request(duplicate, icaos[0])
	waitMetarState(t, m, metarQueueLimit+1, metarQueueLimit, metarQueueLimit)
	overflow := metarTestClient(t, "OVERFLOW")
	m.request(overflow, "ZBAA")
	if packet := receiveMetarPacket(t, overflow); !strings.Contains(packet, ":9::Weather service busy") {
		t.Fatalf("full queue reply = %q", packet)
	}
	clients[1].cancel()
	waitMetarState(t, m, metarQueueLimit, metarQueueLimit-1, metarQueueLimit-1)
	m.request(overflow, "ZBAA")
	waitMetarState(t, m, metarQueueLimit+1, metarQueueLimit, metarQueueLimit)
	m.shutdown()
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarCompletionFailureAndTimeoutReleaseAdmission(t *testing.T) {
	for _, outcome := range []string{"success", "not-found", "transport-error", "timeout", "fallback"} {
		t.Run(outcome, func(t *testing.T) {
			var hits atomic.Int32
			m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
				hits.Add(1)
				icao := req.URL.Query().Get("icao")
				switch outcome {
				case "not-found":
					return metarResponse(req, http.StatusNotFound, "missing"), nil
				case "transport-error":
					return nil, errors.New("weather provider unavailable")
				case "timeout":
					<-req.Context().Done()
					return nil, req.Context().Err()
				case "fallback":
					if req.URL.Host == "weather.test" {
						return metarResponse(req, http.StatusNotFound, "missing"), nil
					}
				}
				return metarResponse(req, http.StatusOK, icao+" 111200Z CAVOK"), nil
			}, 4)
			if outcome == "timeout" {
				m.client.Timeout = 20 * time.Millisecond
			}
			perLookup := int32(1)
			if outcome == "fallback" {
				m.config.Get().WeatherFallback = "https://fallback.test/?icao="
				perLookup = 2
			}
			runMetarTestService(t, m)
			client := metarTestClient(t, "PILOT")
			for _, icao := range []string{"KJFK", "KJFK", "KSEA"} {
				m.request(client, icao)
				packet := receiveMetarPacket(t, client)
				if outcome == "success" || outcome == "fallback" {
					if packet != "$ARserver:PILOT:METAR:"+icao+" 111200Z CAVOK\r\n" {
						t.Fatalf("METAR success reply = %q", packet)
					}
				} else if packet != "$ERserver:PILOT:9::No METAR available for "+icao+"\r\n" {
					t.Fatalf("METAR failure reply = %q", packet)
				}
				waitMetarState(t, m, 0, 0, 0)
			}
			if got := hits.Load(); got != 2*perLookup {
				t.Fatalf("upstream hits = %d, want %d with cached repeat", got, 2*perLookup)
			}
		})
	}
}

func TestMetarShutdownReleasesActiveAndQueuedRequests(t *testing.T) {
	started := make(chan struct{}, 4)
	m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	}, 4)
	stop := runMetarTestService(t, m)
	for i, icao := range []string{"KJFK", "KSEA", "ZBAA", "RJTT", "KLAX", "EGLL", "LFPG"} {
		m.request(metarTestClient(t, fmt.Sprintf("PILOT%d", i)), icao)
	}
	for range 4 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("four independent requests did not reach four workers")
		}
	}
	waitMetarState(t, m, 7, 3, 7)
	stop()
	waitMetarState(t, m, 0, 0, 0)
	client := metarTestClient(t, "LATE")
	m.request(client, "KJFK")
	if packet := receiveMetarPacket(t, client); !strings.Contains(packet, ":9::Weather service busy") {
		t.Fatalf("request after shutdown = %q", packet)
	}
}

func TestMetarCachedRequestsRetainCallsignRateLimit(t *testing.T) {
	m := metarTestService(t, nil, 4)
	m.store("KJFK", "KJFK 111200Z CAVOK", true)
	client := metarTestClient(t, "PILOT")
	for range metarPerClientLimit {
		m.request(client, "KJFK")
		if packet := receiveMetarPacket(t, client); !strings.HasPrefix(packet, "$ARserver:PILOT:METAR:") {
			t.Fatalf("cached METAR reply = %q", packet)
		}
	}
	reconnected := metarTestClient(t, "PILOT")
	m.request(reconnected, "KJFK")
	if packet := receiveMetarPacket(t, reconnected); !strings.Contains(packet, ":9::Weather service rate limit exceeded") {
		t.Fatalf("reconnect reset callsign rate limit: %q", packet)
	}
	healthy := metarTestClient(t, "OTHER")
	m.request(healthy, "KJFK")
	receiveMetarPacket(t, healthy)
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarExcessRequestsNeverWaitForAFullOutboundBuffer(t *testing.T) {
	m := metarTestService(t, nil, 4)
	client := metarTestClient(t, "STALLED")
	m.request(client, "KJFK")
	for range cap(client.sendCh) {
		client.sendCh <- "full"
	}
	done := make(chan struct{})
	go func() { m.request(client, "KSEA"); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("excess METAR request blocked the packet loop")
	}
	if client.Context().Err() == nil {
		t.Fatal("stalled outbound client remained connected")
	}
	waitMetarState(t, m, 0, 0, 0)
}

func TestMetarRequestProtocolAndProviderParsing(t *testing.T) {
	var hits atomic.Int32
	m := metarTestService(t, func(req *http.Request) (*http.Response, error) {
		hits.Add(1)
		if got := req.URL.Query().Get("icao"); got != "KJFK" {
			t.Errorf("requested ICAO = %q", got)
		}
		if req.Header.Get("Accept") != "text/plain" || !strings.Contains(req.Header.Get("User-Agent"), "can-fsd") {
			t.Errorf("weather request headers = %v", req.Header)
		}
		return metarResponse(req, http.StatusOK, "2026/10/11 12:00\nKJFK 111200Z NOTE:A CAVOK\n"), nil
	}, 4)
	runMetarTestService(t, m)
	s := &Server{metar: m}
	client := metarTestClient(t, "PILOT")
	for _, packet := range []string{"$AXPILOT:OTHER:METAR:KJFK", "$AXPILOT:SERVER:OTHER:KJFK"} {
		s.handleMetarRequest(client, []byte(packet))
	}
	select {
	case packet := <-client.sendCh:
		t.Fatalf("unrelated weather request produced %q", packet)
	default:
	}
	s.handleMetarRequest(client, []byte("$AXPILOT:SERVER:METAR:K1FK"))
	if packet := receiveMetarPacket(t, client); packet != "$ERserver:PILOT:9::Invalid ICAO code\r\n" {
		t.Fatalf("invalid ICAO reply = %q", packet)
	}
	s.handleMetarRequest(client, []byte("$AXPILOT:SERVER:METAR: kjfk "))
	if packet := receiveMetarPacket(t, client); packet != "$ARserver:PILOT:METAR:KJFK 111200Z NOTE A CAVOK\r\n" {
		t.Fatalf("METAR protocol reply = %q", packet)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("weather fetch count = %d, want one valid lookup", got)
	}
}

func TestDefaultWeatherConfigurationMakesNoExternalRequest(t *testing.T) {
	cfg := config.Defaults()
	m := newMetarService(config.NewStore("unused", &cfg), 1)
	m.client.Transport = metarTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("default configuration attempted an external weather request")
		return nil, errors.New("unexpected weather request")
	})
	if report, ok := m.lookup(context.Background(), "AAAA"); ok || report != "" {
		t.Fatalf("unconfigured weather returned (%q, %t)", report, ok)
	}
}
