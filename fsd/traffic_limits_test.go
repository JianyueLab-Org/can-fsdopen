package fsd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/JianyueLab-Org/can-fsdopen/config"
	"strings"
	"testing"
	"time"
)

func TestInboundFloodDisconnectsBeforeBroadcastingEverything(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message string
		count   int
		config  string
		want    int
	}{
		{"packet flood", "position", 100, "{}", 60},
		{"byte flood", strings.Repeat("x", 15000), 20, "{}", 8},
		{"configured packet limit", "position", 100, `{"inbound_packets_per_second":2}`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Defaults()
			if err := json.Unmarshal([]byte(tc.config), &cfg); err != nil {
				t.Fatal(err)
			}
			s := &Server{postOffice: newPostOffice(16), config: config.NewStore("", &cfg)}
			sender := newIdleClient(t, "CCA101")
			peer := newIdleClient(t, "CCA102")
			peer.sendCh = make(chan string, 256)
			for _, c := range []*Client{sender, peer} {
				if err := s.postOffice.register(c); err != nil {
					t.Fatal(err)
				}
			}
			packet := "#TMCCA101:@22800:" + tc.message + "\r\n"
			sender.scanner = NewScanner(bufio.NewReader(strings.NewReader(strings.Repeat(packet, tc.count))))
			s.eventLoop(sender)
			if sender.DisconnectReason() != "inbound rate limit exceeded" {
				t.Fatalf("disconnect reason = %q, forwarded %d packets", sender.DisconnectReason(), len(peer.sendCh))
			}
			if len(peer.sendCh) != tc.want {
				t.Fatalf("forwarded %d packets, want %d", len(peer.sendCh), tc.want)
			}
		})
	}
}

func TestWallopServerBudgetBoundsMultipleClients(t *testing.T) {
	cfg := config.Defaults()
	cfg.WallopsPerServerMinute = 5
	s := &Server{postOffice: newPostOffice(16), config: config.NewStore("", &cfg)}
	supervisor := newIdleClient(t, "TEST_SUP")
	supervisor.NetworkRating = RatingSupervisor
	if err := s.postOffice.register(supervisor); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		client := newIdleClient(t, fmt.Sprintf("CCA10%d", i))
		for j := 0; j < 3; j++ {
			s.handleWallop(client, []byte(fmt.Sprintf("#TM%s:*S:Need assistance\r\n", client.Callsign)))
		}
	}
	if got := len(supervisor.sendCh); got != 5 {
		t.Fatalf("relayed %d wallops, want configured server limit 5", got)
	}
}

func TestWallopSpellingsSharePerClientLimit(t *testing.T) {
	cfg := config.Defaults()
	s := &Server{postOffice: newPostOffice(16), config: config.NewStore("", &cfg)}
	supervisor := newIdleClient(t, "TEST_SUP")
	supervisor.NetworkRating = RatingSupervisor
	if err := s.postOffice.register(supervisor); err != nil {
		t.Fatal(err)
	}
	client := newIdleClient(t, "CCA101")
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			s.handleWallop(client, []byte("#TMCCA101:*S:Need assistance\r\n"))
		} else {
			s.handleServerMessage(client, []byte("#TMCCA101:SERVER:.wallop Need assistance\r\n"))
		}
	}
	if got := len(supervisor.sendCh); got != 3 {
		t.Fatalf("relayed %d wallops, want 3 across both spellings", got)
	}
}

func TestBroadcastDoesNotWaitForStalledPeer(t *testing.T) {
	s := &Server{postOffice: newPostOffice(16)}
	sender := newIdleClient(t, "CCA101")
	peer := newIdleClient(t, "CCA102")
	for _, c := range []*Client{sender, peer} {
		if err := s.postOffice.register(c); err != nil {
			t.Fatal(err)
		}
	}
	for len(peer.sendCh) < cap(peer.sendCh) {
		peer.sendCh <- "filler"
	}
	started := time.Now()
	s.broadcastRanged(sender, "#TMCCA101:@22800:hello\r\n")
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("broadcast waited %v for a stalled peer", elapsed)
	}
	if peer.DisconnectReason() != "outbound buffer full: client not reading" {
		t.Fatalf("stalled peer disconnect = %q", peer.DisconnectReason())
	}
}
