package fsd

import (
	"testing"
	"time"

	"github.com/JianyueLab-Org/can-fsdopen/config"
)

// A regional-looking callsign carries no separate membership requirement.
func TestNormalModeControllerNeedsNoRegionalRoster(t *testing.T) {
	for _, callsign := range []string{"ZBAA_TWR", "RJTT_TWR", "VHHH_TWR", "LAX_TWR"} {
		t.Run(callsign, func(t *testing.T) {
			h := newHarness(t, harnessOptions{liveMode: true, enforcement: config.EnforcementStrict})
			c := h.loginATC(t, callsign, "1001", 5)
			c.send("%" + callsign + ":18000:4:100:5:35:120:0")
			c.send("$PI" + callsign + ":SERVER:position-accepted")
			c.expect("$POSERVER:" + callsign + ":position-accepted")
			client, err := h.server.postOffice.find(callsign)
			if err != nil || !client.HasPosition() || client.FacilityType() != FacilityTower {
				t.Fatal("qualified controller position was not admitted")
			}
		})
	}
}

func TestReconnectPlanIdentityBoundary(t *testing.T) {
	h := newHarness(t, harnessOptions{users: map[string]*testIdentity{
		"1000": {Password: "secret", Rating: 1},
		"1002": {Password: "secret", Rating: 1},
	}})
	pilot := h.loginPilot(t, "TEST101", "1000", 100)
	pilot.send("$FPTEST101:*A:I:B738:430:AAAA:1200:1200:35000:BBBB:2:0:3:0:CCCC:synthetic:FIX1 FIX2")
	// A ping follows filing on the same ordered connection.
	pilot.send("$PITEST101:SERVER:filed")
	pilot.expect("$POSERVER:TEST101:filed")
	pilot.conn.Close()
	h.waitForOnline(t, 0)
	same := h.loginPilot(t, "TEST101", "1000", 100)
	same.send("$PITEST101:SERVER:restored")
	same.expect("$POSERVER:TEST101:restored")
	current, _ := h.server.postOffice.find("TEST101")
	if current.FlightPlanData() == nil {
		t.Fatal("same identity lost reconnect plan")
	}
	same.conn.Close()
	h.waitForOnline(t, 0)
	other := h.loginPilot(t, "TEST101", "1002", 100)
	other.send("$PITEST101:SERVER:separate")
	other.expect("$POSERVER:TEST101:separate")
	current, _ = h.server.postOffice.find("TEST101")
	if current.FlightPlanData() != nil {
		t.Fatal("different identity inherited reconnect plan")
	}
	if fp := h.server.lookupFlightPlan("TEST101"); fp != nil {
		t.Fatal("offline plan leaked through live callsign query")
	}
}

func TestClientChallengeKeyIsOptional(t *testing.T) {
	var state AuthState
	if err := state.Initialize(40000, "", []byte("seed")); err != ErrUnsupportedAuthClient {
		t.Fatalf("empty key error = %v", err)
	}
	if state.Initialized() {
		t.Fatal("unsupported client initialized challenge state")
	}
	h := newHarness(t, harnessOptions{})
	c := h.dial(t)
	c.send("$IDTEST101:SERVER:abcd:Synthetic:1:0:1000:0:seed")
	c.send("#APTEST101:SERVER:1000:secret:1:100:0:Synthetic Pilot")
	c.expect("#TMSERVER")
	c.send("$ZCTEST101:SERVER:hello")
	c.expectNone("$ZR", 100*time.Millisecond)
}

func TestSyntheticChallengeStateVectors(t *testing.T) {
	vectors := []struct {
		id            uint16
		first, second string
	}{
		{40000, "cf804a0e604972b22b9ae8d9a4dbce55", "47011d236b35447125ea17b8248ab154"},
		{40001, "2956ca872f6346431561a6caba160dc5", "21a180df20cf63fcaf83f7b07c25fdbb"},
		{40002, "3b5f77d105fb7ff99380c6f1ab8449fc", "b8a3b925ffb61228d8f2b8cd5d08ac37"},
		{40003, "c57989f36cad64f4bfc20bb5ec26b5bb", "19a6041caad26ee6da1666a01e43fe2c"},
		{40004, "8541044b0101cd1d6aff7acdd75dc32d", "0be265ed529d10eaa297b134cf575ead"},
		{40005, "19060fd71f65075237a18ee2817ff0cb", "58ad1ed8cb7b4494e09f61c322a43d03"},
	}
	for _, vector := range vectors {
		var state AuthState
		if err := state.Initialize(vector.id, "0123456789abcdef0123456789abcdef", []byte("synthetic-seed")); err != nil {
			t.Fatal(err)
		}
		first := state.Response([]byte("challenge-one"))
		if string(first[:]) != vector.first {
			t.Fatalf("client %d first response = %s, want %s", vector.id, first, vector.first)
		}
		state.Advance(&first)
		second := state.Response([]byte("challenge-two"))
		if string(second[:]) != vector.second {
			t.Fatalf("client %d second response = %s, want %s", vector.id, second, vector.second)
		}
	}
}

func TestReconnectPlanGraceIsBoundedAndExpires(t *testing.T) {
	cfg := config.Defaults()
	cfg.MaxClients = 2
	s, err := NewServer(Options{Config: config.NewStore("unused", &cfg), Users: &fakeAuth{}})
	if err != nil {
		t.Fatal(err)
	}
	s.holdOfflinePlan("TEST1", "synthetic-a", &FlightPlan{Route: "FIX1"})
	oldest := s.offlinePlans["TEST1"]
	oldest.OfflineAt = time.Now().Add(-2 * flightPlanGrace).UnixNano()
	s.holdOfflinePlan("TEST2", "synthetic-a", &FlightPlan{Route: "FIX2"})
	s.holdOfflinePlan("TEST3", "synthetic-a", &FlightPlan{Route: "FIX3"})
	if len(s.offlinePlans) != 2 {
		t.Fatalf("offline plans = %d, want 2", len(s.offlinePlans))
	}
	if _, ok := s.offlinePlans["TEST1"]; ok {
		t.Fatal("oldest offline plan was not evicted")
	}
	expired := s.offlinePlans["TEST2"]
	expired.OfflineAt = time.Now().Add(-2 * flightPlanGrace).UnixNano()
	if fp := s.takeOfflinePlan("TEST2", "synthetic-a"); fp != nil {
		t.Fatal("expired reconnect plan was restored")
	}
	if fp := s.takeOfflinePlan("TEST3", "synthetic-b"); fp != nil {
		t.Fatal("another identity received the held plan")
	}
	if len(s.offlinePlans) != 0 {
		t.Fatal("expired or claimed plan retained")
	}
}

func TestChallengeInitializationFailureClearsOldState(t *testing.T) {
	var state AuthState
	key := "0123456789abcdef0123456789abcdef"
	if err := state.Initialize(40000, key, []byte("synthetic")); err != nil {
		t.Fatal(err)
	}
	if err := state.Initialize(40000, key, nil); err == nil {
		t.Fatal("empty initial challenge accepted")
	}
	if state.Initialized() {
		t.Fatal("failed initialization retained old state")
	}
}
