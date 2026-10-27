package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func configFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOfflineDefaultsAndRelativeCredentials(t *testing.T) {
	path := configFile(t, `{"users_file":"users.json"}`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Weather != "" || c.WeatherFallback != "" || c.FacilityEnforcement != EnforcementStrict || c.ListenAddrs[0] != "127.0.0.1:6809" {
		t.Fatalf("unsafe defaults: %+v", c)
	}
	if c.CredentialsFile != filepath.Join(filepath.Dir(path), "users.json") {
		t.Fatal(c.CredentialsFile)
	}
	if len(c.ClientAuthKeys) != 0 {
		t.Fatal("embedded client keys")
	}
}

func TestRejectInvalidConfig(t *testing.T) {
	for _, text := range []string{
		`{"database_dsn":"forbidden"}`, `{"region_prefixes":{}}`, `{"max_clients":"2"}`,
		`{"fsd_mode":2}`, `{"max_clients":0}`, `{"auth_concurrency":0}`,
		`{"listen_addrs":["127.0.0.1:0"]}`, `{"listen_addrs":[]}`, `{"listen_addrs":["localhost"]}`,
		`{"server_ident":"bad:V3.4"}`, `{"server_ident":"no-version"}`, `{"server_ident":"FSD V3.4\nbad"}`,
		`{"server_ident_sweatbox":"FSD:V3.4"}`, `{"server_challenge_key":"bad\rseed"}`, `{"server_challenge_key_sweatbox":"bad:seed"}`,
		`{"welcome":"hello\r#AAinjection"}`, `{"welcome":"trailing\r"}`,
		`{"tls_listen_addrs":["127.0.0.1:6810"]}`, `{"plaintext_policy":"off"}`,
		`{"facility_enforcement":"anything"}`, `{"client_auth_keys":{"1":"bad:key"}}`, `{"weather":"file:///secret"}`,
		`null`, `{} {}`, `{"max_clients":null}`,
	} {
		t.Run(text, func(t *testing.T) {
			if _, err := Load(configFile(t, text)); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}

func TestWelcomePreservesURLsTimesAndMultilineText(t *testing.T) {
	for _, welcome := range []string{
		"Visit https://example.org:8443/help at 12:00",
		"Welcome\nVisit https://example.org/help\r\n#AAstill-message-text",
		strings.Repeat("x", 4096),
	} {
		data, err := json.Marshal(map[string]string{"welcome": welcome})
		if err != nil {
			t.Fatal(err)
		}
		c, err := Load(configFile(t, string(data)))
		if err != nil {
			t.Fatalf("valid welcome rejected: %v", err)
		}
		if c.Welcome != welcome {
			t.Fatal("welcome value changed")
		}
	}
}

func TestWelcomeRejectsUnsafeControlsAndOversizeText(t *testing.T) {
	for _, welcome := range []string{"hello\x00world", "hello\tworld", "hello\x1bworld", "hello\x7fworld", strings.Repeat("x", 4097)} {
		data, err := json.Marshal(map[string]string{"welcome": welcome})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Load(configFile(t, string(data))); err == nil {
			t.Fatal("unsafe welcome accepted")
		}
	}
}

func TestMissingConfigDoesNotCreateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")
	if _, err := Load(path); err == nil {
		t.Fatal("missing config accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("Load created a file")
	}
}

func TestReloadRejectsStartupChangesAtomically(t *testing.T) {
	path := configFile(t, `{"users_file":"users.json"}`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(path, c)
	for _, change := range []string{`"fsd_mode":1`, `"users_file":"other.json"`, `"listen_addrs":["127.0.0.1:6900"]`, `"max_clients":601`, `"wallops_per_server_minute":31`, `"server_ident":"Other FSD V3.41"`, `"server_challenge_key":"other-seed"`, `"tls_cert_file":"other.pem","tls_key_file":"other.key"`} {
		text := `{"users_file":"users.json","welcome":"must not apply","max_pre_auth_per_ip":9,` + change + `}`
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err != nil {
			t.Fatalf("startup-only test configuration is invalid: %v", err)
		}
		if _, err := s.Reload(); err == nil {
			t.Fatalf("accepted startup change: %s", change)
		}
		if s.Get() != c {
			t.Fatal("failed reload replaced config")
		}
	}
}

func TestReloadAppliesLiveLimitsAndWelcomeAtomically(t *testing.T) {
	path := configFile(t, `{}`)
	old, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	before := *old
	s := NewStore(path, old)
	text := `{
		"welcome":"Updated at 12:00\nhttps://example.org",
		"max_pre_auth_connections":64,"max_pre_auth_per_ip":4,
		"auth_concurrency":2,"auth_attempts_per_minute":30,
		"auth_attempt_burst":6,"auth_ip_state_limit":2048,
		"login_timeout_seconds":5,"ident_delay_ms":500,
		"inbound_packets_per_second":30,"inbound_bytes_per_second":65536,
		"wallops_per_client_minute":2,
		"weather":"https://example.org/metar","weather_fallback":"https://example.org/fallback",
		"facility_enforcement":"warn","client_auth_keys":{"1":"local-key"}
	}`
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := s.Reload()
	if err != nil {
		t.Fatal(err)
	}
	want := *old
	want.Welcome = "Updated at 12:00\nhttps://example.org"
	want.MaxPreAuthConnections = 64
	want.MaxPreAuthPerIP = 4
	want.AuthConcurrency = 2
	want.AuthAttemptsPerMinute = 30
	want.AuthAttemptBurst = 6
	want.AuthIPStateLimit = 2048
	want.LoginTimeoutSeconds = 5
	want.IdentDelayMS = 500
	want.InboundPacketsPerSecond = 30
	want.InboundBytesPerSecond = 65536
	want.WallopsPerClientMinute = 2
	want.Weather = "https://example.org/metar"
	want.WeatherFallback = "https://example.org/fallback"
	want.FacilityEnforcement = EnforcementWarn
	want.ClientAuthKeys = map[uint16]string{1: "local-key"}
	if !reflect.DeepEqual(next, &want) || s.Get() != next {
		t.Fatal("reload did not publish the complete update")
	}
	if !reflect.DeepEqual(old, &before) {
		t.Fatal("reload mutated the previous snapshot")
	}
	for _, invalid := range []string{
		`{"welcome":"must not apply","max_pre_auth_per_ip":0}`,
		`{"welcome":"must not apply","max_pre_auth_per_ip":9,"auth_concurrency":9}`,
	} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Reload(); err == nil {
			t.Fatal("invalid mixed update accepted")
		}
		if s.Get() != next || !reflect.DeepEqual(s.Get(), &want) {
			t.Fatal("invalid update changed the active snapshot")
		}
	}
}
