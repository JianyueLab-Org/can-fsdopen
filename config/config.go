// Package config loads the standalone server's core settings.
package config

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type FacilityEnforcement string

const (
	EnforcementOff    FacilityEnforcement = "off"
	EnforcementWarn   FacilityEnforcement = "warn"
	EnforcementStrict FacilityEnforcement = "strict"
)

type Config struct {
	ServerIdent         string `json:"server_ident"`
	ServerIdentSweatbox string `json:"server_ident_sweatbox"`
	IdentDelayMS        int    `json:"ident_delay_ms"`
	// Server challenge seeds are public $DI fields, not client authentication keys.
	ServerChallengeKey         string              `json:"server_challenge_key"`
	ServerChallengeKeySweatbox string              `json:"server_challenge_key_sweatbox"`
	ClientAuthKeys             map[uint16]string   `json:"client_auth_keys"`
	Welcome                    string              `json:"welcome"`
	Weather                    string              `json:"weather"`
	WeatherFallback            string              `json:"weather_fallback"`
	FSDMode                    int                 `json:"fsd_mode"`
	ListenAddrs                []string            `json:"listen_addrs"`
	SweatboxListenAddrs        []string            `json:"sweatbox_listen_addrs"`
	TLSListenAddrs             []string            `json:"tls_listen_addrs"`
	SweatboxTLSListenAddrs     []string            `json:"sweatbox_tls_listen_addrs"`
	TLSCertFile                string              `json:"tls_cert_file"`
	TLSKeyFile                 string              `json:"tls_key_file"`
	ATCPlaintextPolicy         string              `json:"plaintext_policy"`
	MaxClients                 int                 `json:"max_clients"`
	MaxPreAuthConnections      int                 `json:"max_pre_auth_connections"`
	MaxPreAuthPerIP            int                 `json:"max_pre_auth_per_ip"`
	AuthConcurrency            int                 `json:"auth_concurrency"`
	LoginTimeoutSeconds        int                 `json:"login_timeout_seconds"`
	AuthAttemptsPerMinute      int                 `json:"auth_attempts_per_minute"`
	AuthAttemptBurst           int                 `json:"auth_attempt_burst"`
	AuthIPStateLimit           int                 `json:"auth_ip_state_limit"`
	InboundPacketsPerSecond    int                 `json:"inbound_packets_per_second"`
	InboundBytesPerSecond      int                 `json:"inbound_bytes_per_second"`
	WallopsPerClientMinute     int                 `json:"wallops_per_client_minute"`
	WallopsPerServerMinute     int                 `json:"wallops_per_server_minute"`
	FacilityEnforcement        FacilityEnforcement `json:"facility_enforcement"`
	CredentialsFile            string              `json:"users_file"`
}

func Defaults() Config {
	return Config{
		ServerIdent: "VATSIM FSD V3.41b", ServerIdentSweatbox: "VATSIM FSD Sweatbox V3.41b", IdentDelayMS: 1500,
		ServerChallengeKey: "765506eff2203f92d0c12f525ee12de2", ServerChallengeKeySweatbox: "4836fae8c870d20a0268a253bef62f26",
		Welcome: "Enjoy your flight!", ListenAddrs: []string{"127.0.0.1:6809"}, SweatboxListenAddrs: []string{"127.0.0.1:6820"},
		TLSListenAddrs: []string{}, SweatboxTLSListenAddrs: []string{}, ClientAuthKeys: map[uint16]string{},
		ATCPlaintextPolicy: "on", MaxClients: 600, MaxPreAuthConnections: 128, MaxPreAuthPerIP: 8, AuthConcurrency: 4, LoginTimeoutSeconds: 10,
		AuthAttemptsPerMinute: 60, AuthAttemptBurst: 12, AuthIPStateLimit: 4096, InboundPacketsPerSecond: 60, InboundBytesPerSecond: 128 * 1024,
		WallopsPerClientMinute: 3, WallopsPerServerMinute: 30, FacilityEnforcement: EnforcementStrict,
	}
}

// Load reads an existing JSON object. Relative file paths use its directory.
// Missing files are errors; Load never creates accounts or configuration.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("config: cannot open configuration; provide an existing JSON file with -config and check permissions")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil {
		return nil, errors.New("config: cannot read configuration file")
	}
	if len(data) > 1<<20 {
		return nil, errors.New("config: file exceeds 1 MiB")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return nil, errors.New("config: expected a JSON object")
	}
	for _, value := range raw {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("config: fields cannot be null")
		}
	}
	cfg := Defaults()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, errors.New("config: invalid configuration fields; check names and JSON types")
	}
	for _, dst := range []*string{&cfg.CredentialsFile, &cfg.TLSCertFile, &cfg.TLSKeyFile} {
		if *dst != "" && !filepath.IsAbs(*dst) {
			*dst = filepath.Join(filepath.Dir(path), *dst)
		}
		if *dst != "" {
			absolute, err := filepath.Abs(*dst)
			if err != nil {
				return nil, err
			}
			*dst = absolute
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.FSDMode != 0 && c.FSDMode != 1 {
		return errors.New("config: fsd_mode must be 0 or 1")
	}
	switch c.FacilityEnforcement {
	case EnforcementOff, EnforcementWarn, EnforcementStrict:
	default:
		return errors.New("config: facility_enforcement must be off, warn or strict")
	}
	if c.ATCPlaintextPolicy != "on" && c.ATCPlaintextPolicy != "off" {
		return errors.New("config: plaintext_policy must be on or off")
	}
	for _, field := range []struct {
		name            string
		value, min, max int
	}{
		{"max_clients", c.MaxClients, 1, 100000}, {"max_pre_auth_connections", c.MaxPreAuthConnections, 1, 100000}, {"max_pre_auth_per_ip", c.MaxPreAuthPerIP, 1, 10000},
		{"auth_concurrency", c.AuthConcurrency, 1, 8}, {"login_timeout_seconds", c.LoginTimeoutSeconds, 1, 30}, {"auth_attempts_per_minute", c.AuthAttemptsPerMinute, 1, 60000},
		{"auth_attempt_burst", c.AuthAttemptBurst, 1, 1000}, {"auth_ip_state_limit", c.AuthIPStateLimit, 1, 100000}, {"inbound_packets_per_second", c.InboundPacketsPerSecond, 1, 100000},
		{"inbound_bytes_per_second", c.InboundBytesPerSecond, 1, 64 << 20}, {"wallops_per_client_minute", c.WallopsPerClientMinute, 1, 60000}, {"wallops_per_server_minute", c.WallopsPerServerMinute, 1, 60000},
		{"ident_delay_ms", c.IdentDelayMS, 0, 30000},
	} {
		if field.value < field.min || field.value > field.max {
			return fmt.Errorf("config: %s must be between %d and %d", field.name, field.min, field.max)
		}
	}
	for _, value := range []string{c.ServerIdent, c.ServerIdentSweatbox, c.ServerChallengeKey, c.ServerChallengeKeySweatbox} {
		if strings.ContainsAny(value, ":\r\n\x00") {
			return errors.New("config: protocol strings must not contain delimiters or control characters")
		}
		for _, r := range value {
			if r < 32 || r == 127 {
				return errors.New("config: protocol strings contain control characters")
			}
		}
		if len(value) > 4096 {
			return errors.New("config: protocol string too long")
		}
	}
	// Welcome is a text-message body, not a delimiter-separated $DI field.
	if len(c.Welcome) > 4096 {
		return errors.New("config: welcome exceeds 4096 bytes")
	}
	for i, r := range c.Welcome {
		if (r < 32 && r != '\r' && r != '\n') || r == 127 ||
			(r == '\r' && (i+1 == len(c.Welcome) || c.Welcome[i+1] != '\n')) {
			return errors.New("config: welcome contains unsafe control characters")
		}
	}
	if !hasVersionNumber(c.ServerIdent) || !hasVersionNumber(c.ServerIdentSweatbox) {
		return errors.New("config: server identifiers need a numeric version")
	}
	if len(c.ClientAuthKeys) > 65536 {
		return errors.New("config: too many client auth keys")
	}
	for _, key := range c.ClientAuthKeys {
		if key == "" || len(key) > 256 || strings.ContainsAny(key, ":\r\n\x00") {
			return errors.New("config: invalid client auth key")
		}
	}
	for _, value := range []string{c.Weather, c.WeatherFallback} {
		if value == "" {
			continue
		}
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || strings.ContainsAny(value, "\r\n") {
			return errors.New("config: weather URLs must be absolute HTTP(S) URLs without credentials or fragments")
		}
	}
	seen := make(map[string]bool)
	for _, addrs := range [][]string{c.ListenAddrs, c.SweatboxListenAddrs, c.TLSListenAddrs, c.SweatboxTLSListenAddrs} {
		for _, addr := range addrs {
			host, port, err := net.SplitHostPort(addr)
			if err != nil || strings.ContainsAny(host, " \r\n\t") {
				return errors.New("config: listener must be host:port")
			}
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return errors.New("config: listener port must be between 1 and 65535")
			}
			if seen[addr] {
				return errors.New("config: duplicate listener address")
			}
			seen[addr] = true
		}
	}
	tlsAddrs := c.AllTLSListenAddrs()
	if c.ATCPlaintextPolicy == "off" && len(tlsAddrs) == 0 {
		return errors.New("config: plaintext off requires TLS listeners")
	}
	if c.ATCPlaintextPolicy == "on" && len(c.AllListenAddrs()) == 0 {
		return errors.New("config: at least one plaintext listener is required")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("config: TLS certificate and key must be configured together")
	}
	if len(c.TLSListenAddrs)+len(c.SweatboxTLSListenAddrs) > 0 {
		if c.TLSCertFile == "" {
			return errors.New("config: TLS listeners require a certificate and key")
		}
		if _, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile); err != nil {
			return errors.New("config: cannot load TLS certificate/key pair")
		}
	}
	return nil
}
func hasVersionNumber(ident string) bool {
	for i := 1; i < len(ident)-1; i++ {
		if ident[i] == '.' && ident[i-1] >= '0' && ident[i-1] <= '9' && ident[i+1] >= '0' && ident[i+1] <= '9' {
			return true
		}
	}
	return false
}
func (c *Config) ServerIdentString() string {
	if c.Sweatbox() {
		return c.ServerIdentSweatbox
	}
	return c.ServerIdent
}
func (c *Config) ServerChallenge() string {
	if c.Sweatbox() {
		return c.ServerChallengeKeySweatbox
	}
	return c.ServerChallengeKey
}
func (c *Config) Sweatbox() bool { return c.FSDMode == 1 }
func (c *Config) AllListenAddrs() []string {
	if !c.Sweatbox() {
		return c.ListenAddrs
	}
	return append(append([]string{}, c.ListenAddrs...), c.SweatboxListenAddrs...)
}
func (c *Config) AllTLSListenAddrs() []string {
	if !c.Sweatbox() {
		return c.TLSListenAddrs
	}
	return append(append([]string{}, c.TLSListenAddrs...), c.SweatboxTLSListenAddrs...)
}

// Store publishes immutable snapshots. Callers must not mutate Get's result.
type Store struct {
	path string
	cur  atomic.Pointer[Config]
	mu   sync.Mutex
}

func NewStore(path string, cfg *Config) *Store { s := &Store{path: path}; s.cur.Store(cfg); return s }
func (s *Store) Get() *Config                  { return s.cur.Load() }

// Reload atomically updates welcome, weather, rating enforcement, client keys
// and admission/session limits read live or when a connection starts.
// Credentials, listeners, TLS, mode, identifiers/seeds, plaintext policy,
// MaxClients and WallopsPerServerMinute require restart.
func (s *Store) Reload() (*Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := Load(s.path)
	if err != nil {
		return nil, err
	}
	old := s.Get()
	startup := *next
	startup.Welcome = old.Welcome
	startup.Weather = old.Weather
	startup.WeatherFallback = old.WeatherFallback
	startup.FacilityEnforcement = old.FacilityEnforcement
	startup.ClientAuthKeys = old.ClientAuthKeys
	startup.MaxPreAuthConnections = old.MaxPreAuthConnections
	startup.MaxPreAuthPerIP = old.MaxPreAuthPerIP
	startup.AuthConcurrency = old.AuthConcurrency
	startup.AuthAttemptsPerMinute = old.AuthAttemptsPerMinute
	startup.AuthAttemptBurst = old.AuthAttemptBurst
	startup.AuthIPStateLimit = old.AuthIPStateLimit
	startup.LoginTimeoutSeconds = old.LoginTimeoutSeconds
	startup.IdentDelayMS = old.IdentDelayMS
	startup.InboundPacketsPerSecond = old.InboundPacketsPerSecond
	startup.InboundBytesPerSecond = old.InboundBytesPerSecond
	startup.WallopsPerClientMinute = old.WallopsPerClientMinute
	if !reflect.DeepEqual(&startup, old) {
		return nil, errors.New("config: startup-only settings changed; restart required (including credentials, listeners, mode and TLS)")
	}
	s.cur.Store(next)
	return next, nil
}
