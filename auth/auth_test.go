package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func authFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func accountJSON(t *testing.T, rating int) string {
	t.Helper()
	hash, err := HashPassword("synthetic-password")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"users": []map[string]any{{"id": "900001", "password_hash": hash, "rating": rating}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func TestHashPasswordBounds(t *testing.T) {
	for _, p := range []string{"", strings.Repeat("x", 73), "bad\npassword", "bad:password"} {
		if _, err := HashPassword(p); err == nil {
			t.Fatal("accepted invalid credential")
		}
	}
	hash, err := HashPassword("synthetic-password")
	if err != nil {
		t.Fatal(err)
	}
	if cost, err := bcrypt.Cost([]byte(hash)); err != nil || cost != 12 {
		t.Fatal("unexpected hash cost")
	}
}
func TestLoadRejectsMalformedUsers(t *testing.T) {
	valid := accountJSON(t, 3)
	for _, text := range []string{`{}`, `null`, `{"users":[]}`, `{"users":[{"id":"900001","password":"plaintext","rating":3}]}`, `{"users":[{"id":"900001","password_hash":"plaintext","rating":3}]}`, strings.Replace(valid, `"rating":3`, `"rating":13`, 1), strings.Replace(valid, `900001`, `bad:cid`, 1), strings.Replace(valid, `}]}`, `},`+strings.TrimSuffix(strings.TrimPrefix(valid, `{"users":[`), `]}`)+`]}`, 1), strings.Replace(valid, `"rating":3`, `"rating":null`, 1)} {
		if _, err := Load(authFile(t, text)); err == nil {
			t.Fatalf("accepted malformed account document")
		}
	}
	hash, _ := bcrypt.GenerateFromPassword([]byte("synthetic"), 4)
	data, _ := json.Marshal(map[string]any{"users": []map[string]any{{"id": "900001", "password_hash": string(hash), "rating": 3}}})
	if _, err := Load(authFile(t, string(data))); err == nil {
		t.Fatal("accepted weak hash")
	}
	if validHash(strings.Replace(strings.TrimSpace(string(hash)), "$04$", "$15$", 1)) {
		t.Fatal("accepted excessive bcrypt cost")
	}
}
func TestLoadRejectsMixedBcryptCosts(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-password"), 13)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{string(hash), strings.Replace(string(hash), "$13$", "$14$", 1)} {
		data, err := json.Marshal(map[string]any{"users": []map[string]any{{"id": "900001", "password_hash": candidate, "rating": 3}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Load(authFile(t, string(data))); err == nil {
			t.Fatal("accepted bcrypt cost differing from the dummy and generated hashes")
		} else if strings.Contains(err.Error(), candidate) || strings.Contains(err.Error(), "900001") {
			t.Fatal("credential input exposed in load error")
		}
	}
}

func TestAuthenticateIdentityAndRatingBoundaries(t *testing.T) {
	a, err := Load(authFile(t, accountJSON(t, 3)))
	if err != nil {
		t.Fatal(err)
	}
	id, err := a.Authenticate(context.Background(), "900001", "synthetic-password", 3, "192.0.2.1")
	if err != nil || id == nil || id.ID != "900001" || id.Rating != 3 {
		t.Fatal("valid login rejected", err)
	}
	for _, tc := range []struct {
		id, password string
		rating       int
		want         error
	}{
		{"900001", "wrong", 3, ErrInvalidCredential}, {"900002", "wrong", 3, ErrUnknownIdentity}, {"900001", "synthetic-password", 4, ErrRatingDenied}, {"900001", "synthetic-password", 13, ErrRatingDenied},
	} {
		if _, err := a.Authenticate(context.Background(), tc.id, tc.password, tc.rating, "192.0.2.1"); !errors.Is(err, tc.want) {
			t.Fatalf("expected %v, got %v", tc.want, err)
		}
	}
	for _, rating := range []int{-1, 0} {
		inactive, err := Load(authFile(t, accountJSON(t, rating)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := inactive.Authenticate(context.Background(), "900001", "synthetic-password", 1, "192.0.2.1"); !errors.Is(err, ErrInactive) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Authenticate(ctx, "900001", "synthetic-password", 1, "192.0.2.1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestFailureLimiterBoundsExpiryAndSuccess(t *testing.T) {
	now := time.Unix(100, 0)
	l := newFailureLimiter()
	l.now = func() time.Time { return now }
	for range cidFailureLimit {
		if !l.begin("900001", "192.0.2.1") {
			t.Fatal("premature throttle")
		}
		l.finish("900001", "192.0.2.1", false)
	}
	if l.begin("900001", "192.0.2.1") {
		t.Fatal("missing CID throttle")
	}
	now = now.Add(failureWindow)
	if !l.begin("900001", "192.0.2.1") {
		t.Fatal("expired throttle")
	}
	l.finish("900001", "192.0.2.1", true)
	for i := range ipFailureLimit {
		id := fmt.Sprintf("9%05d", i)
		if !l.begin(id, "192.0.2.2") {
			t.Fatal("premature IP throttle")
		}
		l.finish(id, "192.0.2.2", false)
	}
	if l.begin("999999", "192.0.2.2") {
		t.Fatal("missing aggregate IP throttle")
	}
	// Distinct CID buckets may not remove aggregate IP failures on success.
	l.buckets = map[string]failureBucket{"ip:192.0.2.3": {count: ipFailureLimit, expires: now.Add(failureWindow)}}
	if l.begin("900003", "192.0.2.3") {
		t.Fatal("missing IP throttle")
	}
	l.buckets = make(map[string]failureBucket)
	for i := range failureStateLimit {
		l.buckets[string(rune(i))] = failureBucket{expires: now.Add(failureWindow)}
	}
	if l.begin("900004", "192.0.2.4") {
		t.Fatal("state limit allowed more buckets")
	}
	if len(l.buckets) > failureStateLimit {
		t.Fatal("unbounded state")
	}
}
func TestAuthenticateAppliesFailureThrottle(t *testing.T) {
	a, err := Load(authFile(t, accountJSON(t, 3)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	a.limiter.buckets["cid:900001:192.0.2.9"] = failureBucket{count: cidFailureLimit, expires: now.Add(failureWindow)}
	if _, err := a.Authenticate(context.Background(), "900001", "synthetic-password", 1, "192.0.2.9"); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
}
