// Package auth provides a local, bcrypt-backed authenticator.
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"golang.org/x/crypto/bcrypt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Identity struct {
	ID     string
	Rating int
}

var (
	ErrUnknownIdentity   = errors.New("unknown identity")
	ErrInvalidCredential = errors.New("invalid credential")
	ErrInactive          = errors.New("inactive identity")
	ErrRatingDenied      = errors.New("rating denied")
	ErrRateLimited       = errors.New("authentication rate limited")
)

type account struct {
	ID           string `json:"id"`
	PasswordHash string `json:"password_hash"`
	Rating       *int   `json:"rating"`
}
type FileAuthenticator struct {
	users     map[string]account
	dummyHash []byte
	limiter   *failureLimiter
}

// Load reads a strict users document. Password hashes must use bcrypt cost 12,
// matching HashPassword and the unknown-identity dummy verification.
// Credential changes require constructing a new authenticator or restarting.
func Load(path string) (*FileAuthenticator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("auth: cannot open users file; configure a readable users_file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
	if err != nil || len(data) > 16<<20 {
		return nil, errors.New("auth: cannot read users file or file exceeds 16 MiB")
	}
	var doc struct {
		Users []account `json:"users"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return nil, errors.New("auth: invalid users JSON; expected users with id, password_hash and rating")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("auth: trailing users JSON")
	}
	if len(doc.Users) == 0 {
		return nil, errors.New("auth: users file must contain at least one account")
	}
	a := &FileAuthenticator{users: make(map[string]account, len(doc.Users)), limiter: newFailureLimiter()}
	for _, u := range doc.Users {
		if !validID(u.ID) || u.Rating == nil || *u.Rating < -1 || *u.Rating > 12 {
			return nil, errors.New("auth: each account needs a numeric ID and rating from -1 to 12")
		}
		if _, ok := a.users[u.ID]; ok {
			return nil, errors.New("auth: duplicate account ID")
		}
		if !validHash(u.PasswordHash) {
			return nil, errors.New("auth: password_hash must be a complete bcrypt hash with cost 12")
		}
		a.users[u.ID] = u
	}
	a.dummyHash, err = bcrypt.GenerateFromPassword([]byte("unused-dummy-authentication-value"), 12)
	if err != nil {
		return nil, errors.New("auth: cannot initialize password verification")
	}
	return a, nil
}
func validID(id string) bool {
	if len(id) < 1 || len(id) > 12 {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
func validHash(hash string) bool {
	if len(hash) != 60 || hash[0] != '$' || hash[1] != '2' || !strings.ContainsRune("aby", rune(hash[2])) || hash[3] != '$' || hash[6] != '$' {
		return false
	}
	for _, c := range hash[7:] {
		if !strings.ContainsRune("./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", c) {
			return false
		}
	}
	cost, err := bcrypt.Cost([]byte(hash))
	return err == nil && cost == 12
}
func validPassword(password string) bool {
	return len(password) > 0 && len(password) <= 72 && !strings.ContainsAny(password, ":\r\n\x00")
}

// HashPassword generates a cost-12 bcrypt hash without retaining plaintext.
func HashPassword(password string) (string, error) {
	if !validPassword(password) {
		return "", errors.New("auth: password must be 1–72 bytes without protocol delimiters")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	if err != nil {
		return "", errors.New("auth: password hashing failed")
	}
	return string(hash), nil
}

// Authenticate uses only the socket peer's source IP for failed-attempt limits.
// Unknown and wrong-password accounts must be mapped to one protocol error by callers.
func (a *FileAuthenticator) Authenticate(ctx context.Context, id, password string, rating int, sourceIP string) (*Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Reject non-wire values before constructing limiter keys to bound their size.
	if !validID(id) || !validPassword(password) {
		return nil, ErrInvalidCredential
	}
	sourceIP = normalizeIP(sourceIP)
	if !a.limiter.begin(id, sourceIP) {
		return nil, ErrRateLimited
	}
	success := false
	defer func() { a.limiter.finish(id, sourceIP, success) }()
	u, exists := a.users[id]
	hash := a.dummyHash
	if exists {
		hash = []byte(u.PasswordHash)
	}
	err := bcrypt.CompareHashAndPassword(hash, []byte(password))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if !exists {
		return nil, ErrUnknownIdentity
	}
	if err != nil {
		return nil, ErrInvalidCredential
	}
	if *u.Rating < 1 {
		return nil, ErrInactive
	}
	if rating < 1 || rating > 12 || rating > *u.Rating {
		return nil, ErrRatingDenied
	}
	success = true
	return &Identity{ID: u.ID, Rating: *u.Rating}, nil
}
func normalizeIP(value string) string {
	if ip := net.ParseIP(value); ip != nil {
		return ip.String()
	}
	return "unknown"
}

const (
	cidFailureLimit   = 30
	ipFailureLimit    = 120
	failureWindow     = 15 * time.Minute
	failureStateLimit = 8192
)

type failureBucket struct {
	count, pending int
	expires        time.Time
}
type failureLimiter struct {
	mu        sync.Mutex
	buckets   map[string]failureBucket
	now       func() time.Time
	nextSweep time.Time
}

func newFailureLimiter() *failureLimiter {
	return &failureLimiter{buckets: make(map[string]failureBucket), now: time.Now}
}
func failureKeys(id, ip string) [2]string { return [2]string{"cid:" + id + ":" + ip, "ip:" + ip} }
func (l *failureLimiter) sweep(now time.Time) {
	for key, b := range l.buckets {
		if b.pending == 0 && !now.Before(b.expires) {
			delete(l.buckets, key)
		}
	}
	l.nextSweep = now.Add(time.Minute)
}
func (l *failureLimiter) begin(id, ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if !now.Before(l.nextSweep) {
		l.sweep(now)
	}
	keys := failureKeys(id, ip)
	missing := 0
	for i, key := range keys {
		b, ok := l.buckets[key]
		if ok && b.pending == 0 && !now.Before(b.expires) {
			delete(l.buckets, key)
			ok = false
		}
		if !ok {
			missing++
			continue
		}
		limit := cidFailureLimit
		if i == 1 {
			limit = ipFailureLimit
		}
		if b.count+b.pending >= limit {
			return false
		}
	}
	if len(l.buckets)+missing > failureStateLimit {
		l.sweep(now)
		if len(l.buckets)+missing > failureStateLimit {
			return false
		}
	}
	for _, key := range keys {
		b, ok := l.buckets[key]
		if !ok {
			b.expires = now.Add(failureWindow)
		}
		b.pending++
		l.buckets[key] = b
	}
	return true
}
func (l *failureLimiter) finish(id, ip string, success bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, key := range failureKeys(id, ip) {
		b, ok := l.buckets[key]
		if !ok {
			continue
		}
		b.pending--
		if !success {
			b.count++
		} else if i == 0 {
			b.count = 0
		}
		if b.count == 0 && b.pending == 0 {
			delete(l.buckets, key)
		} else {
			l.buckets[key] = b
		}
	}
}
