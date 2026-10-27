package fsd

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
)

// ErrUnsupportedAuthClient is returned when a client software ID has no known
// obfuscation key. Such a client simply cannot answer challenges.
var ErrUnsupportedAuthClient = errors.New("vatsimauth: unsupported client")

// AuthState is the per-connection VATSIM challenge/response state machine.
//
// Not safe for concurrent use; it is owned by the connection's read goroutine,
// which is the only thing that ever touches it.
type AuthState struct {
	initial  [16]byte
	current  [16]byte
	clientID uint16
}

// Initialize seeds the state from the client's software ID and the initial
// challenge sent in its $ID packet.
func (s *AuthState) Initialize(clientID uint16, keyStr string, initialChallenge []byte) error {
	*s = AuthState{}
	if keyStr == "" || clientID == 0 {
		return ErrUnsupportedAuthClient
	}
	if len(initialChallenge) == 0 {
		return errors.New("vatsimauth: empty initial challenge")
	}

	s.clientID = clientID

	var key [32]byte
	copy(key[:], keyStr)

	s.initial = s.obfuscate(&key, initialChallenge)
	s.current = s.initial

	return nil
}

// Initialized reports whether the state can answer a challenge.
func (s *AuthState) Initialized() bool { return s.clientID != 0 }

// Response computes the reply to a challenge, hex-encoded.
func (s *AuthState) Response(challenge []byte) (res [32]byte) {
	current := hexOf(&s.current)
	round := s.obfuscate(&current, challenge)
	hex.Encode(res[:], round[:])
	return res
}

// Advance folds an issued response back into the state, so each challenge
// depends on the whole exchange rather than only the seed.
func (s *AuthState) Advance(response *[32]byte) {
	initial := hexOf(&s.initial)

	var buf [64]byte
	copy(buf[:32], initial[:])
	copy(buf[32:], response[:])

	s.current = md5.Sum(buf[:])
}

func hexOf(src *[16]byte) (dst [32]byte) {
	hex.Encode(dst[:], src[:])
	return dst
}

// obfuscate is one round of the VATSIM scheme: the challenge is split in two,
// optionally swapped, then interleaved into three slices of the current state
// before being hashed. Which interleaving is used depends on the client ID.
func (s *AuthState) obfuscate(current *[32]byte, challenge []byte) [16]byte {
	half := len(challenge) / 2
	c1, c2 := challenge[:half], challenge[half:]

	if s.clientID&1 == 1 {
		c1, c2 = c2, c1
	}

	s1, s2, s3 := current[0:12], current[12:22], current[22:32]

	buf := make([]byte, 0, 64)
	switch s.clientID % 3 {
	case 0:
		buf = append(buf, s1...)
		buf = append(buf, c1...)
		buf = append(buf, s2...)
		buf = append(buf, c2...)
		buf = append(buf, s3...)
	case 1:
		buf = append(buf, s2...)
		buf = append(buf, c1...)
		buf = append(buf, s3...)
		buf = append(buf, c2...)
		buf = append(buf, s1...)
	default:
		buf = append(buf, s3...)
		buf = append(buf, c1...)
		buf = append(buf, s1...)
		buf = append(buf, c2...)
		buf = append(buf, s2...)
	}

	return md5.Sum(buf)
}
