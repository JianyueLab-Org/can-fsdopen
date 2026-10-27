package fsd

import (
	"testing"
	"time"
)

func TestMetarNegativeCacheSuppressesRepeatedMisses(t *testing.T) {
	m := &metarService{cache: make(map[string]metarEntry), limits: make(map[string]metarLimit)}
	m.store("XXXX", "", false)
	entry, ok := m.cached("XXXX")
	if !ok || entry.ok {
		t.Fatalf("negative cache = (%+v, %v), want a cached miss", entry, ok)
	}
	m.cacheMu.Lock()
	m.cache["XXXX"] = metarEntry{fetchedAt: time.Now().Add(-metarNegativeTTL - time.Second)}
	m.cacheMu.Unlock()
	if _, ok := m.cached("XXXX"); ok {
		t.Fatal("expired negative cache entry was reused")
	}
}

func TestMetarRequestsAreLimitedPerClient(t *testing.T) {
	m := &metarService{cache: make(map[string]metarEntry), limits: make(map[string]metarLimit)}
	for i := 0; i < metarPerClientLimit; i++ {
		if !m.allow("CCA101") {
			t.Fatalf("request %d was unexpectedly limited", i+1)
		}
	}
	if m.allow("CCA101") {
		t.Fatal("request beyond per-client limit was allowed")
	}
	if !m.allow("CES500") {
		t.Fatal("limit leaked between clients")
	}
}

func TestMetarEvictRemovesExpiredClientLimitsAndPreservesActiveLimits(t *testing.T) {
	m := &metarService{cache: make(map[string]metarEntry), limits: make(map[string]metarLimit)}
	m.limits["EXPIRED"] = metarLimit{started: time.Now().Add(-metarLimitWindow - time.Second), count: metarPerClientLimit}
	m.limits["ACTIVE"] = metarLimit{started: time.Now(), count: metarPerClientLimit}

	m.evict()

	if _, ok := m.limits["EXPIRED"]; ok {
		t.Fatal("eviction retained an expired client limiter entry")
	}
	if m.allow("ACTIVE") {
		t.Fatal("eviction reset an active client's exhausted request limit")
	}
	if !m.allow("EXPIRED") {
		t.Fatal("expired client could not begin a new request window")
	}
}
