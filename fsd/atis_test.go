package fsd

import (
	"context"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func assertATIS(t *testing.T, c *Client, want []string) {
	t.Helper()
	if got := c.TextATIS(); !slices.Equal(got, want) {
		t.Fatalf("ATIS = %q, want %q", got, want)
	}
}

func pendingATIS(c *Client) *textATISResponse {
	c.atisMu.Lock()
	defer c.atisMu.Unlock()
	return c.pendingATIS
}

func TestATISPublishesOnlyCompleteImmutableSnapshots(t *testing.T) {
	c := newIdleClient(t, "TEST_ATIS")
	old := []string{"OLD", "COMPLETE"}
	c.setTextATIS(old)
	old[0] = "MUTATED"
	oldSnapshot := c.TextATIS()
	c.queueTextATISRefresh()
	c.receiveTextATIS("T", "NEW")
	assertATIS(t, c, []string{"OLD", "COMPLETE"})
	c.receiveTextATIS("T", "COMPLETE")
	assertATIS(t, c, []string{"OLD", "COMPLETE"})
	c.receiveTextATIS("E", "")
	assertATIS(t, c, []string{"NEW", "COMPLETE"})
	if !slices.Equal(oldSnapshot, []string{"OLD", "COMPLETE"}) {
		t.Fatalf("previously published snapshot changed: %q", oldSnapshot)
	}
	c.receiveTextATIS("T", "NEXT")
	assertATIS(t, c, []string{"NEW", "COMPLETE"})
}

func TestATISEmptyResponseAndUnsolicitedMarkers(t *testing.T) {
	for _, start := range []string{"poll", "A", "Z", "V", "T"} {
		t.Run(start, func(t *testing.T) {
			c := newIdleClient(t, "TEST_ATIS")
			c.setTextATIS([]string{"OLD"})
			c.receiveTextATIS("E", "")
			c.receiveTextATIS("unknown", "IGNORED")
			assertATIS(t, c, []string{"OLD"})
			if start == "poll" {
				c.queueTextATISRefresh()
			} else {
				c.receiveTextATIS(start, "")
			}
			assertATIS(t, c, []string{"OLD"})
			c.receiveTextATIS("E", "")
			assertATIS(t, c, nil)
			c.receiveTextATIS("E", "")
			assertATIS(t, c, nil)
		})
	}
}

func TestATISOverlappingPollDoesNotResetPendingResponse(t *testing.T) {
	for _, unsolicited := range []bool{false, true} {
		t.Run(map[bool]string{false: "polled", true: "unsolicited"}[unsolicited], func(t *testing.T) {
			c := newIdleClient(t, "TEST_ATIS")
			if !unsolicited {
				c.queueTextATISRefresh()
			}
			c.receiveTextATIS("T", "FIRST")
			before := pendingATIS(c)
			queued := len(c.sendCh)
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(c.queueTextATISRefresh)
			}
			wg.Wait()
			if pendingATIS(c) != before || len(c.sendCh) != queued {
				t.Fatal("overlapping poll replaced the pending response or queued another request")
			}
			c.receiveTextATIS("T", "SECOND")
			c.receiveTextATIS("E", "")
			assertATIS(t, c, []string{"FIRST", "SECOND"})
		})
	}
}

func TestATISBoundsRejectEntireResponse(t *testing.T) {
	for _, bound := range []string{"lines", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			c := newIdleClient(t, "TEST_ATIS")
			c.setTextATIS([]string{"OLD"})
			c.queueTextATISRefresh()
			if bound == "lines" {
				for range maxATISLines {
					c.receiveTextATIS("T", "LINE")
				}
			} else {
				c.receiveTextATIS("T", strings.Repeat("X", maxATISBytes))
			}
			c.atisMu.Lock()
			valid := !c.pendingATIS.invalid
			c.atisMu.Unlock()
			if !valid {
				t.Fatal("response at the bound was rejected")
			}
			c.receiveTextATIS("T", "OVERFLOW")
			c.receiveTextATIS("T", "IGNORED")
			c.atisMu.Lock()
			invalid := c.pendingATIS.invalid && len(c.pendingATIS.lines) == 0 && c.pendingATIS.bytes == 0
			c.atisMu.Unlock()
			if !invalid {
				t.Fatal("overflow did not invalidate and release the buffered response")
			}
			c.receiveTextATIS("E", "")
			assertATIS(t, c, []string{"OLD"})
			if pendingATIS(c) != nil {
				t.Fatal("invalid response was not cleared on E")
			}
			c.receiveTextATIS("T", "RECOVERED")
			c.receiveTextATIS("E", "")
			assertATIS(t, c, []string{"RECOVERED"})
		})
	}
}

func TestATISMaximumLinesCanComplete(t *testing.T) {
	c := newIdleClient(t, "TEST_ATIS")
	lines := make([]string, maxATISLines)
	for i := range lines {
		lines[i] = "LINE"
		c.receiveTextATIS("T", lines[i])
	}
	c.receiveTextATIS("E", "")
	assertATIS(t, c, lines)
}

func TestATISTimeoutRetainsSnapshotAndDrainsLateResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newIdleClient(t, "TEST_ATIS")
		defer c.cancel()
		c.setTextATIS([]string{"OLD"})
		c.queueTextATISRefresh()
		c.receiveTextATIS("T", "INCOMPLETE")
		time.Sleep(atisResponseTimeout)
		synctest.Wait()
		if pendingATIS(c) != nil {
			t.Fatal("timeout retained the pending buffer")
		}
		for _, kind := range []string{"T", "A", "V", "Z"} {
			c.receiveTextATIS(kind, "LATE")
		}
		if pendingATIS(c) != nil {
			t.Fatal("late response restarted an expired refresh")
		}
		c.receiveTextATIS("E", "")
		assertATIS(t, c, []string{"OLD"})
		c.receiveTextATIS("T", "UNSOLICITED")
		c.receiveTextATIS("E", "")
		assertATIS(t, c, []string{"UNSOLICITED"})
	})
}

func TestATISDeadlineCheckedBeforeTimerCallback(t *testing.T) {
	for _, action := range []string{"end", "poll"} {
		t.Run(action, func(t *testing.T) {
			c := newIdleClient(t, "TEST_ATIS")
			c.setTextATIS([]string{"OLD"})
			c.queueTextATISRefresh()
			c.receiveTextATIS("T", "EXPIRED")
			previous := pendingATIS(c)
			c.atisMu.Lock()
			previous.deadline = time.Now().Add(-time.Second)
			c.atisMu.Unlock()
			if action == "end" {
				c.receiveTextATIS("E", "")
				assertATIS(t, c, []string{"OLD"})
			} else {
				c.queueTextATISRefresh()
				if pendingATIS(c) == previous || len(c.sendCh) != 2 {
					t.Fatal("new poll did not replace expired response")
				}
				c.receiveTextATIS("T", "NEW")
				c.receiveTextATIS("E", "")
				assertATIS(t, c, []string{"NEW"})
			}
		})
	}
}

func TestATISOldTimerCannotDiscardNewResponse(t *testing.T) {
	c := newIdleClient(t, "TEST_ATIS")
	c.queueTextATISRefresh()
	previous := pendingATIS(c)
	c.receiveTextATIS("T", "FIRST")
	c.receiveTextATIS("E", "")
	c.queueTextATISRefresh()
	next := pendingATIS(c)
	c.receiveTextATIS("T", "SECOND")
	c.expireTextATISResponse(previous)
	if pendingATIS(c) != next {
		t.Fatal("old timer discarded the replacement response")
	}
	c.receiveTextATIS("E", "")
	assertATIS(t, c, []string{"SECOND"})
}

func TestATISEnqueueFailureRetainsSnapshot(t *testing.T) {
	c := newIdleClient(t, "TEST_ATIS")
	c.setTextATIS([]string{"OLD"})
	for range cap(c.sendCh) {
		c.sendCh <- "FULL"
	}
	done := make(chan struct{})
	go func() {
		c.queueTextATISRefresh()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue failure deadlocked cancellation cleanup")
	}
	if c.ctx.Err() == nil || pendingATIS(c) != nil {
		t.Fatal("enqueue failure did not disconnect and discard the pending response")
	}
	c.receiveTextATIS("T", "LATE")
	c.receiveTextATIS("E", "")
	assertATIS(t, c, []string{"OLD"})
}

func TestATISContextCancellationDiscardsPendingResponse(t *testing.T) {
	for _, cause := range []string{"client", "disconnect", "server"} {
		t.Run(cause, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				local, remote := net.Pipe()
				defer local.Close()
				defer remote.Close()
				c := newClient(ctx, local, nil, LoginData{Callsign: "TEST_ATIS", IsATC: true})
				defer c.cancel()
				c.setTextATIS([]string{"OLD"})
				c.queueTextATISRefresh()
				c.receiveTextATIS("T", "INCOMPLETE")
				switch cause {
				case "client":
					c.cancel()
				case "disconnect":
					c.Disconnect("test disconnect")
				case "server":
					cancel()
				}
				synctest.Wait()
				if pendingATIS(c) != nil {
					t.Fatal("cancellation retained pending response")
				}
				queued := len(c.sendCh)
				c.queueTextATISRefresh()
				c.receiveTextATIS("T", "LATE")
				c.receiveTextATIS("E", "")
				if pendingATIS(c) != nil || len(c.sendCh) != queued {
					t.Fatal("canceled client started another response")
				}
				assertATIS(t, c, []string{"OLD"})
			})
		})
	}
}

func TestATISConcurrentReadersNeverObservePartialSnapshot(t *testing.T) {
	c := newIdleClient(t, "TEST_ATIS")
	c.setTextATIS([]string{"OLD", "OLD"})
	var readers sync.WaitGroup
	done := make(chan struct{})
	for range 8 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					lines := c.TextATIS()
					if len(lines) != 2 || lines[0] != lines[1] {
						t.Errorf("observed partial ATIS snapshot: %q", lines)
						return
					}
				}
			}
		})
	}
	for range 1000 {
		for _, line := range []string{"NEW", "OLD"} {
			c.receiveTextATIS("T", line)
			c.receiveTextATIS("T", line)
			c.receiveTextATIS("E", "")
		}
	}
	close(done)
	readers.Wait()
}

func TestATISHandlerRetainsColonsAndIgnoresPilotResponses(t *testing.T) {
	s := &Server{}
	c := newIdleClient(t, "TEST_ATIS")
	s.handleTextATISResponse(c, []byte("$CRTEST_ATIS:SERVER:ATIS:T:IGNORED\r\n"))
	s.handleTextATISResponse(c, []byte("$CRTEST_ATIS:SERVER:ATIS:E:2\r\n"))
	assertATIS(t, c, nil)
	c.IsATC = true
	s.handleTextATISResponse(c, []byte("$CRTEST_ATIS:SERVER:ATIS:T:ONE:TWO\r\n"))
	assertATIS(t, c, nil)
	s.handleTextATISResponse(c, []byte("$CRTEST_ATIS:SERVER:ATIS:E:2\r\n"))
	assertATIS(t, c, []string{"ONE:TWO"})
}
