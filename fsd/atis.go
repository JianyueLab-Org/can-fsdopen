package fsd

import (
	"fmt"
	"time"
)

const (
	maxATISLines = 64
	// Legacy decoding can expand one wire byte into a three-byte UTF-8 rune.
	maxATISBytes        = 3 * maxATISLines * MaxPacketSize
	atisResponseTimeout = 30 * time.Second
)

type textATISResponse struct {
	lines    []string
	bytes    int
	invalid  bool
	deadline time.Time
	timer    *time.Timer
}

// queueTextATISRefresh starts at most one response and retains the published
// snapshot until its end marker. The nonblocking enqueue and response state
// are serialized so an immediate reply cannot race the refresh initialization.
func (c *Client) queueTextATISRefresh() {
	c.atisMu.Lock()
	defer c.atisMu.Unlock()
	if c.ctx.Err() != nil {
		c.clearPendingATISLocked()
		return
	}
	if pending := c.pendingATIS; pending != nil {
		if time.Now().Before(pending.deadline) {
			return
		}
		c.clearPendingATISLocked()
	}
	c.drainATIS = false
	c.startTextATISResponseLocked()
	if err := c.SendBestEffort(fmt.Sprintf("$CQSERVER:%s:ATIS\r\n", c.Callsign)); err != nil || c.ctx.Err() != nil {
		c.clearPendingATISLocked()
		c.drainATIS = true
	}
}

func (c *Client) startTextATISResponseLocked() {
	pending := &textATISResponse{deadline: time.Now().Add(atisResponseTimeout)}
	c.pendingATIS = pending
	pending.timer = time.AfterFunc(atisResponseTimeout, func() { c.expireTextATISResponse(pending) })
}

// receiveTextATIS buffers text and atomically publishes it on E. Metadata can
// start an unsolicited response, including one with no text to publish.
func (c *Client) receiveTextATIS(kind, line string) {
	c.atisMu.Lock()
	defer c.atisMu.Unlock()
	if c.ctx.Err() != nil {
		c.clearPendingATISLocked()
		return
	}
	if pending := c.pendingATIS; pending != nil && !time.Now().Before(pending.deadline) {
		c.clearPendingATISLocked()
		c.drainATIS = true
	}
	if c.drainATIS {
		if kind == "E" {
			c.drainATIS = false
		}
		return
	}
	if kind == "E" {
		if pending := c.pendingATIS; pending != nil {
			if !pending.invalid {
				c.setTextATIS(pending.lines)
			}
			c.clearPendingATISLocked()
		}
		return
	}
	if kind != "T" && kind != "A" && kind != "Z" && kind != "V" {
		return
	}
	if c.pendingATIS == nil {
		c.startTextATISResponseLocked()
	}
	pending := c.pendingATIS
	if kind != "T" || line == "" || pending.invalid {
		return
	}
	if len(pending.lines) >= maxATISLines || len(line) > maxATISBytes-pending.bytes {
		pending.invalid = true
		pending.lines = nil
		pending.bytes = 0
		return
	}
	pending.lines = append(pending.lines, line)
	pending.bytes += len(line)
}

func (c *Client) expireTextATISResponse(pending *textATISResponse) {
	c.atisMu.Lock()
	defer c.atisMu.Unlock()
	if c.pendingATIS == pending {
		c.clearPendingATISLocked()
		c.drainATIS = true
	}
}

func (c *Client) clearPendingATISLocked() {
	if c.pendingATIS != nil {
		c.pendingATIS.timer.Stop()
		c.pendingATIS = nil
	}
}

// Cancellation cleanup runs asynchronously; Disconnect can be called by the
// enqueue path while atisMu is held.
func (c *Client) discardTextATISRefresh() {
	c.atisMu.Lock()
	defer c.atisMu.Unlock()
	c.clearPendingATISLocked()
	c.drainATIS = true
}
