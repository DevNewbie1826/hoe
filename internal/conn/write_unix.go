//go:build linux || darwin

package conn

import (
	"io"
	"time"
)

// Write blocks at socket backpressure instead of queueing unbounded output.
func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	total := 0
	writeInterest := false
	defer func() {
		if writeInterest {
			c.raw.SetInterest(true, false)
		}
	}()
	for len(p) > 0 {
		t, _ := c.deadline("write")
		if !t.IsZero() && !time.Now().Before(t) {
			c.cancelRequest()
			return total, c.timeoutError("write")
		}
		ready := c.raw.WriteReady()
		n, err := c.pipelineWrite(p)
		total += n
		p = p[n:]
		if interrupted(err) {
			continue
		}
		if wouldBlock(err) {
			if !writeInterest {
				if err = c.raw.SetInterest(true, true); err != nil {
					return total, err
				}
				writeInterest = true
			}
			if err = c.wait("write", ready); err != nil {
				c.cancelRequest()
				return total, err
			}
			continue
		}
		if err != nil {
			c.peerEOF()
			c.cancelRequest()
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}
