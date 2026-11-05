package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math/bits"
	"net"
	"sync"
	"time"
)

func readHello(in *bufio.Reader) error {
	line, err := in.ReadSlice('\n')
	if err != nil || !bytes.Equal(line, []byte("HTTP/1.1 200 OK\r\n")) {
		return fmt.Errorf("invalid status %q: %v", line, err)
	}
	length := false
	for {
		line, err = in.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, []byte("Content-Length: 15\r\n")) {
			length = true
		}
		if bytes.Equal(line, []byte("\r\n")) {
			break
		}
	}
	if !length {
		return fmt.Errorf("missing or wrong Content-Length")
	}
	body, err := in.Peek(len(hello))
	if err != nil || string(body) != hello {
		return fmt.Errorf("invalid body %q: %v", body, err)
	}
	_, err = in.Discard(len(hello))
	return err
}

func pipeline(addr string, connections, depth int, duration time.Duration) (float64, error) {
	conns := make([]net.Conn, 0, connections)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	var dial stageHist
	for range connections {
		dialStart := time.Now()
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		dialed := time.Now()
		if err != nil {
			return 0, err
		}
		dial.add(dialed.Sub(dialStart))
		conns = append(conns, c)
	}
	payload := bytes.Repeat([]byte(request), depth)
	type result struct {
		count     uint64
		err       error
		write     stageHist
		firstByte stageHist
		drain     stageHist
	}
	results := make(chan result, connections)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(connections)
	until := time.Now().Add(duration)
	for _, c := range conns {
		go func() {
			reader := bufio.NewReader(c)
			c.SetDeadline(until.Add(10 * time.Second))
			ready.Done()
			<-start
			var count uint64
			var write, firstByte, drain stageHist
			for time.Now().Before(until) {
				writeStart := time.Now()
				if _, err := c.Write(payload); err != nil {
					results <- result{count: count, err: err}
					return
				}
				wrote := time.Now()
				write.add(wrote.Sub(writeStart))
				if _, err := reader.Peek(1); err != nil {
					results <- result{count: count, err: err}
					return
				}
				firstByteAt := time.Now()
				firstByte.add(firstByteAt.Sub(wrote))
				for range depth {
					if err := readHello(reader); err != nil {
						results <- result{count: count, err: err}
						return
					}
					count++
				}
				// drain is mixed server pacing plus client parse; net/http/httptrace cannot attach to a raw net.Conn client.
				drain.add(time.Now().Sub(firstByteAt))
			}
			results <- result{count: count, write: write, firstByte: firstByte, drain: drain}
		}()
	}
	got := make([]result, connections)
	ready.Wait()
	begin := time.Now()
	close(start)
	var count uint64
	var firstError error
	for i := range got {
		got[i] = <-results
		count += got[i].count
		if firstError == nil {
			firstError = got[i].err
		}
	}
	elapsed := time.Since(begin)
	if firstError != nil || count == 0 {
		return 0, fmt.Errorf("pipeline requests=%d error=%v", count, firstError)
	}
	var write, firstByte, drain stageHist
	for _, item := range got {
		write.merge(item.write)
		firstByte.merge(item.firstByte)
		drain.merge(item.drain)
	}
	// batches is the number of pipeline rounds measured, one write sample per round per connection, summed across connections - it is a measurement, not a constant.
	fmt.Printf("TRACE pipeline c=%d depth=%d batches=%d dial_p50_us=%.1f dial_max_us=%.1f write_p50_us=%.1f write_max_us=%.1f firstbyte_p50_us=%.1f firstbyte_max_us=%.1f drain_p50_us=%.1f drain_max_us=%.1f\n",
		connections, depth, write.n,
		dial.p50us(), dial.maxus(),
		write.p50us(), write.maxus(),
		firstByte.p50us(), firstByte.maxus(),
		drain.p50us(), drain.maxus())
	return float64(count) / elapsed.Seconds(), nil
}

type stageHist struct {
	buckets [64]uint64
	n       uint64
	max     int64
}

func (h *stageHist) add(d time.Duration) {
	ns := int64(d)
	h.n++
	if h.n == 1 || ns > h.max {
		h.max = ns
	}
	if ns <= 0 {
		h.buckets[0]++
		return
	}
	h.buckets[bits.Len64(uint64(ns))]++
}

func (h *stageHist) merge(o stageHist) {
	for i := range h.buckets {
		h.buckets[i] += o.buckets[i]
	}
	// An empty operand must not contribute a zero max: a receiver of only negative samples would otherwise gain one.
	if o.n > 0 && (h.n == 0 || o.max > h.max) {
		h.max = o.max
	}
	h.n += o.n
}

func (h *stageHist) p50us() float64 {
	if h.n == 0 {
		return 0
	}
	target := (h.n + 1) / 2
	var cum uint64
	for b, c := range h.buckets {
		cum += c
		if cum < target {
			continue
		}
		if b == 0 {
			if h.max <= 0 {
				return float64(h.max) / 1000
			}
			return 0
		}
		hi := int64((uint64(1) << uint(b)) - 1)
		if hi > h.max {
			hi = h.max
		}
		return float64(hi) / 1000
	}
	// add bumps one bucket and merge sums buckets with n, so a consistent histogram always returns here.
	panic("stageHist: bucket sum below sample count")
}

func (h *stageHist) maxus() float64 {
	if h.n == 0 {
		return 0
	}
	return float64(h.max) / 1000
}
