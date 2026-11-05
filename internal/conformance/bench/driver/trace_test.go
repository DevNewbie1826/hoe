package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	traceResponse = "HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\nHello Benchmark"
	tracePrefix   = "TRACE pipeline c=2 depth=4 "
)

var traceFields = []string{
	"batches",
	"dial_p50_us", "dial_max_us",
	"write_p50_us", "write_max_us",
	"firstbyte_p50_us", "firstbyte_max_us",
	"drain_p50_us", "drain_max_us",
}

// startTraceServer serves one canned HTTP response per request until cleanup.
// Requests are framed by their blank-line terminator, so pipelined requests in
// a single read are answered one by one.
func startTraceServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[c] = struct{}{}
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					c.Close()
					mu.Lock()
					delete(conns, c)
					mu.Unlock()
				}()
				serveTraceConn(c)
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func serveTraceConn(c net.Conn) {
	in := bufio.NewReader(c)
	var request []byte
	for {
		line, err := in.ReadBytes('\n')
		request = append(request, line...)
		if bytes.HasSuffix(request, []byte("\r\n\r\n")) {
			if _, werr := c.Write([]byte(traceResponse)); werr != nil {
				return
			}
			request = request[:0]
		}
		if err != nil {
			return
		}
	}
}

// capturePipelineTrace swaps os.Stdout for a pipe around a synchronous
// pipeline call. pipeline prints from the caller's goroutine after workers
// have reported on the result channel, so there is no concurrent writer and
// no mutex: the single short line fits far inside the pipe buffer.
func capturePipelineTrace(t *testing.T, addr string) (trace string, rps float64, err error) {
	t.Helper()
	old := os.Stdout
	r, w, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatalf("stdout pipe: %v", pipeErr)
	}
	os.Stdout = w
	restored := false
	defer func() {
		if !restored {
			os.Stdout = old
		}
		r.Close()
	}()
	rps, err = pipeline(addr, 2, 4, 300*time.Millisecond)
	os.Stdout = old
	restored = true
	w.Close()
	captured, readErr := io.ReadAll(r)
	if readErr != nil {
		t.Fatalf("read captured stdout: %v", readErr)
	}
	return string(captured), rps, err
}

func parseTraceLine(t *testing.T, trace string) map[string]string {
	t.Helper()
	if !strings.HasSuffix(trace, "\n") || strings.Count(trace, "\n") != 1 {
		t.Fatalf("trace output = %q, want exactly one newline-terminated line", trace)
	}
	line := strings.TrimSuffix(trace, "\n")
	if !strings.HasPrefix(line, tracePrefix) {
		t.Fatalf("trace line = %q, want prefix %q", line, tracePrefix)
	}
	fields := strings.Fields(line)
	if len(fields) < 4 {
		t.Fatalf("trace line = %q, want at least 4 fields", line)
	}
	got := make(map[string]string, len(fields))
	for _, field := range fields[4:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" {
			t.Fatalf("field %q is not key=value", field)
		}
		if _, dup := got[key]; dup {
			t.Fatalf("duplicate field %q", key)
		}
		got[key] = value
	}
	return got
}

func TestPipelineTrace(t *testing.T) {
	addr := startTraceServer(t)
	trace, rps, err := capturePipelineTrace(t, addr)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if rps <= 0 {
		t.Errorf("rps = %v, want > 0", rps)
	}
	got := parseTraceLine(t, trace)

	seen := make(map[string]bool, len(got))
	for _, key := range traceFields {
		if _, ok := got[key]; !ok {
			t.Errorf("missing field %q", key)
			continue
		}
		seen[key] = true
	}
	for key := range got {
		if !seen[key] {
			t.Errorf("unexpected field %q", key)
		}
	}

	// batches is the measured round count: one write sample per round per
	// connection, summed across connections. With c=2 workers each completes at
	// least one round in the 300ms window, so the floor is 2 (not a constant).
	rawBatches, ok := got["batches"]
	if !ok {
		t.Errorf("missing field %q", "batches")
	} else if batches, err := strconv.Atoi(rawBatches); err != nil {
		t.Errorf("batches = %q, want integer", rawBatches)
	} else if batches < 2 {
		t.Errorf("batches = %d, want >= 2 (at least one measured round per connection)", batches)
	}

	decimalOne := regexp.MustCompile(`^[0-9]+\.[0-9]$`)
	number := func(key string) (float64, bool) {
		raw, ok := got[key]
		if !ok {
			return 0, false
		}
		if !decimalOne.MatchString(raw) {
			t.Errorf("%s = %q, want one decimal place", key, raw)
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Errorf("%s = %q: %v", key, raw, err)
			return 0, false
		}
		if value < 0 {
			t.Errorf("%s = %v, want >= 0", key, value)
			return value, false
		}
		return value, true
	}
	for _, pair := range [][2]string{
		{"dial_p50_us", "dial_max_us"},
		{"write_p50_us", "write_max_us"},
		{"firstbyte_p50_us", "firstbyte_max_us"},
		{"drain_p50_us", "drain_max_us"},
	} {
		p50, p50OK := number(pair[0])
		max, maxOK := number(pair[1])
		if maxOK && max <= 0 {
			t.Errorf("%s = %v, want > 0 with thousands of samples", pair[1], max)
		}
		if p50OK && maxOK && max < p50 {
			t.Errorf("%s = %v < %s = %v", pair[1], max, pair[0], p50)
		}
	}
	// A completed dial is always a positive duration, unlike a sub-50ns stage
	// sample that can format as 0.0.
	if dialP50, ok := number("dial_p50_us"); ok && dialP50 <= 0 {
		t.Errorf("dial_p50_us = %v, want > 0", dialP50)
	}
}

func TestStageHist(t *testing.T) {
	const micro = time.Microsecond
	cases := []struct {
		name  string
		add   []time.Duration
		n     uint64
		max   int64
		p50   float64
		maxus float64
	}{
		{
			name:  "zero histogram",
			n:     0,
			max:   0,
			p50:   0,
			maxus: 0,
		},
		{
			name:  "p50 is bucket upper bound below max",
			add:   []time.Duration{micro, 3 * micro, 10 * micro},
			n:     3,
			max:   10_000,
			p50:   4.095, // target sample lands in bucket 12: [2048, 4095]
			maxus: 10,
		},
		{
			name:  "p50 clamps bucket upper bound to max",
			add:   []time.Duration{1000 * time.Nanosecond, 1005 * time.Nanosecond},
			n:     2,
			max:   1_005,
			p50:   1.005, // bucket 10 covers [512, 1023], clamped to 1005ns
			maxus: 1.005,
		},
		{
			name:  "non-positive sample lands in bucket zero",
			add:   []time.Duration{10 * micro, -time.Millisecond},
			n:     2,
			max:   10_000,
			p50:   0, // median is the bucket-zero sample and max is positive
			maxus: 10,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h stageHist
			for _, d := range tc.add {
				h.add(d)
			}
			if h.n != tc.n {
				t.Errorf("n = %d, want %d", h.n, tc.n)
			}
			if h.max != tc.max {
				t.Errorf("max = %d, want %d", h.max, tc.max)
			}
			if got := h.p50us(); got != tc.p50 {
				t.Errorf("p50us() = %v, want %v", got, tc.p50)
			}
			if got := h.maxus(); got != tc.maxus {
				t.Errorf("maxus() = %v, want %v", got, tc.maxus)
			}
		})
	}

	t.Run("merge empty leaves receiver unchanged", func(t *testing.T) {
		cases := []struct {
			name string
			add  []time.Duration
			n    uint64
			max  int64
		}{
			{"normal samples", []time.Duration{2 * micro, 3 * micro}, 2, 3_000},
			// A receiver of only negative samples has n>0 and max<0; an empty
			// operand must not reset its max to zero.
			{"negative-only samples", []time.Duration{-2 * time.Millisecond}, 1, -2_000_000},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var h stageHist
				for _, d := range tc.add {
					h.add(d)
				}
				if h.n != tc.n || h.max != tc.max {
					t.Fatalf("receiver = (n=%d, max=%d), want (n=%d, max=%d)", h.n, h.max, tc.n, tc.max)
				}
				p50 := h.p50us()
				h.merge(stageHist{})
				if h.n != tc.n || h.max != tc.max || h.p50us() != p50 {
					t.Errorf("merge(empty) changed histogram: n=%d max=%d p50=%v, want n=%d max=%d p50=%v",
						h.n, h.max, h.p50us(), tc.n, tc.max, p50)
				}
			})
		}
	})

	t.Run("bucket sum below sample count panics", func(t *testing.T) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("p50us() did not panic for an inconsistent histogram")
			}
			if msg := fmt.Sprint(recovered); !strings.Contains(msg, "bucket sum below sample count") {
				t.Fatalf("panic = %q, want message containing %q", msg, "bucket sum below sample count")
			}
		}()
		(&stageHist{n: 3}).p50us()
	})
}
