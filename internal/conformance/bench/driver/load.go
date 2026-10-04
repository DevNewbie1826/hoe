package main

import (
	"bufio"
	"bytes"
	"fmt"
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
	for range connections {
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return 0, err
		}
		conns = append(conns, c)
	}
	payload := bytes.Repeat([]byte(request), depth)
	type result struct {
		count uint64
		err   error
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
			for time.Now().Before(until) {
				if _, err := c.Write(payload); err != nil {
					results <- result{count, err}
					return
				}
				for range depth {
					if err := readHello(reader); err != nil {
						results <- result{count, err}
						return
					}
					count++
				}
			}
			results <- result{count, nil}
		}()
	}
	ready.Wait()
	begin := time.Now()
	close(start)
	var count uint64
	var firstError error
	for range connections {
		result := <-results
		count += result.count
		if firstError == nil {
			firstError = result.err
		}
	}
	if firstError != nil || count == 0 {
		return 0, fmt.Errorf("pipeline requests=%d error=%v", count, firstError)
	}
	return float64(count) / time.Since(begin).Seconds(), nil
}
