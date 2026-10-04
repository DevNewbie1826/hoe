package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type stats struct {
	Mallocs    uint64
	Requests   uint64
	Goroutines int
	Idle       int
}

func readStats(p *process, idle int) (stats, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	res, err := client.Get(fmt.Sprintf("http://%s/stats?idle=%d", p.Admin, idle))
	if err != nil {
		return stats{}, err
	}
	defer res.Body.Close()
	var value stats
	err = json.NewDecoder(res.Body).Decode(&value)
	return value, err
}

func resources(binary, impl, wsclient string) (err error) {
	p, err := launch(binary, impl, true, "")
	if err != nil {
		return err
	}
	var idleClients []net.Conn
	defer func() {
		if e := p.close(); err == nil {
			err = e
		}
		// The server is the active closer; client ephemeral ports must not
		// accumulate TIME_WAIT across the two 10,000-connection fixtures.
		for _, c := range idleClients {
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			var b [1]byte
			n, e := c.Read(b[:])
			if err == nil && (n != 0 || e != io.EOF) {
				err = fmt.Errorf("idle fixture did not receive server EOF: n=%d err=%v", n, e)
			}
			c.Close()
		}
	}()
	if err := sample(p); err != nil {
		return err
	}
	if wsclient != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, wsclient, "-url", "ws://"+p.Address+"/ws", "-n", "16")
		cmd.Stderr = os.Stderr
		output, err := cmd.Output()
		cancel()
		if err != nil {
			return err
		}
		fmt.Printf("COMMAND %s -url ws://%s/ws -n 16\n%s", wsclient, p.Address, output)
	}
	c, err := net.DialTimeout("tcp", p.Address, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Minute))
	in := bufio.NewReader(c)
	get := []byte(request)
	round := func() error {
		if _, err := c.Write(get); err != nil {
			return err
		}
		return readHello(in)
	}
	for range 1000 {
		if err := round(); err != nil {
			return err
		}
	}
	before, err := readStats(p, 1)
	if err != nil {
		return err
	}
	for range 10000 {
		if err := round(); err != nil {
			return err
		}
	}
	after, err := readStats(p, 1)
	if err != nil {
		return err
	}
	requests := after.Requests - before.Requests
	if requests != 10000 {
		return fmt.Errorf("allocation request count=%d, want 10000", requests)
	}
	allocations := float64(after.Mallocs-before.Mallocs) / float64(requests)
	fmt.Printf("RESOURCE impl=%s allocations_per_GET=%.4f requests=%d errors=0 all_bodies_validated=true\n", impl, allocations, requests)
	if impl == "hoe" && allocations > 6 {
		return fmt.Errorf("server GET allocations %.4f exceed 6", allocations)
	}
	c.Close()
	// The admin stats request itself is present in both snapshots.
	before, err = readStats(p, 0)
	if err != nil {
		return err
	}
	idleClients = make([]net.Conn, 0, 10000)
	for range 10000 {
		c, err := net.DialTimeout("tcp", p.Address, 5*time.Second)
		if err != nil {
			return err
		}
		idleClients = append(idleClients, c)
		c.SetDeadline(time.Now().Add(2 * time.Minute))
		if _, err := c.Write(get); err != nil {
			return err
		}
		if err := readHello(bufio.NewReader(c)); err != nil {
			return err
		}
	}
	after, err = readStats(p, 10000)
	if err != nil {
		return err
	}
	delta := after.Goroutines - before.Goroutines
	fmt.Printf("RESOURCE impl=%s idle_connections=%d server_goroutine_delta=%d errors=0 all_bodies_validated=true\n", impl, after.Idle, delta)
	if after.Idle != 10000 || (impl == "hoe" && delta > 50) {
		return fmt.Errorf("idle resources connections=%d goroutine delta=%d", after.Idle, delta)
	}
	return nil
}
