package sched

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete")
	}
}

func TestScheduledFlagExclusivity(t *testing.T) {
	p := New(time.Minute)
	defer p.Close()
	var q Queue
	gate := make(chan struct{})
	defer close(gate)
	started := make(chan struct{})
	if !q.Submit(p, func() { close(started); <-gate }) {
		t.Fatal("first task rejected")
	}
	await(t, started)
	var active, overlap atomic.Int64
	var got []int
	done := make(chan struct{})
	for i := range 5000 {
		if !q.Submit(p, func() {
			if active.Add(1) != 1 {
				overlap.Add(1)
			}
			got = append(got, i)
			active.Add(-1)
			if i == 4999 {
				close(done)
			}
		}) {
			t.Fatal("queued task rejected")
		}
	}
	// Release exactly after the whole FIFO exists.
	gate <- struct{}{}
	await(t, done)
	if overlap.Load() != 0 {
		t.Fatal("multiple workers on one connection")
	}
	for i, n := range got {
		if i != n {
			t.Fatalf("FIFO[%d]=%d", i, n)
		}
	}
	if len(got) != 5000 {
		t.Fatalf("executed=%d", len(got))
	}
}

func TestConcurrentQueueSubmissions(t *testing.T) {
	p := New(time.Minute)
	defer p.Close()
	var q Queue
	var active, overlap, count atomic.Int64
	var submit, executed sync.WaitGroup
	submit.Add(16)
	executed.Add(1600)
	for range 16 {
		go func() {
			defer submit.Done()
			for range 100 {
				if !q.Submit(p, func() {
					if active.Add(1) != 1 {
						overlap.Add(1)
					}
					count.Add(1)
					active.Add(-1)
					executed.Done()
				}) {
					t.Error("task rejected")
					executed.Done()
				}
			}
		}()
	}
	submit.Wait()
	executed.Wait()
	if count.Load() != 1600 || overlap.Load() != 0 {
		t.Fatalf("count=%d overlap=%d", count.Load(), overlap.Load())
	}
}

func TestBlockingTaskDoesNotStall(t *testing.T) {
	p := New(time.Minute)
	defer p.Close()
	gate := make(chan struct{})
	defer close(gate)
	started, done := make(chan struct{}), make(chan struct{})
	if !p.Submit(func() { close(started); <-gate }) {
		t.Fatal("blocking task rejected")
	}
	await(t, started)
	if !p.Submit(func() { close(done) }) {
		t.Fatal("unrelated task rejected")
	}
	await(t, done)
}

func TestIdleWorkerExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(time.Second)
		defer p.Close()
		done := make(chan struct{})
		if !p.Submit(func() { close(done) }) {
			t.Fatal("task rejected")
		}
		<-done
		synctest.Wait()
		if p.Workers() != 1 {
			t.Fatalf("workers=%d", p.Workers())
		}
		time.Sleep(2 * time.Second) // Time/expiry is the behavior; virtual clock.
		synctest.Wait()
		if p.Workers() != 0 {
			t.Fatalf("idle worker retained: %d", p.Workers())
		}
	})
}

func TestCloseDrainsAcceptedBatch(t *testing.T) {
	p := New(time.Minute)
	var count atomic.Int64
	tasks := make([]func(), 100)
	for i := range tasks {
		tasks[i] = func() { count.Add(1) }
	}
	if !p.SubmitBatch(tasks) {
		t.Fatal("batch rejected")
	}
	p.Close()
	if count.Load() != 100 {
		t.Fatalf("drained=%d", count.Load())
	}
	if p.Submit(func() { t.Error("closed pool executed task") }) {
		t.Fatal("closed pool accepted task")
	}
}
