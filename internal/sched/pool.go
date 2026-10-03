// Package sched dispatches elastic workers and serializes connection work.
package sched

import (
	"sync"
	"time"
)

type worker struct {
	work chan func()
	idle bool
}

// Pool runs unrelated tasks without a concurrency cap. Call Close to drain
// accepted tasks and release workers. Tasks must eventually return; panics
// propagate just as they do in ordinary goroutines.
type Pool struct {
	mu     sync.Mutex
	idle   []*worker
	expiry time.Duration
	closed bool
	count  int
	wg     sync.WaitGroup
}

// New creates a pool whose idle workers expire after idleTimeout.
// A nonpositive timeout selects one minute.
func New(idleTimeout time.Duration) *Pool {
	if idleTimeout <= 0 {
		idleTimeout = time.Minute
	}
	return &Pool{expiry: idleTimeout}
}

// Submit accepts a task unless the pool has closed.
func (p *Pool) Submit(task func()) bool {
	return p.SubmitBatch([]func(){task})
}

// SubmitBatch dispatches tasks under one lock. Each idle worker receives
// at most one coalesced wake; blocked tasks never impose a concurrency cap.
func (p *Pool) SubmitBatch(tasks []func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, task := range tasks {
		p.submitLocked(task)
	}
	return true
}

func (p *Pool) submitLocked(task func()) {
	if n := len(p.idle); n != 0 {
		w := p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
		w.idle = false
		w.work <- task
		return
	}
	w := &worker{work: make(chan func(), 1)}
	p.count++
	p.wg.Add(1)
	go p.run(w, task)
}

func (p *Pool) run(w *worker, task func()) {
	defer p.wg.Done()
	defer func() { p.mu.Lock(); p.count--; p.mu.Unlock() }()
	timer := time.NewTimer(p.expiry)
	defer timer.Stop()
	for {
		task()
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		w.idle = true
		p.idle = append(p.idle, w)
		p.mu.Unlock()
		timer.Reset(p.expiry)
		select {
		case next, ok := <-w.work:
			if !ok {
				return
			}
			timer.Stop()
			task = next
		case <-timer.C:
			p.mu.Lock()
			if w.idle {
				for i, idle := range p.idle {
					if idle == w {
						copy(p.idle[i:], p.idle[i+1:])
						p.idle[len(p.idle)-1] = nil
						p.idle = p.idle[:len(p.idle)-1]
						break
					}
				}
				w.idle = false
				p.mu.Unlock()
				return
			}
			// Submission or Close won the lock while the timer fired.
			p.mu.Unlock()
			next, ok := <-w.work
			if !ok {
				return
			}
			task = next
		}
	}
}

// Close rejects new submissions, drains accepted tasks, and joins workers.
// It must not be called by a task running in this pool.
func (p *Pool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for _, w := range p.idle {
			w.idle = false
			close(w.work)
		}
		clear(p.idle)
		p.idle = nil
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// Workers reports the current worker count.
func (p *Pool) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}
