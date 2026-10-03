package sched

import "sync"

// Queue serializes tasks for one connection, in submission order.
// Its zero value is ready for use. All submissions must use the same pool.
type Queue struct {
	mu        sync.Mutex
	tasks     []func()
	head      int
	scheduled bool
}

// Submit accepts a task with exactly one scheduled worker per Queue.
// Submission to a closed pool fails even if this queue still has active work.
func (q *Queue) Submit(p *Pool, task func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	q.submitLocked(p, task)
	return true
}

func (q *Queue) submitLocked(p *Pool, task func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	if !q.scheduled {
		q.scheduled = true
		p.submitLocked(q.run)
	}
}

// Task pairs a callback with the connection queue that owns its ordering.
type Task struct {
	// Queue owns serialization and FIFO order for this callback.
	Queue *Queue
	// Run is the callback to execute.
	Run func()
}

// Dispatch hands an entire reactor batch to its queues under one pool lock.
// A queue already scheduled receives no additional worker wake.
func (p *Pool) Dispatch(tasks []Task) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, task := range tasks {
		task.Queue.submitLocked(p, task.Run)
	}
	return true
}

func (q *Queue) run() {
	for {
		q.mu.Lock()
		if q.head == len(q.tasks) {
			q.tasks = nil
			q.head = 0
			q.scheduled = false
			q.mu.Unlock()
			return
		}
		task := q.tasks[q.head]
		q.tasks[q.head] = nil
		q.head++
		q.mu.Unlock()
		task()
	}
}
