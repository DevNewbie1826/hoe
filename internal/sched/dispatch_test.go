package sched

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchDispatchPreservesConnectionFIFO(t *testing.T) {
	p := New(time.Minute)
	defer p.Close()
	var queues [2]Queue
	var counts [2]atomic.Int64
	var active [2]atomic.Int64
	var overlap atomic.Int64
	tasks := make([]Task, 0, 2000)
	for i := range 1000 {
		for j := range queues {
			tasks = append(tasks, Task{Queue: &queues[j], Run: func() {
				if active[j].Add(1) != 1 {
					overlap.Add(1)
				}
				if got := counts[j].Add(1); got != int64(i+1) {
					t.Errorf("queue=%d order=%d want=%d", j, got, i+1)
				}
				active[j].Add(-1)
			}})
		}
	}
	if !p.Dispatch(tasks) {
		t.Fatal("batch rejected")
	}
	p.Close()
	if overlap.Load() != 0 || counts[0].Load() != 1000 || counts[1].Load() != 1000 {
		t.Fatalf("overlap=%d counts=%d/%d", overlap.Load(), counts[0].Load(), counts[1].Load())
	}
}
