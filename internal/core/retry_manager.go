package core

import (
	"container/heap"
	"context"
	"time"
)

type retryItem struct {
	runAt time.Time
	seq   int64
	task  *Task
}

type retryHeap []retryItem

func (h retryHeap) Len() int { return len(h) }
func (h retryHeap) Less(i, j int) bool {
	if h[i].runAt.Equal(h[j].runAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].runAt.Before(h[j].runAt)
}
func (h retryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *retryHeap) Push(x interface{}) { *h = append(*h, x.(retryItem)) }
func (h *retryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type RetryManager struct {
	out     chan<- *Task
	in      chan retryItem
	seq     int64
	timer   *time.Timer
	pending retryHeap
}

func NewRetryManager(out chan<- *Task) *RetryManager {
	return &RetryManager{
		out:   out,
		in:    make(chan retryItem, 64),
		timer: time.NewTimer(time.Hour), // будет немедленно переставлен
	}
}

func (m *RetryManager) Start(ctx context.Context) {
	heap.Init(&m.pending)
	_ = m.timer.Stop() // таймер неактивен
	for {
		var nextDeadline <-chan time.Time
		if len(m.pending) > 0 {
			//now := time.Now()
			head := m.pending[0].runAt
			d := time.Until(head)
			if d < 0 {
				d = 0
			}
			m.resetTimer(d)
			nextDeadline = m.timer.C
		}

		select {
		case <-ctx.Done():
			_ = m.timer.Stop()
			return

		case it := <-m.in:
			heap.Push(&m.pending, it)

		case <-nextDeadline:
			if len(m.pending) == 0 {
				continue
			}
			it := heap.Pop(&m.pending).(retryItem)
			select {
			case m.out <- it.task:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *RetryManager) Schedule(t *Task, at time.Time) {
	m.seq++
	m.in <- retryItem{runAt: at, seq: m.seq, task: t}
}

func (m *RetryManager) resetTimer(d time.Duration) {
	if !m.timer.Stop() {
		select {
		case <-m.timer.C:
		default:
		}
	}
	m.timer.Reset(d)
}
