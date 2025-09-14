package core

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

type Processor func(ctx context.Context, t *Task) error

type WorkerPool struct {
	ready     <-chan *Task
	store     *MemStore
	retry     *RetryManager
	workers   int
	wg        sync.WaitGroup
	processor Processor

	backoffBase time.Duration
	backoffMax  time.Duration
	rnd         func() float64
}

func NewWorkerPool(ready <-chan *Task, store *MemStore, retry *RetryManager, workers int, processor Processor,
	backoffBase, backoffMax time.Duration, rnd func() float64,
) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if processor == nil {
		processor = defaultProcessor
	}
	if rnd == nil {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		rnd = r.Float64
	}
	return &WorkerPool{
		ready:       ready,
		store:       store,
		retry:       retry,
		workers:     workers,
		processor:   processor,
		backoffBase: backoffBase,
		backoffMax:  backoffMax,
		rnd:         rnd,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return

				case t, ok := <-wp.ready:
					if !ok {
						return
					}

					wp.store.SetStatus(t.ID, StatusRunning)

					err := wp.processor(ctx, t)
					if err != nil && t.Attempt < t.MaxRetries {
						t.Attempt++
						delay := ExpJitter(t.Attempt, wp.backoffBase, wp.backoffMax, wp.rnd)
						t.NextRunAt = time.Now().Add(delay)
						wp.store.SetStatus(t.ID, StatusQueued)
						wp.retry.Schedule(t, t.NextRunAt)
					} else if err != nil {
						wp.store.SetStatus(t.ID, StatusFailed)
					} else {
						wp.store.SetStatus(t.ID, StatusDone)
					}

					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}()
	}
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// Заглушка-обработчик: 100–500мс, ~20% ошибка.
func defaultProcessor(ctx context.Context, t *Task) error {
	// имитация работы
	d := 100 + rand.Intn(401) // 100..500 ms
	select {
	case <-time.After(time.Duration(d) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if rand.Float64() < 0.2 {
		return errors.New("simulated error")
	}
	return nil
}
