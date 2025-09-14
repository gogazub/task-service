package core

import (
	"container/heap"
	"context"
	"time"
)

type retryItem struct {
	runAt time.Time // когда задача должна снова пойти в работу
	seq   int64     // порядковый номер для устойчивого порядка при равных runAt
	task  *Task     // сама задача
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
	out     chan<- *Task   // Сюда отправляем задачу, у которой пришло время
	in      chan retryItem // Основная очередь тасок
	seq     int64          // Порядковый номер
	timer   *time.Timer    // Один общий таймер, всегда сбрасывается на ближайший runAt
	pending retryHeap      // min-heap ожидающих тасок
}

func NewRetryManager(out chan<- *Task) *RetryManager {
	return &RetryManager{
		out:   out,
		in:    make(chan retryItem, 64),
		timer: time.NewTimer(time.Hour), // будет немедленно переставлен
	}
}

// Start запускает главный цикл RetryManager.
// Менеджер хранит отложенные задачи в min-heap (по ближайшему runAt) и держит
// ровно один time.Timer, всегда выставленный на дедлайн головы кучи.
// Когда таймер срабатывает, ближайшая задача перекладывается в канал m.out.
// Завершение - по ctx.Done(): таймер останавливается, цикл выходит.
func (m *RetryManager) Start(ctx context.Context) {
	heap.Init(&m.pending)

	// Таймер был создан в конструкторе. Сразу же остановим его, чтобы не ловить
	// случайное стартовое срабатывание. Далее мы будем управлять им вручную.
	_ = m.timer.Stop()

	for {

		var nextDeadline <-chan time.Time

		if len(m.pending) > 0 {
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

// Schedule планирует задачу t на выполнение не раньше времени at.
// Внутри формируется retryItem и отправляется во внутренний канал m.in,
// откуда его подхватит Start и положит в кучу.
func (m *RetryManager) Schedule(t *Task, at time.Time) {
	m.seq++
	m.in <- retryItem{runAt: at, seq: m.seq, task: t}
}

// resetTimer корректно перевзводит один и тот же time.Timer на новую задержку d.
func (m *RetryManager) resetTimer(d time.Duration) {
	if !m.timer.Stop() {
		select {
		case <-m.timer.C:
		default:
		}
	}
	m.timer.Reset(d)
}
