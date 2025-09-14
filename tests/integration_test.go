package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogazub/app/internal/api"
	"github.com/gogazub/app/internal/core"
)

// -------------------- test app builder --------------------

type testApp struct {
	srv    *httptest.Server
	store  *core.MemStore
	cancel context.CancelFunc
	wp     *core.WorkerPool
	ready  chan *core.Task
	retry  *core.RetryManager
}

type appOpts struct {
	workers       int
	queueCap      int
	backoffBase   time.Duration
	backoffMax    time.Duration
	rnd           func() float64
	processor     core.Processor
	startWorkers  bool // можно блокировать очередь, не начиная воркеров
	startRetryMgr bool
}

func newTestApp(t *testing.T, opts appOpts) *testApp {
	t.Helper()

	if opts.workers <= 0 {
		opts.workers = 1
	}
	if opts.queueCap <= 0 {
		opts.queueCap = 16
	}
	if opts.backoffBase == 0 {
		opts.backoffBase = 50 * time.Millisecond
	}
	if opts.backoffMax == 0 {
		opts.backoffMax = 500 * time.Millisecond
	}

	store := core.NewMemStore()
	ready := make(chan *core.Task, opts.queueCap)
	retry := core.NewRetryManager(ready)

	ctx, cancel := context.WithCancel(context.Background())

	if opts.startRetryMgr {
		go retry.Start(ctx)
	}

	wp := core.NewWorkerPool(
		ready,
		store,
		retry,
		opts.workers,
		opts.processor,
		opts.backoffBase,
		opts.backoffMax,
		opts.rnd,
	)
	if opts.startWorkers {
		wp.Start(ctx)
	}

	h := &api.Handlers{Store: store, ReadyOut: ready}
	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", h.Enqueue)
	mux.HandleFunc("/healthz", h.Healthz)

	srv := httptest.NewServer(mux)

	return &testApp{
		srv:    srv,
		store:  store,
		cancel: cancel,
		wp:     wp,
		ready:  ready,
		retry:  retry,
	}
}

func (a *testApp) close() {
	// Отключаем HTTP (перестаём принимать новые)
	a.srv.Close()
	// Останавливаем фоновые компоненты
	a.cancel()
	// Ждём завершения текущих задач
	a.wp.Wait()
}

// -------------------- helpers --------------------

func httpEnqueue(t *testing.T, baseURL, id, payload string, maxRetries int) *http.Response {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"id":          id,
		"payload":     payload,
		"max_retries": maxRetries,
	})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/enqueue", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func waitStatus(t *testing.T, store *core.MemStore, id string, want core.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if st, ok := store.GetStatus(id); ok && st == want {
			return
		}
		if time.Now().After(deadline) {
			st, _ := store.GetStatus(id)
			t.Fatalf("waitStatus timeout: id=%s got=%s want=%s", id, st, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// processor, который считает вызовы по id и позволяет задавать, сколько раз упасть
type countingProc struct {
	mu        sync.Mutex
	calls     map[string]*int32
	failTimes map[string]int // сколько первых вызовов для id должны упасть
	startCh   map[string]chan struct{}
	sleep     time.Duration
}

func newCountingProc(failTimes map[string]int, startCh map[string]chan struct{}, sleep time.Duration) *countingProc {
	return &countingProc{
		calls:     make(map[string]*int32),
		failTimes: failTimes,
		startCh:   startCh,
		sleep:     sleep,
	}
}

func (p *countingProc) getCounter(id string) *int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.calls[id]; ok {
		return c
	}
	var zero int32
	p.calls[id] = &zero
	return p.calls[id]
}

func (p *countingProc) Calls(id string) int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.calls[id]; ok {
		return atomic.LoadInt32(c)
	}
	return 0
}
func (p *countingProc) Proc(ctx context.Context, t *core.Task) error {
	// сигнал о старте
	if ch, ok := p.startCh[t.ID]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	// ВАЖНО: игнорируем ctx.Done() здесь — текущая задача должна доделаться
	if p.sleep > 0 {
		time.Sleep(p.sleep)
	}

	c := p.getCounter(t.ID)
	n := atomic.AddInt32(c, 1) // текущий вызов (1..)

	if k, ok := p.failTimes[t.ID]; ok && int(n) <= k {
		return context.DeadlineExceeded // любая ошибка
	}
	return nil
}

// -------------------- tests --------------------

func TestHealthzIntegration(t *testing.T) {
	app := newTestApp(t, appOpts{
		workers:       2,
		queueCap:      8,
		backoffBase:   20 * time.Millisecond,
		backoffMax:    200 * time.Millisecond,
		rnd:           func() float64 { return 0.5 },
		processor:     nil, // default OK
		startWorkers:  true,
		startRetryMgr: true,
	})
	defer app.close()

	resp, err := http.Get(app.srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d want=200", resp.StatusCode)
	}
}

func TestEnqueueAndProcessSuccess(t *testing.T) {
	app := newTestApp(t, appOpts{
		workers:       3,
		queueCap:      32,
		backoffBase:   10 * time.Millisecond,
		backoffMax:    100 * time.Millisecond,
		rnd:           func() float64 { return 0.3 },
		processor:     nil, // default ~20% ошибок, но нам нужна 100% успешность → лучше фиксированный
		startWorkers:  true,
		startRetryMgr: true,
	})
	defer app.close()

	// Перекроем default-процессор «в момент» — используем countingProc без падений
	proc := newCountingProc(map[string]int{}, nil, 10*time.Millisecond)
	app.cancel() // остановим старый контекст
	ctx, cancel := context.WithCancel(context.Background())
	app.cancel = cancel
	go app.retry.Start(ctx)
	app.wp = core.NewWorkerPool(app.ready, app.store, app.retry, 3, proc.Proc, 10*time.Millisecond, 100*time.Millisecond, func() float64 { return 0.3 })
	app.wp.Start(ctx)

	ids := []string{"a1", "a2", "a3", "a4", "a5"}
	for _, id := range ids {
		resp := httpEnqueue(t, app.srv.URL, id, "data", 0)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("enqueue %s status=%d want=202", id, resp.StatusCode)
		}
	}
	for _, id := range ids {
		waitStatus(t, app.store, id, core.StatusDone, 2*time.Second)
		if got := proc.Calls(id); got != 1 {
			t.Fatalf("task %s calls=%d want=1", id, got)
		}
	}
}

func TestRetryThenSuccess(t *testing.T) {
	// id=t1: упасть 2 раза, потом успех; base=10ms, rnd=0 → задержки 10ms, 20ms (пример экспоненты)
	startCh := map[string]chan struct{}{"t1": make(chan struct{}, 1)}
	proc := newCountingProc(map[string]int{"t1": 2}, startCh, 5*time.Millisecond)

	app := newTestApp(t, appOpts{
		workers:       1,
		queueCap:      8,
		backoffBase:   10 * time.Millisecond,
		backoffMax:    50 * time.Millisecond,
		rnd:           func() float64 { return 1.0 }, // full jitter → равен cap, но это ок
		processor:     proc.Proc,
		startWorkers:  true,
		startRetryMgr: true,
	})
	defer app.close()

	resp := httpEnqueue(t, app.srv.URL, "t1", "x", 3)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("enqueue status=%d want=202", resp.StatusCode)
	}

	// Ждём финального успеха
	waitStatus(t, app.store, "t1", core.StatusDone, 3*time.Second)

	// Должно быть 3 вызова: 2 фейла + 1 успех
	if got := proc.Calls("t1"); got != 3 {
		t.Fatalf("calls=%d want=3", got)
	}
}

func TestRetryExhaustedToFailed(t *testing.T) {
	// всегда падать; max_retries=2 → 3 вызова и финальный failed
	proc := newCountingProc(map[string]int{"t2": 100}, nil, 5*time.Millisecond)

	app := newTestApp(t, appOpts{
		workers:       1,
		queueCap:      8,
		backoffBase:   10 * time.Millisecond,
		backoffMax:    50 * time.Millisecond,
		rnd:           func() float64 { return 0.0 }, // минимальный джиттер
		processor:     proc.Proc,
		startWorkers:  true,
		startRetryMgr: true,
	})
	defer app.close()

	resp := httpEnqueue(t, app.srv.URL, "t2", "y", 2)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("enqueue status=%d want=202", resp.StatusCode)
	}
	waitStatus(t, app.store, "t2", core.StatusFailed, 3*time.Second)

	if got := proc.Calls("t2"); got != 3 {
		t.Fatalf("calls=%d want=3", got)
	}
}

func TestQueueFullReturns503(t *testing.T) {
	// Не запускаем воркеров и retry manager, очередь остаётся забитой
	app := newTestApp(t, appOpts{
		workers:       1, // не важно, мы их не стартуем
		queueCap:      1,
		backoffBase:   10 * time.Millisecond,
		backoffMax:    50 * time.Millisecond,
		startWorkers:  false,
		startRetryMgr: false,
	})
	defer app.close()

	// Первый запрос занимает единственный слот
	resp1 := httpEnqueue(t, app.srv.URL, "q1", "data", 0)
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first enqueue status=%d want=202", resp1.StatusCode)
	}
	// Второй должен вернуть 503 (non-blocking send в handler)
	resp2 := httpEnqueue(t, app.srv.URL, "q2", "data", 0)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second enqueue status=%d want=503", resp2.StatusCode)
	}
	// И статус второй задачи не должен оставаться queued (handler откатывает до failed)
	if st, _ := app.store.GetStatus("q2"); st != core.StatusFailed {
		t.Fatalf("q2 status=%s want=failed", st)
	}
}

func TestGracefulShutdownWaitsRunningOnly(t *testing.T) {
	// Первая задача "tA" начнёт выполняться и будет работать 120ms.
	// Сразу после старта делаем shutdown: текущая доделается, вторая "tB" останется queued.
	startA := make(chan struct{}, 1)
	proc := newCountingProc(map[string]int{}, map[string]chan struct{}{"tA": startA}, 120*time.Millisecond)

	app := newTestApp(t, appOpts{
		workers:       1,
		queueCap:      8,
		backoffBase:   10 * time.Millisecond,
		backoffMax:    50 * time.Millisecond,
		processor:     proc.Proc,
		startWorkers:  true,
		startRetryMgr: true,
	})
	defer app.close()

	resp := httpEnqueue(t, app.srv.URL, "tA", "p", 0)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("enqueue tA status=%d want=202", resp.StatusCode)
	}
	// ждём фактического старта tA
	select {
	case <-startA:
	case <-time.After(2 * time.Second):
		t.Fatal("tA did not start in time")
	}

	// tB успеет попасть в очередь до shutdown
	respB := httpEnqueue(t, app.srv.URL, "tB", "p", 0)
	respB.Body.Close()
	if respB.StatusCode != http.StatusAccepted {
		t.Fatalf("enqueue tB status=%d want=202", respB.StatusCode)
	}

	// Инициируем graceful shutdown
	app.srv.Close()
	app.cancel()
	app.wp.Wait()

	// tA завершилась
	waitStatus(t, app.store, "tA", core.StatusDone, 2*time.Second)

	// tB должна остаться queued (воркеры остановлены до её старта)
	st, _ := app.store.GetStatus("tB")
	if st != core.StatusQueued {
		t.Fatalf("tB status=%s want=queued", st)
	}
}
