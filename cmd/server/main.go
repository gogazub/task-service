package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gogazub/task-service/internal/api"
	"github.com/gogazub/task-service/internal/core"
	"github.com/gogazub/task-service/internal/util"
)

func main() {
	// Конфиг
	workers := util.GetInt("WORKERS", 4)
	queueCap := util.GetInt("QUEUE_SIZE", 64)
	baseMS := util.GetInt("BACKOFF_BASE_MS", 100)
	maxMS := util.GetInt("BACKOFF_MAX_MS", 5000)

	// Компоненты
	store := core.NewMemStore()
	ready := make(chan *core.Task, queueCap)
	retry := core.NewRetryManager(ready)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Старт RetryManager и пула воркеров
	go retry.Start(ctx)
	wp := core.NewWorkerPool(
		ready,
		store,
		retry,
		workers,
		nil, // default processor
		time.Duration(baseMS)*time.Millisecond,
		time.Duration(maxMS)*time.Millisecond,
		nil, // default rnd
	)
	wp.Start(ctx)

	// HTTP
	h := &api.Handlers{Store: store, ReadyOut: ready}
	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", h.Enqueue)
	mux.HandleFunc("/healthz", h.Healthz)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Запуск сервера
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Ожидаем SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill) // в *nix вместо os.Kill используйте syscall.SIGTERM
	<-sigCh

	// Graceful shutdown:
	// 1) закрываем вход HTTP
	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	_ = srv.Shutdown(shCtx)

	// 2) останавливаем фоновые компоненты: перестаём читать ready/расписание ретраев
	cancel()

	// 3) ждём завершения только текущих задач (очередь не вырабатываем)
	wp.Wait()

	log.Print("shutdown complete")
}
