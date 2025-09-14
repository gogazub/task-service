package api

import (
	"encoding/json"
	"net/http"

	"github.com/gogazub/app/internal/core"
)

type Handlers struct {
	Store    *core.MemStore
	ReadyOut chan<- *core.Task
}

func (h *Handlers) Enqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.MaxRetries < 0 {
		http.Error(w, "invalid id or max_retries", http.StatusBadRequest)
		return
	}
	created := h.Store.CreateIfAbsent(req.ID, core.StatusQueued)
	if !created {
		http.Error(w, "id already exists", http.StatusConflict)
		return
	}

	task := &core.Task{
		ID:         req.ID,
		Payload:    req.Payload,
		MaxRetries: req.MaxRetries,
	}
	// не блокируемся: если очередь переполнена — 503
	select {
	case h.ReadyOut <- task:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(EnqueueResponse{ID: req.ID})
	default:
		// откатываем статус, чтобы не висел "queued" без шанса на обработку
		h.Store.SetStatus(req.ID, core.StatusFailed)
		http.Error(w, "queue full", http.StatusServiceUnavailable)
	}
}

func (h *Handlers) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
