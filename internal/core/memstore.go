package core

import (
	"sync"
)

type MemStore struct {
	mu     sync.RWMutex
	status map[string]Status
}

func NewMemStore() *MemStore {
	return &MemStore{status: make(map[string]Status)}
}

// CreateIfAbsent ставит статус для нового id. Возвращает true, если создали.
func (s *MemStore) CreateIfAbsent(id string, st Status) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.status[id]; ok {
		return false
	}
	s.status[id] = st
	return true
}

func (s *MemStore) SetStatus(id string, st Status) {
	s.mu.Lock()
	s.status[id] = st
	s.mu.Unlock()
}

func (s *MemStore) GetStatus(id string) (Status, bool) {
	s.mu.RLock()
	st, ok := s.status[id]
	s.mu.RUnlock()
	return st, ok
}
