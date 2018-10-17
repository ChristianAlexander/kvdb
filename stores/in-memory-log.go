package stores

import (
	"context"
	"fmt"
	"sync"
)

type inMemoryStore struct {
	mu     sync.RWMutex
	values map[string]string
}

func NewInMemoryStore() Store {
	return &inMemoryStore{
		values: make(map[string]string),
	}
}

func (s *inMemoryStore) Set(ctx context.Context, key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values[key] = value

	return nil
}

func (s *inMemoryStore) Get(ctx context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.values[key]
	if !ok {
		return "", fmt.Errorf("value for key '%s' not found", key)
	}

	return v, nil
}

func (s *inMemoryStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.values, key)

	return nil
}

func (s *inMemoryStore) Keys(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for k := range s.values {
		result = append(result, k)
	}

	return result, nil
}
