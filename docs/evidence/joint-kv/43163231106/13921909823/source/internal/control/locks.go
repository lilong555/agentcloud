package control

import "sync"

// keyedMutex holds a key until its owner and all waiters have released it.
// Counting waiters prevents retirement from creating two locks for one identity.
type keyedMutex[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*keyLock
}
type keyLock struct {
	mu    sync.Mutex
	users int
}

func (m *keyedMutex[K]) lock(key K) func() {
	m.mu.Lock()
	if m.entries == nil {
		m.entries = make(map[K]*keyLock)
	}
	entry := m.entries[key]
	if entry == nil {
		entry = &keyLock{}
		m.entries[key] = entry
	}
	entry.users++
	m.mu.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		m.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(m.entries, key)
		}
		m.mu.Unlock()
	}
}
