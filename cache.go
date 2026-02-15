package main

import (
	"database/sql"
	"fmt"
	"sync"
)

// Generic thread-safe cache for any key-value pairs
type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]V
}

// NewCache creates a new generic cache
func NewCache[K comparable, V any]() *Cache[K, V] {
	return &Cache[K, V]{
		items: make(map[K]V),
	}
}

// Get retrieves a value from the cache
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, exists := c.items[key]
	return value, exists
}

// Set stores a value in the cache
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
}

// Delete removes a value from the cache
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// Len returns the number of items in the cache
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all items from the cache
func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[K]V)
}

// GetOrSet retrieves a value or sets it if it doesn't exist
func (c *Cache[K, V]) GetOrSet(key K, factory func() (V, error)) (V, error) {
	// Try to get existing value first
	if value, exists := c.Get(key); exists {
		return value, nil
	}

	// Create new value under write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check pattern to avoid race condition
	if value, exists := c.items[key]; exists {
		return value, nil
	}

	value, err := factory()
	if err != nil {
		var zero V
		return zero, err
	}

	c.items[key] = value
	return value, nil
}

// PreparedStmtCache is a specialized cache for SQL prepared statements
type PreparedStmtCache struct {
	cache *Cache[string, *sql.Stmt]
}

// NewPreparedStmtCache creates a new prepared statement cache
func NewPreparedStmtCache() *PreparedStmtCache {
	return &PreparedStmtCache{
		cache: NewCache[string, *sql.Stmt](),
	}
}

// GetOrPrepare retrieves or prepares a statement
func (c *PreparedStmtCache) GetOrPrepare(db *sql.DB, query string) (*sql.Stmt, error) {
	return c.cache.GetOrSet(query, func() (*sql.Stmt, error) {
		globalMetrics.RecordCacheMiss()
		stmt, err := db.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare statement: %w", err)
		}
		return stmt, nil
	})
}

// Get retrieves a prepared statement from cache
func (c *PreparedStmtCache) Get(query string) (*sql.Stmt, bool) {
	stmt, exists := c.cache.Get(query)
	if exists {
		globalMetrics.RecordCacheHit()
	}
	return stmt, exists
}

// Close closes all prepared statements and clears the cache
func (c *PreparedStmtCache) Close() {
	c.cache.mu.Lock()
	defer c.cache.mu.Unlock()

	for _, stmt := range c.cache.items {
		if stmt != nil {
			stmt.Close()
		}
	}
	c.cache.items = make(map[string]*sql.Stmt)
}

// Len returns the number of cached statements
func (c *PreparedStmtCache) Len() int {
	return c.cache.Len()
}
