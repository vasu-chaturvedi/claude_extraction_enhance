package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type ProcLog struct {
	SolID         string
	Procedure     string
	StartTime     time.Time
	EndTime       time.Time
	ExecutionTime time.Duration
	Status        string
	ErrorDetails  string
}

type ColumnConfig struct {
	Name   string
	Length int
	Align  string
}

type ProcSummary struct {
	Procedure string
	StartTime time.Time
	EndTime   time.Time
	Status    string
}

// Performance metrics for monitoring
type PerformanceMetrics struct {
	TotalQueries       int64
	TotalQueryTime     int64 // nanoseconds
	CacheHits          int64
	CacheMisses        int64
	TotalRowsProcessed int64
	TotalBytesWritten  int64
	mu                 sync.RWMutex
	QueryTimes         []time.Duration
	SlowQueries        int64 // queries > 1 second
}

func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		QueryTimes: make([]time.Duration, 0, 1000),
	}
}

func (pm *PerformanceMetrics) RecordQuery(duration time.Duration, rowCount int64, bytesWritten int64) {
	atomic.AddInt64(&pm.TotalQueries, 1)
	atomic.AddInt64(&pm.TotalQueryTime, int64(duration))
	atomic.AddInt64(&pm.TotalRowsProcessed, rowCount)
	atomic.AddInt64(&pm.TotalBytesWritten, bytesWritten)

	if duration > time.Second {
		atomic.AddInt64(&pm.SlowQueries, 1)
	}

	pm.mu.Lock()
	if len(pm.QueryTimes) < cap(pm.QueryTimes) {
		pm.QueryTimes = append(pm.QueryTimes, duration)
	}
	pm.mu.Unlock()
}

func (pm *PerformanceMetrics) RecordCacheHit() {
	atomic.AddInt64(&pm.CacheHits, 1)
}

func (pm *PerformanceMetrics) RecordCacheMiss() {
	atomic.AddInt64(&pm.CacheMisses, 1)
}

func (pm *PerformanceMetrics) GetStats() (int64, time.Duration, float64, int64) {
	totalQueries := atomic.LoadInt64(&pm.TotalQueries)
	totalTime := atomic.LoadInt64(&pm.TotalQueryTime)
	cacheHits := atomic.LoadInt64(&pm.CacheHits)
	cacheMisses := atomic.LoadInt64(&pm.CacheMisses)
	slowQueries := atomic.LoadInt64(&pm.SlowQueries)

	var avgDuration time.Duration
	if totalQueries > 0 {
		avgDuration = time.Duration(totalTime / totalQueries)
	}

	var cacheHitRate float64
	if cacheHits+cacheMisses > 0 {
		cacheHitRate = float64(cacheHits) / float64(cacheHits+cacheMisses) * 100
	}

	return totalQueries, avgDuration, cacheHitRate, slowQueries
}

var globalMetrics = NewPerformanceMetrics()
