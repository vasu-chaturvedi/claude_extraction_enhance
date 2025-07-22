package main

import (
	"sync"
	"time"
)

// PerformanceMetrics tracks application performance statistics
type PerformanceMetrics struct {
	mu                    sync.RWMutex
	StartTime            time.Time
	TotalSOLsProcessed   int64
	TotalProceduresCalled int64
	TotalErrors          int64
	AverageResponseTime  time.Duration
	PeakConcurrency      int
	DbConnectionStats    DatabaseStats
	lastResponseTimes    []time.Duration
	responseTimeIndex    int
}

// DatabaseStats tracks database connection pool statistics
type DatabaseStats struct {
	MaxOpenConnections int
	OpenConnections    int
	InUse              int
	Idle               int
	WaitCount          int64
	WaitDuration       time.Duration
}

// NewPerformanceMetrics creates a new metrics tracker
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		StartTime:         time.Now(),
		lastResponseTimes: make([]time.Duration, 100), // Keep last 100 response times
	}
}

// RecordProcedureCall records a procedure execution
func (m *PerformanceMetrics) RecordProcedureCall(duration time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.TotalProceduresCalled++
	if !success {
		m.TotalErrors++
	}
	
	// Update rolling average of response times
	m.lastResponseTimes[m.responseTimeIndex] = duration
	m.responseTimeIndex = (m.responseTimeIndex + 1) % len(m.lastResponseTimes)
	
	// Calculate average response time
	var total time.Duration
	count := int64(0)
	for _, rt := range m.lastResponseTimes {
		if rt > 0 {
			total += rt
			count++
		}
	}
	if count > 0 {
		m.AverageResponseTime = total / time.Duration(count)
	}
}

// RecordSOLComplete records completion of a SOL
func (m *PerformanceMetrics) RecordSOLComplete() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalSOLsProcessed++
}

// UpdateConcurrency updates peak concurrency tracking
func (m *PerformanceMetrics) UpdateConcurrency(current int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current > m.PeakConcurrency {
		m.PeakConcurrency = current
	}
}

// UpdateDbStats updates database connection statistics
func (m *PerformanceMetrics) UpdateDbStats(stats DatabaseStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DbConnectionStats = stats
}

// GetMetrics returns current performance metrics (thread-safe)
func (m *PerformanceMetrics) GetMetrics() (int64, int64, int64, time.Duration, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.TotalSOLsProcessed, m.TotalProceduresCalled, m.TotalErrors, m.AverageResponseTime, m.PeakConcurrency
}

// GetThroughput calculates current throughput (SOLs/minute)
func (m *PerformanceMetrics) GetThroughput() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	elapsed := time.Since(m.StartTime)
	if elapsed.Minutes() == 0 {
		return 0
	}
	return float64(m.TotalSOLsProcessed) / elapsed.Minutes()
}

// GetErrorRate calculates current error rate as percentage
func (m *PerformanceMetrics) GetErrorRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.TotalProceduresCalled == 0 {
		return 0
	}
	return float64(m.TotalErrors) / float64(m.TotalProceduresCalled) * 100
}