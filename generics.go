package main

import (
	"fmt"
	"time"
)

// Record represents a generic database record with type-safe column access
type Record[T any] map[string]T

// Get safely retrieves a value from the record with type assertion
func (r Record[T]) Get(key string) (T, bool) {
	value, exists := r[key]
	return value, exists
}

// Set safely sets a value in the record
func (r Record[T]) Set(key string, value T) {
	r[key] = value
}

// Keys returns all keys in the record
func (r Record[T]) Keys() []string {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	return keys
}

// Values returns all values in the record
func (r Record[T]) Values() []T {
	values := make([]T, 0, len(r))
	for _, v := range r {
		values = append(values, v)
	}
	return values
}

// DatabaseRecord represents a type-safe database record
type DatabaseRecord = Record[any]

// NewDatabaseRecord creates a new database record
func NewDatabaseRecord() DatabaseRecord {
	return make(DatabaseRecord)
}

// Result represents a generic operation result with error handling
type Result[T any] struct {
	Value T
	Error error
}

// IsSuccess checks if the result represents a successful operation
func (r Result[T]) IsSuccess() bool {
	return r.Error == nil
}

// IsError checks if the result represents a failed operation
func (r Result[T]) IsError() bool {
	return r.Error != nil
}

// Unwrap returns the value if successful, panics if error
func (r Result[T]) Unwrap() T {
	if r.Error != nil {
		panic(fmt.Sprintf("attempted to unwrap error result: %v", r.Error))
	}
	return r.Value
}

// UnwrapOr returns the value if successful, or the provided default
func (r Result[T]) UnwrapOr(defaultValue T) T {
	if r.Error != nil {
		return defaultValue
	}
	return r.Value
}

// Map applies a function to the result value if successful
func Map[T, U any](r Result[T], fn func(T) U) Result[U] {
	if r.Error != nil {
		return Result[U]{Error: r.Error}
	}
	return Result[U]{Value: fn(r.Value), Error: nil}
}

// FlatMap applies a function that returns a Result to the result value if successful
func FlatMap[T, U any](r Result[T], fn func(T) Result[U]) Result[U] {
	if r.Error != nil {
		return Result[U]{Error: r.Error}
	}
	return fn(r.Value)
}

// Success creates a successful result
func Success[T any](value T) Result[T] {
	return Result[T]{Value: value, Error: nil}
}

// Error creates a failed result
func Error[T any](err error) Result[T] {
	var zero T
	return Result[T]{Value: zero, Error: err}
}

// Optional represents a value that may or may not be present
type Optional[T any] struct {
	value   T
	present bool
}

// Some creates an Optional with a value
func Some[T any](value T) Optional[T] {
	return Optional[T]{value: value, present: true}
}

// None creates an empty Optional
func None[T any]() Optional[T] {
	return Optional[T]{present: false}
}

// IsPresent checks if the Optional contains a value
func (o Optional[T]) IsPresent() bool {
	return o.present
}

// IsEmpty checks if the Optional is empty
func (o Optional[T]) IsEmpty() bool {
	return !o.present
}

// Get returns the value if present, panics if empty
func (o Optional[T]) Get() T {
	if !o.present {
		panic("attempted to get value from empty Optional")
	}
	return o.value
}

// GetOr returns the value if present, or the provided default
func (o Optional[T]) GetOr(defaultValue T) T {
	if !o.present {
		return defaultValue
	}
	return o.value
}

// MapOptional applies a function to the Optional value if present
func MapOptional[T, U any](o Optional[T], fn func(T) U) Optional[U] {
	if !o.present {
		return None[U]()
	}
	return Some(fn(o.value))
}

// FilterOptional returns the Optional if the predicate is true, otherwise None
func FilterOptional[T any](o Optional[T], predicate func(T) bool) Optional[T] {
	if !o.present || !predicate(o.value) {
		return None[T]()
	}
	return o
}

// TimedResult represents a result with execution timing information
type TimedResult[T any] struct {
	Result[T]
	Duration  time.Duration
	StartTime time.Time
	EndTime   time.Time
}

// NewTimedResult creates a new timed result
func NewTimedResult[T any](value T, err error, start, end time.Time) TimedResult[T] {
	return TimedResult[T]{
		Result:    Result[T]{Value: value, Error: err},
		Duration:  end.Sub(start),
		StartTime: start,
		EndTime:   end,
	}
}

// TimeOperation executes a function and returns a timed result
func TimeOperation[T any](fn func() (T, error)) TimedResult[T] {
	start := time.Now()
	value, err := fn()
	end := time.Now()
	return NewTimedResult(value, err, start, end)
}

// Batch represents a collection of items to be processed together
type Batch[T any] struct {
	Items    []T
	Size     int
	Metadata map[string]any
}

// NewBatch creates a new batch with specified size
func NewBatch[T any](size int) *Batch[T] {
	return &Batch[T]{
		Items:    make([]T, 0, size),
		Size:     size,
		Metadata: make(map[string]any),
	}
}

// Add adds an item to the batch
func (b *Batch[T]) Add(item T) bool {
	if len(b.Items) >= b.Size {
		return false // Batch is full
	}
	b.Items = append(b.Items, item)
	return true
}

// IsFull checks if the batch is at capacity
func (b *Batch[T]) IsFull() bool {
	return len(b.Items) >= b.Size
}

// IsEmpty checks if the batch is empty
func (b *Batch[T]) IsEmpty() bool {
	return len(b.Items) == 0
}

// Count returns the current number of items in the batch
func (b *Batch[T]) Count() int {
	return len(b.Items)
}

// Clear removes all items from the batch
func (b *Batch[T]) Clear() {
	b.Items = b.Items[:0]
}

// ProcessBatch processes items in batches with a generic processor function
func ProcessBatch[T, R any](items []T, batchSize int, processor func([]T) ([]R, error)) ([]R, error) {
	var results []R

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		batchResults, err := processor(batch)
		if err != nil {
			return nil, fmt.Errorf("batch processing failed at index %d: %w", i, err)
		}

		results = append(results, batchResults...)
	}

	return results, nil
}
