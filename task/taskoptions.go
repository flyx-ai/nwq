package task

import (
	"math/rand/v2"
	"time"
)

type RetryPolicy struct {
	NumRetries int
	// Backoff is a function that takes the current attempt number (starting from 0) and returns the duration to wait before the next retry.
	Backoff func(attempt int) time.Duration
}

var DefaultRetryPolicy = RetryPolicy{
	NumRetries: 5,
	// Exponential backoff with jitter
	Backoff: func(attempt int) time.Duration {
		base := time.Second
		backoff := base * (1 << attempt) // Exponential backoff
		jitter := time.Duration(float64(backoff) * 0.1 * (0.5 - rand.Float64()))
		return backoff + jitter
	},
}

type taskOptions struct {
	retry                RetryPolicy
	timeout              time.Duration
	isWorkflowCompletion bool
	dedup                bool
}

type TaskOption interface {
	apply(*taskOptions)
}

type taskOptionRetry struct {
	retry RetryPolicy
}

func (o taskOptionRetry) apply(t *taskOptions) {
	t.retry = o.retry
}

func WithRetry(retry RetryPolicy) TaskOption {
	return taskOptionRetry{retry: retry}
}

type taskOptionTimeout struct {
	timeout time.Duration
}

func (o taskOptionTimeout) apply(t *taskOptions) {
	t.timeout = o.timeout
}

func WithTimeout(timeout time.Duration) TaskOption {
	return taskOptionTimeout{timeout: timeout}
}

type taskOptionWorkflowCompletion struct{}

func (o taskOptionWorkflowCompletion) apply(t *taskOptions) {
	t.isWorkflowCompletion = true
}

func withWorkflowCompletion() TaskOption {
	return taskOptionWorkflowCompletion{}
}

type taskOptionDedup struct{}

func (o taskOptionDedup) apply(t *taskOptions) {
	t.dedup = true
}

func WithDedup() TaskOption {
	return taskOptionDedup{}
}
