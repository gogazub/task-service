package core

import "time"

type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Task struct {
	ID         string
	Payload    string
	MaxRetries int

	Attempt   int
	NextRunAt time.Time
}
