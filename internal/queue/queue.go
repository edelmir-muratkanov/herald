package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Priority int8

const (
	PriorityHigh Priority = iota
	PriorityMedium
	PriorityLow
)

type Message struct {
	ID       uuid.UUID
	Priority Priority
	Channel  string
	Attempt  int
	Payload  []byte
	Headers  map[string]string
}

type Kind uint8

const (
	kindInvalid Kind = iota
	KindAck
	KindRetry
	KindDead
)

type Result struct {
	Kind   Kind
	After  *time.Duration
	Reason string
}

func Ack() Result {
	return Result{Kind: KindAck}
}

func Retry(after time.Duration, reason string) Result {
	return Result{Kind: KindRetry, After: &after, Reason: reason}
}

func Dead(reason string) Result {
	return Result{Kind: KindDead, Reason: reason}
}

type Producer interface {
	Enqueue(ctx context.Context, tx pgx.Tx, msgs ...Message) error
}

type Consumer interface {
	Run(ctx context.Context, h Handler) Result
}

type Handler func(ctx context.Context, msg Message) Result
