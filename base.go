package msghub

import (
	"context"
	"errors"
	"time"
)

var (
	ErrorHubClosed     = errors.New("msg hub already closed")
	ErrorNotSubscribed = errors.New("handler not subscribed in msg hub")
)

type MessageHandler func(message *Message)

type LoginHandler func(ctx context.Context, msg string, args ...any)

type Message struct {
	Ctx      context.Context
	Resource string
	Event    string
	Trigger  string
	Retry    int
	Payload  any

	index    int
	priority time.Duration
	at       *time.Time
}

func (m *Message) ExecuteAtUnix() int64 {
	if m.at == nil {
		return 0
	}
	return m.at.Unix()
}

func (m *Message) ExecuteAtMilli() int64 {
	if m.at == nil {
		return 0
	}
	return m.at.UnixMilli()
}

func (m *Message) Clone() *Message {
	n := *m
	n.index = 0
	n.priority = 0
	n.at = nil
	return &n
}
