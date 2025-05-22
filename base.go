package msghub

import (
	"context"
	"errors"
	"time"
)

var (
	ErrorNotSubscribed = errors.New("handler not subscribed in msg hub")
)

type MessageHandler func(message *Message)

type LoggingHandler func(ctx context.Context, msg string, args ...any)

type Message struct {
	Ctx      context.Context
	Resource string
	Event    string
	Trigger  string
	Retry    int
	Payload  any

	index    int
	priority time.Duration

	delay   int64 // 延迟时间
	enqueue int64 // 入列时间
	at      *time.Time
}

// Offset 执行时间偏移预期的秒数
func (m *Message) Offset() int64 {
	if m.at == nil {
		return 0
	}
	return m.enqueue + m.delay - m.at.Unix()
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
	n.enqueue = 0
	n.priority = 0
	n.at = nil
	return &n
}
