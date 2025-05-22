package msghub

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	EventAll = "*"
	MaxDelay = 1200 // 最大延迟1200s
)

type MessageBus struct {
	callbacks map[string][]MessageHandler
	eq        chan *Message
	dq        chan *Message
	fork      chan bool
	log       LoginHandler
	queue     *int32 // 延迟队列处理协程

	deadLetter *int32 // 死信线程数量

	signal context.Context
	cancel context.CancelFunc
}

// Subscribe 关注消息
func (m *MessageBus) Subscribe(resource string, event string, callback MessageHandler) {
	if event == EventAll {
		m.callbacks[resource] = append(m.callbacks[resource], callback)
		return
	}
	key := fmt.Sprintf("%s.%s", resource, event)
	m.callbacks[key] = append(m.callbacks[key], callback)
}

// Publish 推送消息,异步, delay单位是秒
func (m *MessageBus) Publish(msg *Message, delay ...int) error {
	if !m.has(msg.Resource, msg.Event) {
		return ErrorNotSubscribed
	}
	if len(delay) > 0 && delay[0] > 0 {
		d := delay[0]
		if d > MaxDelay {
			d = MaxDelay
		}
		msg.priority = Monotonic() + time.Duration(d)*time.Second
		select {
		case m.dq <- msg:
			return nil
		case <-m.signal.Done():
			msg.priority = 0
			return m.syncPublish(msg)
		}
	}
	return m.syncPublish(msg)
}

func (m *MessageBus) syncPublish(msg *Message) error {
	select {
	case m.eq <- msg:
		return nil
	default:
		if atomic.LoadInt32(m.deadLetter) <= 0 {
			return ErrBlocked
		}
		go func() {
			atomic.AddInt32(m.deadLetter, -1)
			m.execute(msg)
			atomic.AddInt32(m.deadLetter, 1)
		}()

		return nil
	}
}

func (m *MessageBus) Size() int { // 用于外部监控管道长度
	return len(m.eq)
}

// NewMessageBus  通用消息总线  cache是执行队列的长度
func NewMessageBus(options ...Option) *MessageBus {

	opts := &Options{
		executors:  128,
		msgCache:   1024,
		queue:      1,
		queueCache: 256,
		queueSize:  512,
		deadLetter: 512,
		log: func(ctx context.Context, format string, args ...any) {
			// empty
		},
	}

	for _, o := range options {
		o(opts)
	}

	deadLetter := opts.deadLetter
	ctx, cancel := context.WithCancel(context.Background())

	m := &MessageBus{
		log:        opts.log,
		fork:       make(chan bool, 32),
		eq:         make(chan *Message, opts.msgCache),
		dq:         make(chan *Message, opts.queueCache),
		callbacks:  map[string][]MessageHandler{},
		queue:      new(int32),
		deadLetter: &deadLetter,
		signal:     ctx,
		cancel:     cancel,
	}

	m.launch(opts.executors, opts.queue, opts.queueSize)

	return m
}
