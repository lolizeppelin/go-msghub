package msghub

import (
	"context"
	"fmt"
	"runtime/debug"
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
	log       LoggingHandler
	queue     *int32 // 延迟队列处理协程

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

// Publish 推送消息,异步, delay单位是秒,异步队列需要判断是否提前执行
func (m *MessageBus) Publish(msg *Message, delay ...int) error {
	if !m.has(msg.Resource, msg.Event) {
		return ErrorNotSubscribed
	}
	msg.priority = 0
	msg.enqueue = time.Now().Unix()
	if len(delay) > 0 && delay[0] > 0 {
		d := delay[0]
		if d > MaxDelay {
			d = MaxDelay
		}
		msg.delay = int64(d)
		msg.priority = Monotonic() + time.Duration(d)*time.Second
		select {
		case m.dq <- msg:
			return nil
		case <-m.signal.Done():

			return m.syncPublish(msg)
		default:
			return m.syncPublish(msg)
		}
	}
	return m.syncPublish(msg)

}

func (m *MessageBus) syncPublish(msg *Message) (err error) {
	select {
	case m.eq <- msg:
		return
	default: // 异步转同步
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			m.log(m.signal, "dispatcher sync execute", "panic", r, "stack", debug.Stack())
			var ok bool
			if err, ok = r.(error); ok {
				return
			}
			err = fmt.Errorf("dispatcher sync execute panic %v", r)
		}()

		m.execute(msg)
	}
	return
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
		log: func(ctx context.Context, format string, args ...any) {
			// empty
		},
	}

	for _, o := range options {
		o(opts)
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &MessageBus{
		log:       opts.log,
		fork:      make(chan bool, 32),
		eq:        make(chan *Message, opts.msgCache),
		dq:        make(chan *Message, opts.queueCache),
		callbacks: map[string][]MessageHandler{},
		queue:     new(int32),
		signal:    ctx,
		cancel:    cancel,
	}

	m.launch(opts.executors, opts.queue, opts.queueSize)

	return m
}
