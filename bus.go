package msghub

import (
	"context"
	"fmt"
	"time"
)

const (
	EventAll = "*"
	MaxDelay = 300
)

type MessageBus struct {
	callbacks map[string][]func(resource, event, trigger string, payload any)
	eq        chan *executor
	dq        chan *delayExecutor
	fork      chan bool
	log       func(format string, args ...any)

	context context.Context
	cancel  func()
}

// Subscribe 关注消息
func (m *MessageBus) Subscribe(resource string, event string,
	callback func(resource, event, trigger string, payload any)) {
	if event == EventAll {
		m.callbacks[resource] = append(m.callbacks[resource], callback)
		return
	}
	key := fmt.Sprintf("%s.%s", resource, event)
	m.callbacks[key] = append(m.callbacks[key], callback)
}

// Publish 推送消息,异步, delay单位是秒
func (m *MessageBus) Publish(resource string, event string, trigger string, payload any, delay ...int) bool {
	if !m.has(resource, event) {
		return false
	}
	if len(delay) > 0 && delay[0] > 0 {
		d := delay[0]
		if d > MaxDelay {
			d = MaxDelay
		}

		m.dq <- &delayExecutor{
			executor: &executor{resource: resource, event: event, trigger: trigger, payload: payload},
			delay:    time.Duration(d),
		}
		return true
	}
	m.eq <- &executor{resource: resource, event: event, trigger: trigger, payload: payload}
	return true
}

func (m *MessageBus) Stop() {

	if m.cancel == nil {
		return
	}

	m.cancel()
	defer func() {
		err := recover()
		if err != nil && m.log != nil {
			m.log("dispatcher process panic on stop: %v", err)
		}
	}()

	// 清空执行队列
	for {
		select {
		case c := <-m.eq:
			c.execute(m)
		case <-time.After(time.Second * 3):
			return
		}
	}
}

func (m *MessageBus) Size() int { // 用于外部监控管道长度
	return len(m.eq)
}

// NewMessageBus  通用消息总线  cache是执行队列的长度
func NewMessageBus(log func(format string, args ...any), options ...Option) *MessageBus {

	opts := &Options{
		executors:     128,
		executorCache: 1024,
		queue:         1,
		queueCache:    128,
		queueSize:     256,
	}

	for _, o := range options {
		o(opts)
	}

	m := &MessageBus{
		log:       log,
		fork:      make(chan bool),
		eq:        make(chan *executor, opts.executorCache),
		dq:        make(chan *delayExecutor, opts.queueCache),
		callbacks: map[string][]func(resource, event, trigger string, payload any){},
	}

	m.launch(opts.executors, opts.queue, opts.queueSize)

	return m
}
