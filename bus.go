package msghub

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	EventAll = "*"
	MaxDelay = 600 // 最大延迟600s
)

type MessageBus struct {
	callbacks map[string][]func(resource, event, trigger string, payload any)
	eq        chan *executor
	dq        chan *Item
	fork      chan bool
	log       func(format string, args ...any)
	queue     *int32 // 延迟队列处理协程

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

		m.dq <- &Item{
			executor: &executor{resource: resource, event: event, trigger: trigger, payload: payload},
			priority: Monotonic() + time.Duration(d)*time.Second,
		}
		return true
	}
	m.eq <- &executor{resource: resource, event: event, trigger: trigger, payload: payload}
	return true
}

// Stop 停止消息总线,wait为最大等待时间,单位s
func (m *MessageBus) Stop(wait ...time.Duration) {
	var wt time.Duration
	if len(wait) > 0 {
		wt = wait[0]
	}
	if wt < 1 {
		wt = time.Millisecond * 250
	} else {
		wt = wt * time.Second
	}

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

	// 清空执行队列(延迟队列可能不停的推送到执行队列中,所以需要等待)
	for {
		select {
		case c := <-m.eq:
			c.execute(m)
		priority:
			for {
				select {
				case _c := <-m.eq:
					_c.execute(m)
				default:
					break priority
				}
			}
			if atomic.LoadInt32(m.queue) <= 0 { // 延迟线程已经结束
				for {
					select {
					case _c := <-m.eq:
						_c.execute(m)
					default:
						return
					}
				}
			}
		case <-time.After(wt):
			if atomic.LoadInt32(m.queue) > 0 {
				m.log("delay thread not finished")
			}
			return
		}
	}
}

func (m *MessageBus) Size() int { // 用于外部监控管道长度
	return len(m.eq)
}

// NewMessageBus  通用消息总线  cache是执行队列的长度
func NewMessageBus(options ...Option) *MessageBus {

	opts := &Options{
		executors:     128,
		executorCache: 1024,
		queue:         1,
		queueCache:    256,
		queueSize:     512,
	}

	for _, o := range options {
		o(opts)
	}

	m := &MessageBus{
		log:       opts.log,
		fork:      make(chan bool, 32),
		eq:        make(chan *executor, opts.executorCache),
		dq:        make(chan *Item, opts.queueCache),
		callbacks: map[string][]func(resource, event, trigger string, payload any){},
		queue:     new(int32),
	}

	m.launch(opts.executors, opts.queue, opts.queueSize)

	return m
}
