package msghub

import (
	"context"
	"fmt"
	"time"
)

const EventAll = "*"

type MessageBus struct {
	callbacks map[string][]func(resource, event, trigger string, payload any)
	eq        chan *executor
	dq        chan *delayExecutor
	fork      chan bool
	trace     func(format string, args ...any)

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
func (m *MessageBus) Publish(resource string, event string, trigger string, payload any, delay ...time.Duration) bool {
	if !m.has(resource, event) {
		return false
	}
	if len(delay) > 0 && delay[0] > 0 {
		m.dq <- &delayExecutor{
			executor: &executor{resource: resource, event: event, trigger: trigger, payload: payload},
			delay:    delay[0],
		}
		return true
	}
	m.eq <- &executor{resource: resource, event: event, trigger: trigger, payload: payload}
	return true
}

// Launch 启动消息总线 executors 执行线程数默认10, waiters 延迟队列数默认4
func (m *MessageBus) Launch(executors int, waiters int) {

	if m.context != nil {
		return
	}

	if executors <= 0 {
		executors = 10
	}

	if waiters <= 0 {
		waiters = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.context = ctx
	m.cancel = cancel

	go func() { // 执行线程孵化
		for {
			select {
			case q := <-m.fork:
				go m.process(q)
			case <-m.context.Done():
				return
			}
		}
	}()

	for i := 0; i < executors; i++ { // 执行线程
		m.fork <- true
	}

	for i := 0; i < waiters; i++ { // 延迟处理线程
		m.fork <- false
	}

}

func (m *MessageBus) Stop() {

	if m.cancel == nil {
		return
	}

	m.cancel()
	defer func() {
		err := recover()
		if err != nil {
			//log.Errorf("dispatcher process panic on stop: %v", err)
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
func NewMessageBus(cache int32, trace ...func(format string, args ...any)) *MessageBus {
	if cache < 100 {
		cache = 100
	}
	dcache := cache * 2
	if dcache > 8192 {
		dcache = 8192
	}
	m := &MessageBus{
		fork:      make(chan bool),
		eq:        make(chan *executor, cache),
		dq:        make(chan *delayExecutor, dcache),
		callbacks: map[string][]func(resource, event, trigger string, payload any){},
	}
	if len(trace) > 0 {
		m.trace = trace[0]
	}
	return m
}
