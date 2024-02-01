package msghub

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

type delayExecutor struct {
	executor *executor
	delay    time.Duration
}

type executor struct {
	resource string
	event    string
	trigger  string
	payload  any
}

func (c *executor) execute(bus *MessageBus) {
	key := fmt.Sprintf("%s.%s", c.resource, c.event)
	if callbacks, ok := bus.callbacks[key]; ok {
		for _, cb := range callbacks {
			cb(c.resource, c.event, c.trigger, c.payload)
		}
	}
	if callbacks, ok := bus.callbacks[c.resource]; ok {
		for _, cb := range callbacks {
			cb(c.resource, c.event, c.trigger, c.payload)
		}
	}
}

func (m *MessageBus) has(resource string, event string) bool {
	key := fmt.Sprintf("%s.%s", resource, event)
	if _, ok := m.callbacks[key]; ok {
		return true
	}
	_, ok := m.callbacks[resource]
	return ok
}

// launch 启动消息总线 executors 执行线程数默认10, waiters 延迟队列数默认4
func (m *MessageBus) launch(executors int, queue int, total int) {

	if m.context != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.context = ctx
	m.cancel = cancel

	go func() { // 执行线程孵化
		for {
			select {
			case q := <-m.fork:
				go m.spawn(q, total)
			case <-m.context.Done():
				return
			}
		}
	}()

	for i := 0; i < queue; i++ { // 延迟处理线程
		m.fork <- false
	}

	for i := 0; i < executors; i++ { // 执行线程
		m.fork <- true
	}

}

// process 执行线程
func (m *MessageBus) spawn(eq bool, total int) {

	defer func() {
		err := recover()
		if err != nil {
			if m.log != nil {
				m.log("dispatcher process panic\n%s", debug.Stack())
			}
			m.fork <- eq
		}
	}()

	if eq {
		for {
			select {
			case c := <-m.eq:
				c.execute(m)
			case <-m.context.Done():
				return
			}
		}
	} else {
		delayQueue(m.context, total, m.dq, m.eq)
	}

}
