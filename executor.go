package msghub

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
)

func (m *MessageBus) execute(msg *message) {
	key := fmt.Sprintf("%s.%s", msg.resource, msg.event)
	if callbacks, ok := m.callbacks[key]; ok {
		for _, cb := range callbacks {
			cb(msg.ctx, msg.resource, msg.event, msg.trigger, msg.payload)
		}
	}
	if callbacks, ok := m.callbacks[msg.resource]; ok {
		for _, cb := range callbacks {
			cb(msg.ctx, msg.resource, msg.event, msg.trigger, msg.payload)
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

// launch 启动消息总线
func (m *MessageBus) launch(executors int, queue int, total int) {

	if m.signal != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.signal = ctx
	m.cancel = cancel

	go func() { // 执行线程孵化
		for {
			select {
			case q := <-m.fork:
				go m.spawn(q, total)
			case <-m.signal.Done():
				return
			}
		}
	}()

	for i := 0; i < queue; i++ { // 延迟队列处理线程
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
		} else { // 下面的循环正常结束,退出
			if !eq {
				atomic.AddInt32(m.queue, -1)
			}
		}
	}()

	if eq {
		for {
			select {
			case msg := <-m.eq:
				m.execute(msg)
			case <-m.signal.Done():
				return
			}
		}
	} else {
		atomic.AddInt32(m.queue, 1)
		delayQueue(m.signal, total, m.dq, m.eq)
	}

}
