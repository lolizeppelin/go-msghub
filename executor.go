package msghub

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

func (m *MessageBus) execute(msg *Message) {
	key := fmt.Sprintf("%s.%s", msg.Resource, msg.Event)
	now := time.Now()
	if callbacks, ok := m.callbacks[key]; ok {
		msg.at = &now
		for _, cb := range callbacks {
			cb(msg)
		}
	}
	if callbacks, ok := m.callbacks[msg.Resource]; ok {
		if msg.at == nil {
			msg.at = &now
		}
		for _, cb := range callbacks {
			cb(msg)
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
func (m *MessageBus) spawn(sync bool, total int) {

	defer func() {
		err := recover()
		if err != nil {
			m.log(m.signal, "dispatcher process panic", "stack", debug.Stack())
			m.fork <- sync
		} else { // 下面的循环正常结束,退出
			if !sync {
				atomic.AddInt32(m.queue, -1)
			}
		}
	}()

	if sync {
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
