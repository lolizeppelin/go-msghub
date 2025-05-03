package msghub

import (
	"runtime/debug"
	"sync/atomic"
	"time"
)

// Stop 停止消息总线,wait为最大等待时间,单位s
func (m *MessageBus) Stop() {
	m.cancel()

	defer func() {
		close(m.eq)
		err := recover()
		if err != nil {
			m.log(m.signal, "dispatcher process panic on stop", "stack", debug.Stack())
		}
	}()

	time.Sleep(time.Millisecond * 250)

	// 清空执行队列(延迟队列可能不停的推送到执行队列中,所以需要等待)
	for {
		select {
		case msg := <-m.eq:
			m.execute(msg)
		case msg := <-m.dq:
			msg.priority = 0
			m.execute(msg)
		case <-time.After(time.Millisecond * 250):
			if atomic.LoadInt32(m.queue) > 0 {
				break
			}
			return
		}
	}
}
