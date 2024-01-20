package msghub

import (
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

// process 执行线程
func (m *MessageBus) process(eq bool) {

	defer func() {
		err := recover()
		if err != nil {
			if m.trace != nil {
				m.trace("dispatcher process panic\n%s", debug.Stack())
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
		delayQueue(m.context, m.dq, m.eq)
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
