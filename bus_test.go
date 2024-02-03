package msghub

import (
	"fmt"
	"testing"
	"time"
)

func TestBus(t *testing.T) {

	log := func(format string, args ...any) {
		fmt.Println(fmt.Sprintf(format, args...))
	}

	bus := NewMessageBus(WithLog(log), WithDelayQueue(1))
	bus.Subscribe("a", "b", func(resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})

	bus.Subscribe("a", "c", func(resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
		panic("failed")
	})

	bus.Subscribe("a", "d", func(resource, event, trigger string, payload any) {
		//panic("failed")
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})

	bus.Publish("a", "b", "nodelay", "ok")
	bus.Publish("a", "b", "delay", "ok", 5)
	bus.Publish("a", "c", "panic", "ok", 3)
	bus.Publish("a", "d", "stop1", "ok", 15)
	bus.Publish("a", "d", "stop2", "ok", 15)
	bus.Publish("a", "d", "stop3", "ok", 15)
	bus.Publish("a", "d", "stop4", "ok", 15)
	bus.Publish("a", "d", "stop5", "ok", 15)

	time.Sleep(time.Second * 10)
	bus.Stop()

}
