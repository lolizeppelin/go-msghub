package msghub

import (
	"fmt"
	"testing"
	"time"
)

func TestBus(t *testing.T) {

	bus := NewMessageBus(func(format string, args ...any) {
		fmt.Println(fmt.Sprintf(format, args...))
	}, WithDelayQueue(2))
	bus.Subscribe("a", "b", func(resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})

	bus.Subscribe("a", "c", func(resource, event, trigger string, payload any) {
		//panic("failed")
	})

	bus.Publish("a", "b", "nodelay", "ok")
	bus.Publish("a", "b", "delay", "ok", 5)
	bus.Publish("a", "c", "delay", "ok", 3)

	time.Sleep(time.Second * 10)
	bus.Stop()

}
