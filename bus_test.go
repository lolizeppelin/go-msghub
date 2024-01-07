package msghub

import (
	"fmt"
	"testing"
	"time"
)

func TestBus(t *testing.T) {

	bus := NewMessageBus(10000)
	bus.Subscribe("a", "b", func(resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})
	bus.Launch(10, 4)
	bus.Publish("a", "b", "nodelay", "ok")
	bus.Publish("a", "b", "delay", "ok", 5)

	time.Sleep(time.Second * 10)
	bus.Stop()

}
