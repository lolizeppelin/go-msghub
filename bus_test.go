package msghub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBus(t *testing.T) {

	log := func(format string, args ...any) {
		fmt.Println(fmt.Sprintf(format, args...))
	}

	bus := NewMessageBus(WithLog(log), WithDelayQueue(1))
	bus.Subscribe("a", "b", func(context context.Context, resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})

	bus.Subscribe("a", "c", func(context context.Context, resource, event, trigger string, payload any) {
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
		panic("dispatcher panic test")
	})

	bus.Subscribe("a", "d", func(context context.Context, resource, event, trigger string, payload any) {
		//panic("failed")
		fmt.Printf("yes %s ~ %v\n", trigger, payload)
	})
	ctx := context.Background()
	bus.Publish(ctx, "a", "b", "nodelay", "ok")
	bus.Publish(ctx, "a", "b", "delay", "ok", 5)
	bus.Publish(ctx, "a", "c", "panic", "ok", 3)
	bus.Publish(ctx, "a", "d", "stop1", "ok", 15)
	bus.Publish(ctx, "a", "d", "stop2", "ok", 15)
	bus.Publish(ctx, "a", "d", "stop3", "ok", 15)
	bus.Publish(ctx, "a", "d", "stop4", "ok", 15)
	bus.Publish(ctx, "a", "d", "stop5", "ok", 15)

	time.Sleep(time.Second * 10)
	bus.Stop(10)

}
