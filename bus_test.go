package msghub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBus(t *testing.T) {

	log := func(ctx context.Context, msg string, args ...any) {
		fmt.Println(msg)
		for _, arg := range args {
			fmt.Printf("args %v\n", arg)
		}
	}

	bus := NewMessageBus(WithLog(log), WithDelayQueue(1))

	bus.Subscribe("a", "b", func(msg *Message) {
		fmt.Printf("yes %s ~ %v\n", msg.Trigger, msg.Payload)
	})

	bus.Subscribe("a", "c", func(msg *Message) {
		fmt.Printf("yes %s ~ %v\n", msg.Trigger, msg.Payload)
		panic("dispatcher panic test")
	})

	bus.Subscribe("a", "d", func(msg *Message) {
		//panic("failed")
		fmt.Printf("yes %s ~ %v\n", msg.Trigger, msg.Payload)
	})

	msg := &Message{
		Ctx:      context.Background(),
		Resource: "a",
		Event:    "b",
		Trigger:  "nodelay",
		Payload:  "ok",
	}
	bus.Publish(msg)

	msg = msg.Clone()
	msg.Trigger = "delay"
	bus.Publish(msg, 5)

	msg = msg.Clone()
	msg.Event = "c"
	msg.Trigger = "panic"
	bus.Publish(msg, 3)

	msg = msg.Clone()
	msg.Event = "d"
	msg.Trigger = "stop1"
	bus.Publish(msg, 5)

	msg = msg.Clone()
	msg.Event = "d"
	msg.Trigger = "stop2"
	bus.Publish(msg, 15)

	msg = msg.Clone()
	msg.Event = "d"
	msg.Trigger = "stop3"
	bus.Publish(msg, 15)

	msg = msg.Clone()
	msg.Event = "d"
	msg.Trigger = "stop4"
	bus.Publish(msg, 15)

	time.Sleep(time.Second * 10)
	bus.Stop()

}
