package msghub

import (
	"context"
	"time"
)

const (
	sleepTime = time.Second * 30
)

// 延迟队列
func delayQueue(signal context.Context, total int, dq chan *Message, eq chan *Message) {

	pq := NewPriorityList()
	sleep := sleepTime
	overtime := sleep + Monotonic()
	for {
		select {
		case msg := <-dq:
			now := Monotonic()
			if pq.Len() >= total { // 延迟队列过长, 直接发送10个
				for i := 0; i < 10; i++ {
					fire := pq.Pop()
					if fire == nil {
						break
					}
					eq <- fire
				}
				// 重算等待时间
				if next := pq.Next(); next != nil {
					overtime = next.priority
				} else {
					// 队列最小长度128,弹出10个元素不可能队列为空
					overtime = msg.priority
				}
			}
			pq.Push(msg)
			if msg.priority < overtime {
				overtime = msg.priority
			}
			sleep = overtime - now
		case <-time.After(sleep):
			now := Monotonic()
			// 重算等待时间
			fire := 0
			for {
				next := pq.Next()
				if next == nil {
					sleep = sleepTime
					overtime = sleepTime + now
					break
				}
				if next.priority <= now {
					fire += 1
					if fire > 10 { // 避免堵死
						overtime = now
						sleep = 0
						break
					}
					item := pq.Pop()
					eq <- item
				} else {
					overtime = next.priority
					sleep = overtime - now
					break
				}
			}
		case <-signal.Done():
			// 清空优先级队列
			for {
				msg := pq.Pop()
				if msg == nil {
					break
				}
				eq <- msg
			}
			// 清空延迟管道
			for {
				select {
				case msg := <-dq:
					eq <- msg
				default:
					return
				}
			}

		}
	}
}
