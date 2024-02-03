package msghub

import (
	"context"
	"time"
)

const (
	sleepTime = time.Second * 30
)

// 延迟队列
func delayQueue(ctx context.Context, total int, dq chan *delayExecutor, eq chan *executor) {

	pq := NewPriorityList()
	sleep := sleepTime
	overtime := sleep + Monotonic()
	for {
		select {
		case item := <-dq:
			now := Monotonic()
			if pq.Len() >= total { // 延迟队列过长, 直接发送10个
				for i := 0; i < 10; i++ {
					fire := pq.Pop()
					if fire == nil {
						break
					}
					eq <- fire.executor
				}
				// 重算等待时间
				if next := pq.Next(); next != nil {
					overtime = next.at
				} else {
					overtime = sleepTime + Monotonic()
				}
			}
			pq.Push(item)
			if item.at < overtime {
				overtime = item.at
			}
			sleep = overtime - now
		case <-time.After(sleep):
			now := Monotonic()
			item := pq.Pop()
			if item == nil {
				sleep = sleepTime
				overtime = sleepTime + now
				continue
			}
			eq <- item.executor
			// 重算等待时间
			next := pq.Next()
			if next == nil {
				sleep = sleepTime
				overtime = sleepTime + now
				continue
			}
			overtime = next.at
			if overtime < now {
				sleep = 0
				overtime = now
			} else {
				sleep = overtime - now
			}
		case <-ctx.Done():
			// 清空优先级队列
			for {
				item := pq.Pop()
				if item == nil {
					break
				}
				eq <- item.executor
			}
			// 清空延迟管道
			for {
				select {
				case item := <-dq:
					eq <- item.executor
				default:
					return
				}
			}

		}
	}
}
