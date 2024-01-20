package msghub

import (
	"context"
	"time"
)

const (
	maxDelayTime     = time.Second * 30
	maxPriorityQueue = 8192
)

// 延迟队列
func delayQueue(ctx context.Context, dq chan *delayExecutor, eq chan *executor) {

	pq := NewPriorityList()
	sleep := maxDelayTime
	overtime := sleep + Monotonic()
	for {
		select {
		case item := <-dq:
			now := Monotonic()
			if pq.Len() >= maxPriorityQueue { // 延迟队列过长, 直接发送10个
				for i := 0; i < 10; i++ {
					first := pq.Pop()
					if first == nil {
						break
					}
					if e, ok := first.Payload().(*executor); ok {
						eq <- e
					}
				}
				if next := pq.Next(); next != nil {
					overtime = time.Duration(next.Priority())
				} else {
					overtime = maxDelayTime + Monotonic()
				}
			}
			delay := item.delay * time.Second
			at := now + delay
			pq.Push(item.executor, int64(at))
			if at < overtime {
				overtime = at
			}
			sleep = overtime - now
			if sleep <= 0 {
				sleep = 0
				overtime = now
			}
		case <-time.After(sleep):
			now := Monotonic()
			item := pq.Pop()
			if item == nil {
				sleep = maxDelayTime
				overtime = sleep + now
				continue
			}
			e, ok := item.Payload().(*executor)
			if !ok {
				continue
			}
			eq <- e
			next := pq.Next()
			if next == nil {
				sleep = maxDelayTime
				overtime = sleep + now
				continue
			} else {
				overtime = time.Duration(next.Priority())
				if overtime < now {
					sleep = 0
					overtime = now
				} else {
					sleep = overtime - now
				}
			}
		case <-ctx.Done():
			// 清空优先级队列
			for {
				item := pq.Pop()
				if item == nil {
					break
				}
				e, ok := item.Payload().(*executor)
				if !ok {
					continue
				}
				eq <- e
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
