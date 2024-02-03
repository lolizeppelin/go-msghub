package msghub

import (
	"container/heap"
	"time"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*delayExecutor

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].at < pq[j].at
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*delayExecutor)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *delayExecutor, at time.Duration) {
	item.at = at
	heap.Fix(pq, item.index)
}

// PriorityList 堆排序实现的优先级队列,每次弹出优先级最小的对象
type PriorityList struct {
	pq PriorityQueue
}

func NewPriorityList() *PriorityList {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &PriorityList{
		pq: pq,
	}
}

func (l *PriorityList) Next() *delayExecutor {
	if l.Len() < 1 {
		return nil
	}
	return l.pq[0]
}

func (l *PriorityList) Push(item *delayExecutor) {
	heap.Push(&l.pq, item)
}

func (l *PriorityList) Pop() *delayExecutor {
	if l.pq.Len() > 0 {
		item := heap.Pop(&l.pq).(*delayExecutor)
		return item
	}
	return nil
}

func (l *PriorityList) Len() int {
	return l.pq.Len()
}
