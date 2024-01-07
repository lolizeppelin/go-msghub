package msghub

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    any   // The value of the item; arbitrary.
	priority int64 // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

func (i *Item) Priority() int64 {
	return i.priority
}

func (i *Item) Payload() any {
	return i.value
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
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

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int64) {
	item.value = value
	item.priority = priority
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

func (l *PriorityList) Next() *Item {
	if l.Len() < 1 {
		return nil
	}
	return l.pq[0]
}

func (l *PriorityList) Push(v any, priority int64) {
	item := &Item{
		value:    v,
		priority: priority,
	}
	heap.Push(&l.pq, item)
}

func (l *PriorityList) Pop() *Item {
	if l.pq.Len() > 0 {
		item := heap.Pop(&l.pq).(*Item)
		return item
	}
	return nil
}

func (l *PriorityList) Len() int {
	return l.pq.Len()
}
