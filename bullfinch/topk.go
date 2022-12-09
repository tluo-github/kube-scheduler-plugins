package bullfinch

import "container/heap"

// Item 优先队列
type Item struct {
	Value    interface{}
	Priority float64 // 优先级队列中节点的优先级
	index    int     // index是该节点在堆中的位置
}

// PriorityQueue 优先级队列需要实现heap的interface
type PriorityQueue []*Item

// Len 优先级队列需要实现heap的interface
func (pq PriorityQueue) Len() int {
	return len(pq)
}
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

// Pop 绑定put方法，将index置为-1是为了标识该数据已经出了优先级队列了
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	item.index = -1
	return item
}

// Push 绑定push方法
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// update 更新修改了优先级和值的item在优先级队列中的位置
func (pq *PriorityQueue) update(item *Item, value map[string]string, priority float64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.index)
}
