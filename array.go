package inf

import (
	"fmt"
)

type Comparable interface {
	Compare(Comparable) int
}

type array []Comparable

func (a *array) insert(item Comparable) {
	idx, _ := a.shouldBe(item)
	a.insertAt(idx, item)
}

func (a *array) insertAt(idx int, item Comparable) {
	*a = append((*a)[:idx], append([]Comparable{item}, (*a)[idx:]...)...)
}

func (a *array) delete(item Comparable) {
	idx, exactly := a.shouldBe(item)
	if !exactly {
		return
	}
	if idx == 0 {
		*a = (*a)[1:]
		return
	}
	*a = append((*a)[:idx], (*a)[idx+1:]...)
}

func (a *array) get(idx int) Comparable {
	if idx >= len(*a) {
		panic(fmt.Errorf("%d out of range %d", idx, len(*a)))
	}
	return (*a)[idx]
}

func (a *array) shouldBe(item Comparable) (pos int, exactly bool) {
	if len(*a) == 0 {
		pos = 0
		return
	}
	var left int = 0
	var right int = len(*a) - 1
	for left <= right {
		mid := left + (right-left)/2
		r := (*a)[mid].Compare(item)
		if r == 0 {
			pos = mid
			exactly = true
			return
		} else if r > 0 {
			right = mid - 1
		} else if r < 0 {
			left = mid + 1
		}
	}
	pos = left
	exactly = false
	return
}
