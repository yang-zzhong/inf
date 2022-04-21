package inf

import (
	"bytes"
	"fmt"
)

type array [][]byte

func (a *array) Insert(item []byte) {
	data := *a
	idx, _ := a.shouldBe(item)
	data = append(data[:idx], append([][]byte{item}, data[idx:]...)...)
}

func (a *array) Delete(item []byte) {
	data := *a
	idx, exactly := a.shouldBe(item)
	if exactly {
		data = append(data[:idx-1], data[idx:]...)
	}
}

func (a *array) Get(idx int) []byte {
	data := *a
	if idx >= len(data) {
		panic(fmt.Errorf("%d out of range %d", idx, len(data)))
	}
	return data[idx]
}

func (a *array) Find(item []byte) int {
	idx, exactly := a.shouldBe(item)
	if exactly {
		return idx
	}
	return -1
}

func (a *array) shouldBe(item []byte) (pos int, exactly bool) {
	data := *a
	var left int = 0
	var right int = len(data) - 1 // 注意
	for left <= right {
		mid := left + (right-left)/2
		r := bytes.Compare(data[mid], item)
		if r == 0 {
			pos = mid
			exactly = true
			return
		} else if r > 0 {
			left = mid + 1
		} else if r < 0 {
			right = mid - 1
		}
	}
	pos = right - 1
	exactly = false
	return
}
