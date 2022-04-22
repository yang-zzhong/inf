package inf

import (
	"bytes"
	"fmt"
	"testing"
)

type cbytes []byte

func (cb cbytes) Compare(item Comparable) int {
	return bytes.Compare(cb, item.(cbytes))
}

var (
	sample = []Comparable{
		cbytes("132"),
		cbytes("200"),
		cbytes("321"),
		cbytes("330"),
		cbytes("399"),
		cbytes("550"),
		cbytes("880")}
)

func printArray(a array) {
	for _, item := range a {
		fmt.Printf("%s\t", item)
	}
	fmt.Printf("\n")
}

func equalSample(sample, a []Comparable) bool {
	if len(sample) != len(a) {
		return false
	}
	for i := 0; i < len(sample); i++ {
		if sample[i].Compare(a[i]) != 0 {
			return false
		}
	}
	return true
}

func Test_array_Insert(t *testing.T) {
	a := &array{}
	for _, idx := range []int{6, 4, 1, 3, 5, 2, 0} {
		a.insert(sample[idx])
	}
	if !equalSample(sample, []Comparable(*a)) {
		t.Fatalf("insert error")
	}
}

func Test_array_Delete(t *testing.T) {
	a := array(make([]Comparable, 7))
	copy(a, sample)
	a.delete(cbytes("132"))
	if !equalSample(sample[1:], []Comparable(a)) {
		t.Fatalf("delete first element error")
	}
	b := array(make([]Comparable, 7))
	copy(b, sample)
	b.delete(cbytes("330"))
	rest := make([]Comparable, 7)
	copy(rest, sample)
	rest = append(rest[:3], rest[4:]...)
	if !equalSample(rest, []Comparable(b)) {
		t.Fatalf("delete mid element error")
	}
	c := array(make([]Comparable, 7))
	copy(c, sample)
	c.delete(cbytes("880"))
	if !equalSample(sample[:6], []Comparable(c)) {
		t.Fatalf("delete last element error")
	}
}
