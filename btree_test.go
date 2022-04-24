package inf

import (
	"fmt"
	"testing"
)

func printv(tree *btree, key string, t *testing.T) {
	val, err := tree.Get(Key([]byte("hello")))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val.(pair).Val)
}

func TestNode_put(t *testing.T) {
	tree := NewTree(16)
	tree.Put(Pair([]byte("00"), []byte("00")))
	tree.Put(Pair([]byte("02"), []byte("02")))
	tree.Put(Pair([]byte("04"), []byte("04")))
	tree.Put(Pair([]byte("06"), []byte("06")))
	tree.Put(Pair([]byte("08"), []byte("08")))
	tree.Put(Pair([]byte("10"), []byte("10")))
	tree.Put(Pair([]byte("01"), []byte("01")))
	tree.Put(Pair([]byte("03"), []byte("03")))
	tree.Put(Pair([]byte("05"), []byte("05")))
	tree.Put(Pair([]byte("07"), []byte("07")))
	tree.Put(Pair([]byte("09"), []byte("09")))
}

func TestNode_del(t *testing.T) {
	tree := NewTree(16)
	for i := 0; i <= 6; i++ {
		k := []byte(fmt.Sprintf("%02d", i))
		tree.Put(Pair(k, k))
	}
	tree.Del(Key([]byte("00")))
}
