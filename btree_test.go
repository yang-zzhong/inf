package inf

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func printv(tree *btree, key string, t *testing.T) {
	val, err := tree.Get(SK("hello"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val.(pair).Val)
}

func TestNode_put(t *testing.T) {
	tree := NewTree(16)
	tree.Put(SP("00", "00"))
	tree.Put(SP("02", "02"))
	tree.Put(SP("04", "04"))
	tree.Put(SP("06", "06"))
	tree.Put(SP("08", "08"))
	tree.Put(SP("10", "10"))
	tree.Put(SP("01", "01"))
	tree.Put(SP("03", "03"))
	tree.Put(SP("05", "05"))
	tree.Put(SP("07", "07"))
	tree.Put(SP("09", "09"))
}

func TestNode_del(t *testing.T) {
	for _, total := range []int{100000, 1000000, 10000000} {
		insert(total)
	}
}

func insert(total int) {
	tree := NewTree(32)
	start := time.Now().UnixNano()
	for i := 0; i <= total; i++ {
		k := []byte(fmt.Sprintf("%02d", rand.Intn(1000000)))
		tree.Put(BP(k, k))
	}
	end := time.Now().UnixNano()
	fmt.Printf("total: %dns, each: %dns\n", end-start, (end-start)/int64(total))
}

func BenchmarkTree_Put(b *testing.B) {
	tree := NewTree(32)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("%02d", rand.Intn(1000000))
		tree.Put(SP(k, k))
	}
}
