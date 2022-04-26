package inf

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func createTree() *btree {
	tree := NewTree(16)
	for _, k := range []string{"00", "02", "04", "06", "08", "10", "01", "03", "05", "07", "09"} {
		tree.Put(SP(k, k))
	}
	return tree
}

func isNode(name string, n *node, elems []string, t *testing.T) {
	if len(n.elems) != len(elems) {
		t.Fatalf("%s should be %s", name, strings.Join(elems, ","))
	}
	for i, e := range elems {
		if !bytes.Equal([]byte(e), n.elems[i].(elem).data.(pair).Key) {
			t.Fatalf("%s should be %s", name, strings.Join(elems, ","))
		}
	}
}

func printv(tree *btree, key string, t *testing.T) {
	val, err := tree.Get(SK("hello"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val.(pair).Val)
}

func TestNode_put(t *testing.T) {
	tree := createTree()
	isNode("root", tree.root, []string{"04"}, t)
	isNode("level 1 left 1", tree.root.first, []string{"02"}, t)
	isNode("level 2 left 1", tree.root.first.first, []string{"00", "01"}, t)
	isNode("level 2 left 2", tree.root.first.elems[0].(elem).after, []string{"03"}, t)
	isNode("level 1 left 2", tree.root.elems[0].(elem).after, []string{"06", "08"}, t)
	isNode("level 2 left 1", tree.root.elems[0].(elem).after.first, []string{"05"}, t)
	isNode("level 2 left 2", tree.root.elems[0].(elem).after.elems[0].(elem).after, []string{"07"}, t)
	isNode("level 2 left 3", tree.root.elems[0].(elem).after.elems[1].(elem).after, []string{"09", "10"}, t)
}

func TestNode_del(t *testing.T) {
}

// func TestNode_del(t *testing.T) {
// 	for _, total := range []int{100000, 1000000, 10000000} {
// 		insert(total)
// 	}
// }
//
// func insert(total int) {
// 	tree := NewTree(512)
// 	start := time.Now().UnixNano()
// 	for i := 0; i <= total; i++ {
// 		k := []byte(fmt.Sprintf("%02d", rand.Intn(1000000)))
// 		tree.Put(BP(k, k))
// 	}
// 	end := time.Now().UnixNano()
// 	fmt.Printf("total: %dns, each: %dns\n", end-start, (end-start)/int64(total))
// }
//
// func BenchmarkTree_Put(b *testing.B) {
// 	tree := NewTree(512)
// 	rand.Seed(time.Now().UnixNano())
// 	for i := 0; i < b.N; i++ {
// 		// k := fmt.Sprintf("%02d", rand.Intn(1000000))
// 		k := fmt.Sprintf("%02d", i)
// 		tree.Put(SP(k, k))
// 	}
// }
