package inf

import (
	"fmt"
	"testing"
)

func printv(n *node, key string, t *testing.T) {
	val, err := n.root().get([]byte("hello"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
}

func TestNode_put(t *testing.T) {
	n := &node{total: 16}
	n.put([]byte("00"), []byte("00"))
	n = n.root()
	n.put([]byte("02"), []byte("02"))
	n = n.root()
	n.put([]byte("04"), []byte("04"))
	n = n.root()
	n.put([]byte("06"), []byte("06"))
	n = n.root()
	n.put([]byte("08"), []byte("08"))
	n = n.root()
	n.put([]byte("10"), []byte("10"))
	n = n.root()
	n.put([]byte("01"), []byte("01"))
	n = n.root()
	n.put([]byte("03"), []byte("03"))
	n = n.root()
	n.put([]byte("05"), []byte("05"))
	n = n.root()
	n.put([]byte("07"), []byte("07"))
	n = n.root()
	n.put([]byte("09"), []byte("09"))
	n = n.root()
}

func TestNode_del(t *testing.T) {
	n := &node{total: 16}
	for i := 0; i <= 10; i++ {
		k := []byte(fmt.Sprintf("%02d", i))
		n.put(k, k)
		n = n.root()
	}
	n.del([]byte("00"))
	n = n.root()
}
