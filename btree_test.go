package inf

import (
	"fmt"
	"testing"
)

func TestNode_put(t *testing.T) {
	n := &node{total: 16}
	n.root().put([]byte("hello"), []byte("world"))
	n.root().put([]byte("hello1"), []byte("world1"))
	n.root().put([]byte("hello2"), []byte("world2"))
	n.root().put([]byte("hello4"), []byte("world3"))
	n.root().put([]byte("hello5"), []byte("world4"))
	n = n.root()
	val, err := n.root().get([]byte("hello"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
	val, err = n.root().get([]byte("hello1"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
	val, err = n.root().get([]byte("hello2"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
	val, err = n.root().get([]byte("hello4"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
	val, err = n.root().get([]byte("hello5"))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	fmt.Printf("%s\n", val)
}
