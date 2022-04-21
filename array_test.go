package inf

import (
	"fmt"
	"testing"
)

func Test_array_Insert(t *testing.T) {
	a := &array{}
	a.Insert([]byte("1"))
	a.Insert([]byte("2"))
	a.Insert([]byte("3"))
	a.Insert([]byte("4"))
	a.Insert([]byte("5"))
	a.Insert([]byte("6"))
	a.Insert([]byte("7"))
	for i, item := range *a {
		fmt.Printf("%d - %s\n", i, item)
	}
}
