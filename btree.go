package inf

import "errors"

type pos struct {
	page int
	pos  uint16
}

type node struct {
	child pos
	next  pos
	data  []byte
}

type Node struct {
	data  array
	total uint16
	block int
}

type Tree struct {
	store *blockStore
}

func (node *Node) Find(key []byte) (node *Node, err error) {
	if !node.IsFull() {
	}
	return
}

func (node *Node) IsFull() bool {
	return len(node.data) >= node.total
}

func NewTree(pathfile string) *Tree {
	return &Tree{store: New(FileRWSC(pathfile))}
}

func (tree *Tree) Init() error {
	if err := tree.store.Open(); err != nil {
		if !errors.Is(err, ErrRWSCNotExists) {
			return err
		}
		if err := tree.store.Create(V010000, 512); err != nil {
			return err
		}
	}
	return nil
}

func (tree *Tree) Put(key, val []byte) error {

}
