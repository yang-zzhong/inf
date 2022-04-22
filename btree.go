package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type btree struct {
	root   *node
	blocks *blockStore
}

type pair struct {
	key, val []byte
	after    *node
}

func (p pair) Compare(t Comparable) int {
	return bytes.Compare(p.key, t.(pair).key)
}

type node struct {
	data   array
	first  *node
	p      *node
	total  uint16 // total key bytes
	block  int
	synced bool
}

func NewBTree(rwsNew func() (RWSC, error)) *btree {
	return &btree{blocks: New(rwsNew)}
}

func (t *btree) Init() {

}

func (t *btree) syncNode(n *node) error {
	n.sync(t.blocks)
}

func (n *node) put(key, val []byte) {
	p := pair{key: key, val: val}
	pos, exactly := n.data.shouldBe(p)
	if exactly {
		n.set(func() {
			p.after = n.data[pos].(pair).after
			n.data[pos] = p
		})
		return
	}
	if pos == 0 {
		if n.first != nil {
			n.first.put(key, val)
			return
		}
	} else if n.data[pos-1].(pair).after != nil {
		n.data[pos-1].(pair).after.put(key, val)
		return
	}
	n.set(func() {
		n.data.insertAt(pos, p)
		n.popup()
	})
}

func (n *node) del(key []byte) {

}

func (n *node) popup() {
	if !n.overflow() {
		return
	}
	nn, p := n.split()
	if n.p != nil {
		pos, _ := n.p.data.shouldBe(p)
		p.after = nn
		n.p.data.insertAt(pos, p)
		n.p.popup()
		return
	}
	p.after = nn
	n.p = &node{first: n, data: []Comparable{p}, total: n.total}
	nn.p = n.p
}

func (n *node) split() (nn *node, p pair) {
	pos := len(n.data) / 2

	p = n.data[pos].(pair)

	nd := make(array, pos)
	copy(nd, n.data[:pos])
	r := len(n.data) - pos - 1
	if r <= 0 {
		return
	}
	nnd := make(array, r)
	copy(nnd, n.data[pos+1:])
	n.data = nd

	nn = &node{data: nnd, total: n.total}

	return
}

func (n *node) get(key []byte) (val []byte, err error) {
	pos, exactly := n.data.shouldBe(pair{key: key})
	if exactly {
		val = n.data[pos].(pair).val
		return
	}
	if pos == 0 {
		if n.first != nil {
			val, err = n.first.get(key)
			return
		}
		err = errors.New("not found")
		return
	}
	child := n.data[pos-1].(pair).after
	if child == nil {
		err = errors.New("not found")
		return
	}
	val, err = child.get(key)
	return
}

func (n *node) overflow() bool {
	t := 6
	for _, p := range n.data {
		t += len(p.(pair).key) + len(p.(pair).val) + 4 + 4
	}
	return t > int(n.total)
}

func (n *node) sync(store *blockStore) error {
	if n.synced {
		return nil
	}
	bs := make([]byte, n.total)
	first := 0
	if n.first != nil {
		n.first.sync(store)
		first = n.first.block
	}
	binary.BigEndian.PutUint32(bs[:4], uint32(first))
	binary.BigEndian.PutUint16(bs[4:6], uint16(n.total))
	pos := 6
	for _, p := range n.data {
		copy(bs[pos:], p.(pair).key)
		pos += len(p.(pair).key)
		bs[pos] = '\r'
		bs[pos+1] = '\n'
		pos += 2
		copy(bs[pos:], p.(pair).val)
		pos += len(p.(pair).val)
		bs[pos] = '\r'
		bs[pos+1] = '\n'
		pos += 2
		after := 0
		if p.(pair).after != nil {
			p.(pair).after.sync(store)
			after = p.(pair).after.block
		}
		binary.BigEndian.PutUint32(bs[pos:pos+4], uint32(after))
		pos += 4
	}
	if n.block == 0 {
		blocks, err := store.Acquire(1)
		if err != nil {
			return err
		}
		blocks[0].Data = bs
		return store.Put(blocks)
	}
	block := Block{Type: TypeData, Data: bs, Next: 0, size: uint16(pos), idx: n.block}
	return store.Put([]Block{block})
}

func (n *node) set(do func()) {
	do()
	n.synced = false
}

func (n *node) root() *node {
	if n.p != nil {
		return n.p.root()
	}
	return n
}
