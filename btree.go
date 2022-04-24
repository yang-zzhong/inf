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
	key, val   []byte
	withoutVal bool
	valueBlock int
	after      *node
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
	return nil
}

func (n *node) put(key, val []byte) error {
	p := pair{key: key, val: val}
	pos, exactly := n.data.shouldBe(p)
	if exactly {
		n.set(func() {
			p.after = n.data[pos].(pair).after
			n.data[pos] = p
		})
		return nil
	}
	if pos == 0 {
		if n.first != nil {
			n.first.put(key, val)
			return nil
		}
	} else if n.data[pos-1].(pair).after != nil {
		n.data[pos-1].(pair).after.put(key, val)
		return nil
	}
	var err error
	n.set(func() {
		n.data.insertAt(pos, p)
		err = n.popup()
	})
	return err
}

func (n *node) del(key []byte) {
	p := pair{key: key}
	pos, exactly := n.data.shouldBe(p)
	if !exactly {
		if pos == 0 {
			if n.first != nil {
				n.first.del(key)
			}
		} else if pos < len(n.data) {
			if c := n.data[pos-1].(pair).after; c != nil {
				c.del(key)
			}
		}
		return
	}
	p = n.data[pos].(pair)
	n.data = append(n.data[:pos], n.data[pos+1:]...)
	pairs := n.tempDel(p)
	for _, pair := range pairs {
		n.put(pair.key, pair.val)
	}
}

func (n *node) tempDel(p pair) []pair {
	pos, exactly := n.data.shouldBe(p)
	ret := []pair{}
	if pos == 0 || pos == len(n.data) {
		n.traverse(func(p pair) {
			ret = append(ret, p)
		})
		if n.p != nil {
			pos, _ := n.p.data.shouldBe(p)
			if pos == 0 {
				n.p.first = nil
			} else {
				n.p.data = append(n.p.data[:pos], n.p.data[pos+1:]...)
			}
			ret = append(ret, n.p.tempDel(p)...)
		}
		return ret
	}
	p = n.data[pos].(pair)
	if p.after != nil {
		ret = append(ret, p.after.tempDel(p)...)
	}
	if exactly {
		ret = append(ret, p)
	}
	return ret
}

func (n *node) traverse(handle func(p pair)) {
	if n.first != nil {
		n.first.traverse(handle)
	}
	for _, p := range n.data {
		handle(p.(pair))
		if p.(pair).after != nil {
			p.(pair).after.traverse(handle)
		}
	}
}

func (n *node) popup() error {
	if !n.overflow() {
		return nil
	}
	if len(n.data) < 3 {
		return errors.New("room is not enough")
	}
	nn, p := n.split(len(n.data) / 2)
	if n.p != nil {
		nn.p = n.p
		pos, _ := n.p.data.shouldBe(p)
		n.p.data.insertAt(pos, p)
		return n.p.popup()
	}
	n.p = &node{first: n, data: []Comparable{p}, total: n.total}
	nn.p = n.p
	return nil
}

func (n *node) split(pos int) (nn *node, p pair) {
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
	for i, p := range nn.data {
		if p.(pair).after != nil {
			p.(pair).after.p = nn
			nn.data[i] = p
		}
	}
	if p.after != nil {
		p.after.p = nn
		nn.first = p.after
	}
	p.after = nn
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
	block := Block{Type: TypeSingle, Data: bs, Next: 0, size: uint16(pos), idx: n.block}
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
