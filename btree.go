package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type btree struct {
	root  *node
	total uint16 // total key bytes
}

type Storeable interface {
	Comparable
	Bytes(max int) [][]byte
	Size(max int) int
}

type element struct {
	data  Storeable
	after *node
}

type pair struct {
	Key, Val []byte
}

func Pair(key, val []byte) pair {
	return pair{Key: key, Val: val}
}

func Key(key []byte) pair {
	return pair{Key: key}
}

func (p pair) Bytes(max int) [][]byte {
	if len(p.Key)+len(p.Val) > max {
		return [][]byte{p.Key, p.Val}
	}
	return [][]byte{append(p.Key, p.Val...)}
}

func (p pair) Size(max int) int {
	return len(p.Key) + len(p.Val)
}

func (p pair) Compare(t Comparable) int {
	return bytes.Compare(p.Key, t.(pair).Key)
}

func (p element) Compare(t Comparable) int {
	return p.data.Compare(t.(element).data)
}

type node struct {
	data   array
	first  *node
	p      *node
	block  int
	synced bool
	tree   *btree
}

func NewTree(total uint16) *btree {
	n := &node{}
	t := &btree{root: n, total: total}
	n.tree = t
	return t
}

func (tree *btree) Put(data Storeable) error {
	err := tree.root.put(data)
	tree.root = tree.root.root()
	return err
}

func (tree *btree) Del(data Storeable) {
	tree.root = tree.root.del(data)
}

func (tree *btree) Get(data Storeable) (Storeable, error) {
	return tree.root.get(data)
}

func (n *node) put(data Storeable) error {
	p := element{data: data}
	pos, exactly := n.data.shouldBe(p)
	if exactly {
		n.set(func() {
			p.after = n.data[pos].(element).after
			n.data[pos] = p
		})
		return nil
	}
	if pos == 0 {
		if n.first != nil {
			n.first.put(data)
			return nil
		}
	} else if n.data[pos-1].(element).after != nil {
		n.data[pos-1].(element).after.put(data)
		return nil
	}
	var err error
	n.set(func() {
		n.data.insertAt(pos, p)
		err = n.popup()
	})
	return err
}

func (n *node) del(data Storeable) *node {
	p := element{data: data}
	pos, exactly := n.data.shouldBe(p)
	if !exactly {
		if pos == 0 {
			if n.first != nil {
				return n.first.del(data)
			}
		} else if pos < len(n.data) {
			if c := n.data[pos-1].(element).after; c != nil {
				return c.del(data)
			}
		}
	}
	if n.leaf() {
		return n.delLeaf(pos)
	}
	return nil
}

func (n *node) leaf() bool {
	if n.first != nil {
		return false
	}
	for _, p := range n.data {
		if p.(element).after != nil {
			return false
		}
	}
	return true
}

func (n *node) delLeaf(pos int) *node {
	p := n.data[pos].(element)
	n.data = append(n.data[:pos], n.data[pos+1:]...)
	if len(n.data) != 0 {
		return n.root()
	}
	return n.borrow(p)
}

func (n *node) borrow(p element) *node {
	if n.p != nil {
		pos, _ := n.p.data.shouldBe(p)
		np := n.p.data[pos].(element)
		if np.after != nil {
			after := np.after
			np.after = after.first
			n.data = append([]Comparable{np}, after.data...)
		}
		n.p.data = append(n.p.data[:pos], n.p.data[pos+1:]...)
		if len(n.p.data) == 0 {
			n.p.borrow(np)
		}
	}
	return n.root()
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
	n.p = &node{first: n, data: []Comparable{p}, tree: n.tree}
	nn.p = n.p
	return nil
}

func (n *node) split(pos int) (nn *node, p element) {
	p = n.data[pos].(element)
	nd := make(array, pos)
	copy(nd, n.data[:pos])
	r := len(n.data) - pos - 1
	if r <= 0 {
		return
	}
	nnd := make(array, r)
	copy(nnd, n.data[pos+1:])
	n.data = nd
	nn = &node{data: nnd, tree: n.tree}
	for i, p := range nn.data {
		if p.(element).after != nil {
			p.(element).after.p = nn
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

func (n *node) get(data Storeable) (ret Storeable, err error) {
	pos, exactly := n.data.shouldBe(element{data: data})
	if exactly {
		ret = n.data[pos].(element).data
		return
	}
	if pos == 0 {
		if n.first != nil {
			ret, err = n.first.get(data)
			return
		}
		err = errors.New("not found")
		return
	}
	child := n.data[pos-1].(element).after
	if child == nil {
		err = errors.New("not found")
		return
	}
	ret, err = child.get(data)
	return
}

func (n *node) overflow() bool {
	return n.shouldUse() > int(n.tree.total)
}

func (n *node) shouldUse() int {
	t := 6
	for _, p := range n.data {
		t += p.(element).data.Size(n.elemMax())
	}
	return t
}

func (n *node) elemMax() int {
	return int((n.tree.total - 6) / 2)
}

func (n *node) sync(store *blockStore) error {
	if n.synced {
		return nil
	}
	bs := make([]byte, n.tree.total)
	first := 0
	if n.first != nil {
		n.first.sync(store)
		first = n.first.block
	}
	binary.BigEndian.PutUint32(bs[:4], uint32(first))
	binary.BigEndian.PutUint16(bs[4:6], uint16(n.tree.total))
	pos := 6
	for _, p := range n.data {
		elem := p.(element).data
		size := elem.Size(n.elemMax())
		copy(bs[pos:pos+size], elem.Bytes(n.elemMax())[0])
		pos += size
		after := 0
		if p.(element).after != nil {
			p.(element).after.sync(store)
			after = p.(element).after.block
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
