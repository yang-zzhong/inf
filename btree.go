package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
)

type btree struct {
	root  *node
	total uint16 // total key bytes
	lock  sync.RWMutex
}

type Storeable interface {
	Comparable
	Bytes(max int) [][]byte
	Size(max int) int
}

type elem struct {
	data  Storeable
	after *node
}

type pair struct {
	Key, Val []byte
}

func BP(key, val []byte) pair {
	return pair{Key: key, Val: val}
}

func SP(key, val string) pair {
	return pair{Key: []byte(key), Val: []byte(val)}
}

func BK(key []byte) pair {
	return pair{Key: key}
}

func SK(key string) pair {
	return pair{Key: []byte(key)}
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

func (p elem) Compare(t Comparable) int {
	return p.data.Compare(t.(elem).data)
}

type node struct {
	elems  array
	first  *node
	p      *node
	block  int
	synced bool
	tree   *btree
}

func NewTree(total uint16) *btree {
	return &btree{total: total}
}

func (tree *btree) Put(data Storeable) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	if tree.root == nil {
		tree.root = &node{elems: []Comparable{elem{data: data}}, tree: tree}
		return
	}
	tree.root = tree.root.put(data)
}

func (tree *btree) Del(data Storeable) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	tree.root = tree.root.del(data)
	if len(tree.root.elems) == 0 {
		tree.root = tree.root.first
	}
}

func (tree *btree) Get(data Storeable) (Storeable, error) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	return tree.root.get(data)
}

func (n *node) put(data Storeable) *node {
	p := elem{data: data}
	pos, exactly := n.elems.shouldBe(p)
	if exactly {
		p.after = n.elems[pos].(elem).after
		n.elems[pos] = p
		return n.root()
	}
	if pos == 0 {
		if n.first != nil {
			return n.first.put(data)
		}
	} else if n.elems[pos-1].(elem).after != nil {
		return n.elems[pos-1].(elem).after.put(data)
	}
	n.elems.insertAt(pos, p)
	return n.popup()
}

func (n *node) del(data Storeable) *node {
	p := elem{data: data}
	pos, exactly := n.elems.shouldBe(p)
	if !exactly {
		if pos == 0 {
			if n.first != nil {
				return n.first.del(data)
			}
		} else {
			if c := n.elems[pos-1].(elem).after; c != nil {
				return c.del(data)
			}
		}
	}
	if n.leaf() {
		return n.delLeaf(pos)
	}
	return n.delNonLeaf(pos)
}

func (n *node) delNonLeaf(pos int) *node {
	p := n.elems[pos].(elem)
	n.elems = append(n.elems[:pos], n.elems[pos+1:]...)
	if len(n.elems) == 0 {
		return n.merge(p)
	}
	return n.root()
}

func (n *node) leaf() bool {
	if n.first != nil {
		return false
	}
	for _, p := range n.elems {
		if p.(elem).after != nil {
			return false
		}
	}
	return true
}

func (n *node) delLeaf(pos int) *node {
	p := n.elems[pos].(elem)
	n.elems = append(n.elems[:pos], n.elems[pos+1:]...)
	if len(n.elems) != 0 {
		return n.root()
	}
	return n.merge(p)
}

func (n *node) mergeRight(pos int, p elem) *node {
	np := n.p.elems[pos].(elem)
	if np.after != nil {
		after := np.after
		np.after = after.first
		if after.first != nil {
			after.first.p = n
		}
		n.elems = append([]Comparable{np}, after.elems...)
	}
	if p.after != nil {
		if n.first != nil {
			p.after.elems = append(n.first.elems, p.after.elems...)
		}
		n.first = p.after
	}
	n.p.elems = append(n.p.elems[:pos], n.p.elems[pos+1:]...)
	n.popup()
	if len(n.p.elems) == 0 && n.p.p != nil {
		return n.p.merge(np)
	}
	return n.root()
}

func (n *node) mergeLeft(pos int, p elem) *node {
	pos -= 1
	np := n.p.elems[pos].(elem)
	if p.after == nil && n.first != nil && np.Compare(n.first.elems[0]) < 0 {
		p.after = n.first
		n.first = nil
	}
	p.data = np.data
	n.elems = []Comparable{p}
	n.p.elems = append(n.p.elems[:pos], n.p.elems[pos+1:]...)
	ppos := pos - 1
	if ppos == -1 && n.p.first != nil {
		n.elems = append(n.p.first.elems, n.elems...)
		n.first = n.p.first.first
		n.p.first = n
	} else if bnp := n.p.elems[ppos].(elem); bnp.after != nil {
		n.elems = append(bnp.after.elems, n.elems...)
		bnp.after = n
		n.p.elems[ppos] = bnp
	}
	n.popup()
	if len(n.p.elems) == 0 && n.p.p != nil {
		return n.p.merge(np)
	}
	return n.root()
}

func (n *node) merge(p elem) *node {
	if n.p == nil {
		return n
	}
	pos, _ := n.p.elems.shouldBe(p)
	if pos == 0 {
		return n.mergeRight(pos, p)
	}
	return n.mergeLeft(pos, p)
}

func (n *node) popup() *node {
	if !n.overflow() {
		return n.root()
	}
	nn, p := n.split(len(n.elems) / 2)
	if n.p == nil {
		n.p = &node{first: n, elems: []Comparable{p}, tree: n.tree}
		nn.p = n.p
		return n.p
	}
	nn.p = n.p
	pos, _ := n.p.elems.shouldBe(p)
	n.p.elems.insertAt(pos, p)
	return n.p.popup()
}

func (n *node) split(pos int) (nn *node, p elem) {
	p = n.elems[pos].(elem)
	nd := make(array, pos)
	copy(nd, n.elems[:pos])
	r := len(n.elems) - pos - 1
	if r <= 0 {
		return
	}
	nnd := make(array, r)
	copy(nnd, n.elems[pos+1:])
	n.elems = nd
	nn = &node{elems: nnd, tree: n.tree}
	for i, p := range nn.elems {
		if p.(elem).after != nil {
			p.(elem).after.p = nn
			nn.elems[i] = p
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
	pos, exactly := n.elems.shouldBe(elem{data: data})
	if exactly {
		ret = n.elems[pos].(elem).data
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
	child := n.elems[pos-1].(elem).after
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
	for _, p := range n.elems {
		t += p.(elem).data.Size(n.elemMax())
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
	for _, p := range n.elems {
		el := p.(elem).data
		size := el.Size(n.elemMax())
		copy(bs[pos:pos+size], el.Bytes(n.elemMax())[0])
		pos += size
		after := 0
		if p.(elem).after != nil {
			p.(elem).after.sync(store)
			after = p.(elem).after.block
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
