package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	is "github.com/stretchr/testify/require"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data == nil)
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) nodeDump(ptr uint64, keys *[]string, vals *[]string) {
	node := c.tree.get(ptr)
	nkeys := node.nkeys()
	if node.btype() == BNODE_LEAF {
		for i := uint16(0); i < nkeys; i++ {
			*keys = append(*keys, string(node.getKey(i)))
			*vals = append(*vals, string(node.getVal(i)))
		}
	} else {
		for i := uint16(0); i < nkeys; i++ {
			ptr := node.getPtr(i)
			c.nodeDump(ptr, keys, vals)
		}
	}
}

func (c *C) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}
	c.nodeDump(c.tree.root, &keys, &vals)
	assert(keys[0] == "")
	assert(vals[0] == "")
	return keys[1:], vals[1:]
}

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (self sortIF) Len() int {
	return self.len
}
func (self sortIF) Less(i, j int) bool {
	return self.less(i, j)
}
func (self sortIF) Swap(i, j int) {
	self.swap(i, j)
}

func (c *C) verify(t *testing.T) {
	keys, vals := c.dump()

	rkeys, rvals := []string{}, []string{}
	for k, v := range c.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}
	is.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			k, v := rkeys[i], rvals[i]
			rkeys[i], rvals[i] = rkeys[j], rvals[j]
			rkeys[j], rvals[j] = k, v
		},
	})

	is.Equal(t, rkeys, keys)
	is.Equal(t, rvals, vals)

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nkeys := node.nkeys()
		assert(nkeys >= 1)
		if node.btype() == BNODE_LEAF {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			key := node.getKey(i)
			kid := c.tree.get(node.getPtr(i))
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(c.tree.get(c.tree.root))
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func TestBTreeBasic(t *testing.T) {
	c := newC()
	c.add("k", "v")
	c.verify(t)

	// insert
	for i := 0; i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)

	// del
	for i := 2000; i < 250000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
	}
	c.verify(t)

	// overwrite
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		c.add(key, val)
		c.verify(t)
	}
	for i := 0; i < 2000; i++ {
		root := c.tree.root
		c.add("k", "v")
		is.Equal(t, root, c.tree.root)
	}

	is.False(t, c.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
		c.verify(t)
	}

	c.add("k", "v2")
	c.verify(t)
	c.del("k")
	c.verify(t)

	// the dummy empty key
	is.Equal(t, 1, len(c.pages))
	is.Equal(t, uint16(1), c.tree.get(c.tree.root).nkeys())
}

func TestBTreeRandLength(t *testing.T) {
	c := newC()
	for i := 0; i < 2000; i++ {
		klen := fmix32(uint32(2*i+0)) % BTREE_MAX_KEY_SIZE
		vlen := fmix32(uint32(2*i+1)) % BTREE_MAX_VAL_SIZE
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		// rand.Read(val)
		c.add(string(key), string(val))
		c.verify(t)
	}
}

func TestBTreeIncLength(t *testing.T) {
	for l := 1; l < BTREE_MAX_KEY_SIZE+BTREE_MAX_VAL_SIZE; l++ {
		c := newC()

		klen := l
		if klen > BTREE_MAX_KEY_SIZE {
			klen = BTREE_MAX_KEY_SIZE
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := BTREE_PAGE_SIZE / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			rand.Read(key)
			c.add(string(key), string(val))
		}
		c.verify(t)
	}
}

func TestBTreeEmptyNode(t *testing.T) {
	rng := uint32(0)
	gen := func() uint32 {
		rng++
		return fmix32((rng))
	}

	n2k := func(n uint32) string {
		s := make([]byte, BTREE_MAX_KEY_SIZE)
		binary.BigEndian.PutUint32(s, n)
		return string(s)
	}
	k2n := func(key []byte) uint32 {
		if len(key) == 0 {
			return 0
		}
		return binary.BigEndian.Uint32(key)
	}

	c := newC()
	level := func() int {
		n := 1
		node := c.tree.get(c.tree.root)
		for node.btype() == BNODE_NODE {
			node = c.tree.get(node.getPtr(0))
			n++
		}
		return n
	}

	c.add(n2k(gen()), "")
	for level() < 3 {
		c.add(n2k(gen()), "")
	}

	path2node := func(path []uint16) BNode {
		node := c.tree.get(c.tree.root)
		for _, idx := range path {
			ptr := node.getPtr(idx)
			node = c.tree.get(ptr)
		}
		return node
	}

	add_keys := func(path []uint16, sz uint16) {
		node := path2node(path)
		for node.nkeys() < sz {
			assert(node.nkeys() >= 2)
			k1, k2 := k2n(node.getKey(0)), k2n(node.getKey(node.nkeys()-1))
			n := gen()%(k2-k1-1) + k1 + 1
			c.add(n2k(n), "")

			node = path2node(path)
		}
	}

	add_keys(nil, 3)
	add_keys([]uint16{0}, 5)
	add_keys([]uint16{2}, 4)
	assert(level() == 3 && path2node([]uint16{1}).btype() == BNODE_NODE)
	c.verify(t)

	// log := func() {
	// 	n := path2node(nil).nkeys()
	// 	t.Logf("nkeys: %v", n)
	// 	for i := uint16(0); i < n; i++ {
	// 		t.Logf("nkeys %v: %v", i, path2node([]uint16{i}).nkeys())
	// 	}
	// 	t.Logf("")
	// }

	keys, vals := []string{}, []string{}
	mid := c.tree.get(c.tree.root).getPtr(1)
	c.nodeDump(mid, &keys, &vals)
	for _, k := range keys {
		c.del(k)
		// log()
	}
	c.verify(t)
}
