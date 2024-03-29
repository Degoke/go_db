package main

// B-tree iterator
type BIter struct {
	tree *BTree
	path []BNode // from root to leaf
	pos []uint16 // indexes into nodes
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move within this node
	} else if level > 0 {
		iterPrev(iter, level - 1) // move to a sibling node
	} else {
		return // dummy key
	}

	if level + 1 < len(iter.pos) {
		// update the kid node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr((iter.pos[level])))
		iter.path[level + 1] = kid
		iter.pos[level + 1] = kid.nkeys() - 1
	}
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level + 1] < iter.path[level].nkeys() {
		iter.pos[level]++
	} else if level > 0 {
		iterNext(iter, level - 1)
	} else {
		iter.pos[len(iter.pos) - 1]++ // past the last key
		return
	}

	if level + 1 < len(iter.pos) {
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr((iter.pos[level])))
		iter.path[level + 1] = kid
		iter.pos[level + 1] = 0
	}
}

func (iter *BIter) Clone() *BIter {
	return &BIter{
		tree: iter.tree,
		path: append([]BNode(nil), iter.path...),
		pos: append([]uint16(nil), iter.pos...),
	}
}

// get the current kv pair
func (iter *BIter) Deref() ([]byte, []byte) {
	assert(iter.Valid())
	last := len(iter.path) - 1
	node := iter.path[last]
	pos := iter.pos[last]
	return node.getKey(pos), node.getVal(pos)
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	// the first key in the tree is not real: dummy
	dummy := true
	for _, pos := range iter.pos {
		if pos != 0 {
			dummy = false
		}
	}

	if dummy {
		return false
	}

	last := len(iter.path) - 1
	node := iter.path[last]
	return iter.pos[last] < node.nkeys()
}

// moving backward and foward
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path) - 1)
}

func (iter *BIter) Next() {
	iterNext(iter, len(iter.path) - 1)
}

