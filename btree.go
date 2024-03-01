package main

import (
	"bytes"
	"encoding/binary"
)

// node format
// | type | nkeys |    pointers    |    offsets    | key-values
// | 2B   | 2B    |   nkeys * 8B   |   nkeys * 2B  | ...

// key-value format
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const (
  CMP_GE = +3 // >=
  CMP_GT = +2 // >
  CMP_LT = -2 // <
  CMP_LE = -3 //<=
)

func assert(cond bool) {
	if !cond {
    panic("assertion failure")
  }
}

func init() {
  node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
  assert(node1max <= BTREE_PAGE_SIZE)
}

const (
  BNODE_NODE = 1 //internal nodes without values
  BNODE_LEAF = 2 // leaf nodes with values
)

type BNode struct {
  data []byte // can be dumped to disk
}

type BTree struct {
  // pointer (a non zero page number)
  root uint64
  // callbacks for managing on disk pages
  get func(uint64) BNode // dereference a pointer
  new func(BNode) uint64 // allocate a new page
  del func(uint64)       // deallocate a page
}

//header
func (node BNode) btype() uint16 {
  return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
  return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
  binary.LittleEndian.PutUint16(node.data[0:2], btype)
  binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
  assert(idx < node.nkeys())
  pos := HEADER + 8*idx
  return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
  assert(idx < node.nkeys())
  pos := HEADER + 8*idx
  binary.LittleEndian.PutUint64(node.data[pos:], val)
}

//offset list
func offsetPos(node BNode, idx uint16) uint16 {
  assert(1 <= idx && idx <= node.nkeys())
  return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
  if idx == 0 {
    return 0
  }
  return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
  binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
  assert(idx <= node.nkeys())
  return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
  assert(idx < node.nkeys())
  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node.data[pos:])
  return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
  assert(idx < node.nkeys())

  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node.data[pos+0:])
  vlen := binary.LittleEndian.Uint16(node.data[pos+2:])

  return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
  return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func nodeLookupLe(node BNode, key []byte) uint16 {
  nkeys := node.nkeys()
  found := uint16(0)

  // the first key is a copy from the parent node
  // its is always less than or equal to the key
  for i := uint16(1); i < nkeys; i++ {
    cmp := bytes.Compare(node.getKey(i), key)

    if cmp <= 0 {
      found = i
    }

    if cmp >= 0 {
      break
    }
  }

  return found
}

// copy multiple kvs into position
func nodeAppendRange(
  new BNode, old BNode,
  dstNew uint16, srcOld uint16, n uint16,
) {
  assert(srcOld + n <= old.nkeys())
  assert(dstNew + n <= new.nkeys())

  if n == 0 {
    return
  }

  // pointers
  for i := uint16(0); i < n; i++ {
    new.setPtr(dstNew + i, old.getPtr(srcOld + i))
  }

  //offsets
  dstBegin := new.getOffset(dstNew)
  srcBegin := old.getOffset(srcOld)

  for i := uint16(1); i <= n; i++ {
    // NOTE: the range is [1, n]
    offset := dstBegin + old.getOffset(srcOld + i) - srcBegin
    new.setOffset(dstNew + i, offset)
  }

  // kvs
  begin := old.kvPos(srcOld)
  end := old.kvPos(srcOld + n)
  copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

//copy a kv into the position
func nodeAppendKv(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
  // ptrs
  new.setPtr(idx, ptr)

  // kvs
  pos := new.kvPos(idx)
  binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
  binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
  copy(new.data[pos+4:], key)
  copy(new.data[pos+4+uint16(len(key)):], val)
  
  // the offset of the next key
  new.setOffset(idx + 1, new.getOffset(idx + 4 + uint16((len(key) + len(val)))))
}

// replace a link with the same key
func nodeReplaceKid1ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	copy(new.data, old.data[:old.nbytes()])
	new.setPtr(idx, ptr) // only the pointer is changed
}

//replace a link with multiple links
func nodeReplaceKidN(
  tree *BTree, new BNode, old BNode,
  idx uint16, kids ...BNode,
) {
  inc := uint16(len(kids))

  new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
  nodeAppendRange(new, old, 0, 0, idx)

  for i, node := range kids {
    nodeAppendKv(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
  }

  nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16,
	ptr uint64, key []byte,
) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKv(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// the left half may be still too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// split a node if its too big, the results are 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
  if old.nbytes() <= BTREE_PAGE_SIZE {
    old.data = old.data[:BTREE_PAGE_SIZE]
    return 1, [3]BNode{old}
  }

  left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
  right := BNode{make([]byte, BTREE_PAGE_SIZE)}

  nodeSplit2(left, right, old)

  if left.nbytes() <= BTREE_PAGE_SIZE {
    left.data = left.data[:BTREE_PAGE_SIZE]
    return 2, [3]BNode{left, right}
  }

  // the lefty node is still too large
  leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
  middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
  nodeSplit2(left, middle, left)
  assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
  return 3, [3]BNode{leftleft, middle, right}
}

//. add a new key to a leaf node
func leafInsert(
  new BNode, old BNode, idx uint16,
  key []byte, val []byte,
) {
  new.setHeader(BNODE_LEAF, old.nkeys() + 1)
  nodeAppendRange(new, old, 0, 0, idx)
  nodeAppendKv(new, idx, 0, key, val)
  nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// update an existing key from a leaf node
func leafUpdate(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKv(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// insert a kv into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(req *InsertReq, node BNode) BNode {
  //// the result node
  // allowed to be bigger than 1 page and will be split if so 
  new := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}

  // where to insert the key
  idx := nodeLookupLe(node, req.Key);

  // act depending on the nodetype
  switch node.btype() {
  case BNODE_LEAF:
  // leaf, node.getKey(idx) <= key
    if bytes.Equal(req.Key, node.getKey(idx)) {
      // found the key, update it
      if req.Mode == MODE_INSERT_ONLY {
        return BNode{}
      }

      if bytes.Equal(req.Val, node.getVal(idx)) {
        return BNode{}
      }

      leafUpdate(new, node, idx, req.Key, req.Val)
    } else {
      // insert it after the position

      if req.Mode == MODE_UPDATE_ONLY {
        return BNode{}
      }

      leafInsert(new, node, idx + 1, req.Key, req.Val)
      req.Added = true
    }
  case BNODE_NODE:
    // internal node, insert it to a kid nod3
    return nodeInsert(req, new, node, idx)
  default:
    panic("bad node!")
  }

  return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(req *InsertReq, new BNode, node BNode, idx uint16) BNode {

  // recursive indertion to the kid node
  kptr := node.getPtr(idx)
  updated := treeInsert(req, req.tree.get(kptr))
  if len(updated.data) == 0 {
    return BNode{}
  }

  // deallocate the kidnode
  req.tree.del(kptr)
  // split the result
  nsplit, splited := nodeSplit3(updated)
  //update the kid links
  nodeReplaceKidN(req.tree,new, node, idx, splited[:nsplit]...)
  return new
}

//remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
  new.setHeader(BNODE_LEAF, old.nkeys()-1)
  nodeAppendRange(new, old, 0, 0, idx)
  nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
  // where to find the key
  idx := nodeLookupLe(node, key)
  // act depending on the node type
  switch node.btype() {
    case BNODE_LEAF:
      if !bytes.Equal(key, node.getKey(idx)) {
        return BNode{} //not found
      }
      //delete the key in the leaf
      new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
      leafDelete(new, node, idx)
      return new 
    case BNODE_NODE:
      return nodeDelete(tree, node, idx, key)
    default:
      panic("bad node!")
  }
}

// part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
  // recurse into the kid
  kptr := node.getPtr(idx);
  updated := treeDelete(tree, tree.get(kptr), key)

  if (len(updated.data) == 0) {
    return BNode{} // not found
  }

  tree.del(kptr)

  new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

  // check for merging
  mergeDir, sibling := shouldMerge(tree, node, idx, updated)
  switch {
  case mergeDir < 0:
    merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    nodeMerge(merged, sibling, updated)
    tree.del(node.getPtr(idx - 1))
    nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
  case mergeDir > 0:
    merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    nodeMerge(merged, updated, sibling)
    tree.del(node.getPtr(idx + 1))
    nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
  case mergeDir == 0:
    if updated.nkeys() == 0 {
      // kid is emopty after deletion and has no sibling to merge with
      // happens when its partent has only one kid
      // discard the empty kid and return the parent as an empty node 
      assert(node.nkeys() == 1 && idx == 0)
      new.setHeader(BNODE_NODE, 0)
      // the empty node will be eliminated befor reaching root
    } else {
      nodeReplaceKidN(tree, new, node, idx, updated)
    }
  }
  return new
}

// merge 2 nodes into 1 
func nodeMerge(new BNode, left BNode, right BNode) {
  new.setHeader(left.btype(), left.nkeys()+right.nkeys())
  nodeAppendRange(new, left, 0, 0, left.nkeys())
  nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// should the updated kid be merged with a sibling
func shouldMerge(
  tree *BTree, node BNode,
  idx uint16, updated BNode,
) (int, BNode) {
  if updated.nbytes() > BTREE_PAGE_SIZE / 4 {
    return 0, BNode{}
  }

  if idx > 0 {
    sibling := tree.get(node.getPtr(idx - 1))
    merged := sibling.nbytes() + updated.nbytes() - HEADER

    if merged <= BTREE_PAGE_SIZE {
      return -1, sibling
    }
  }

  if idx + 1 < node.nkeys() {
    sibling := tree.get(node.getPtr(idx + 1))
    merged := sibling.nbytes() + updated.nbytes() - HEADER
    if merged <= BTREE_PAGE_SIZE {
      return +1, sibling
    }
  }

  return 0, BNode{}
}

func (tree *BTree) Delete(key []byte) bool {
  assert(len(key) != 0)
  assert(len(key) <= BTREE_MAX_KEY_SIZE)

  if tree.root == 0 {
    return false
  }

  updated := treeDelete(tree, tree.get(tree.root), key)
  if len(updated.data) == 0 {
    return false // not found
  }

  tree.del(tree.root)
  if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
    // remove a level
    tree.root = updated.getPtr(0)
  } else {
    tree.root = tree.new(updated)
  }

  return true

}

func (tree *BTree) Insert(key []byte, val []byte) bool {
  req := &InsertReq{Key: key, Val: val}
  tree.InsertEx(req)
  return req.Added
}

func (tree *BTree) InsertEx(req *InsertReq) {
  assert(len(req.Key) != 0)
  assert(len(req.Key) <= BTREE_MAX_KEY_SIZE)
  assert(len(req.Val) <= BTREE_MAX_VAL_SIZE)

  if tree.root == 0 {
    // create the first node
    root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    root.setHeader(BNODE_LEAF, 2)
    
    // a dummy key, this makes the tree cover the whole key space
    // thuis a lokkup can always find a containing node
    nodeAppendKv(root, 0, 0, nil, nil)
    nodeAppendKv(root, 1, 0, req.Key, req.Val)
    tree.root = tree.new(root)
    req.Added = true
  }

  req.tree = tree
  updated := treeInsert(req, tree.get(tree.root))
  if len(updated.data) == 0 {
    return
  }

  // replace the root node
  tree.del(tree.root)
  nsplit, splitted := nodeSplit3(updated)
  if nsplit > 1 {
    // the root was split, add a new level
    root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    root.setHeader(BNODE_NODE, nsplit)

    for i, knode := range splitted[:nsplit] {
      ptr, key := tree.new(knode), knode.getKey(0)
      nodeAppendKv(root, uint16(i), ptr, key, nil)
    }
    tree.root = tree.new(root)
  } else {
    tree.root = tree.new(splitted[0])
  }
}

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
  idx := nodeLookupLe(node, key)
  switch node.btype() {
  case BNODE_LEAF:
    if bytes.Equal(key, node.getKey(idx)) {
      return node.getVal(idx), true
    } else {
      return nil, false
    }
  case BNODE_NODE:
    return nodeGetKey(tree, tree.get(node.getPtr(idx)), key)
  default:
    panic("bad nodeQ")
  }
}

// key cmp ref
func cmpOK(key []byte, cmp int, ref []byte) bool {
  r := bytes.Compare(key, ref)
  switch cmp {
  case CMP_GE:
    return r >= 0
  case CMP_GT:
    return r > 0
  case CMP_LE:
    return r <= 0
  case CMP_LT:
    return r < 0
  default:
    panic("what?")
  }
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
  if tree.root == 0 {
    return nil, false
  }
  return nodeGetKey(tree, tree.get(tree.root), key)
}

func (tree *BTree) SeekLE(key []byte) *BIter {
  iter := &BIter{tree: tree}

  for ptr := tree.root; ptr != 0; {
    node := tree.get(ptr)
    idx := nodeLookupLe(node, key)
    iter.path = append(iter.path, node)
    iter.pos = append(iter.pos, idx)

    if node.btype() == BNODE_NODE {
      ptr = node.getPtr(idx)
    } else {
      ptr = 0
    }
  }
  return iter
}

// find the closet position to a key with respect to the `cmp` relation
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
  iter := tree.SeekLE(key)

  if cmp != CMP_LE && iter.Valid() {
    cur, _ := iter.Deref()

    if !cmpOK(cur, cmp, key) {
      // off by one
      if cmp > 0 {
        iter.Next()
      } else {
        iter.Prev()
      }
    }
  }
  return iter
}