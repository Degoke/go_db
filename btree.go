package main

import (
	"bytes"
	"encoding/binary"
)

type BTree struct {
  // pointer (a non zero page number)
  root uint64
  // callbacks for managing on disk pages
  get func(uint64) BNode // dereference a pointer
  new func(BNode) uint64 // allocate a new page
  del func(uint64)       // deallocate a page
}

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

// insert a kv into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
  //// the result node
  // allowed to be bigger than 1 page and will be split if so 
  new := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}

  // where to insert the key
  idx := nodeLookupLe(node, key);

  // act depending on the nodetype
  switch node.btype() {
  case BNODE_LEAF:
  // leaf, node.getKey(idx) <= key
    if bytes.Equal(key, node.getKey(idx)) {
      // found the key, update it
      leafUpdate(new, node, idx, key, val)
    } else {
      // insert it after the position
      leafInsert(new, node, idx + 1, key, val)
    }
  case BNODE_NODE:
    // internal node, insert it to a kid nod3
    nodeInsert(tree, new, node, idx, key, val)
  default:
    panic("bad node!")
  }

  return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
  tree *BTree, new BNode, node BNode, idx uint16,
  key []byte, val []byte,
) {
  // get and deallocate the kid node
  kptr := node.getPtr(idx)
  knode := tree.get(kptr)
  tree.del(kptr)

  // recursive indertion to the kid node
  knode = treeInsert(tree, knode, key, val)
  // split the result
  nsplit, splitted := nodeSplit3(knode)
  // update the kid links
  nodeReplaceKidN(tree, new, node, idx, splitted[:nsplit]...)
}

// ,split a bigger than allowed node into 2
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
  //
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

func (tree *BTree) Insert(key []byte, val []byte) {
  assert(len(key) != 0)
  assert(len(key) <= BTREE_MAX_KEY_SIZE)
  assert(len(key) <= BTREE_MAX_VAL_SIZE)

  if tree.root == 0 {
    // create the first node
    root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    root.setHeader(BNODE_LEAF, 2)

    // a dummy key this makes the tree cover the whole key space
    // thius the lookup can always find a containing node

    nodeAppendKv(root, 0, 0, nil, nil)
    nodeAppendKv(root, 1, 0, key, val)

    tree.root = tree.new(root)
    return
  }

  node := tree.get(tree.root)
  tree.del(tree.root)

  node = treeInsert(tree, node, key, val)
  nsplit, splitted := nodeSplit3(node)
  if nsplit > 1 {
    // the root was split add a new level
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
