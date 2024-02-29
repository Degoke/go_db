package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const DB_SIG = "GoDbByDegoke0000"

type KV struct {
	Path string
	//internals
	fp *os.File
	tree BTree
	free FreeList
	mmap struct {
		file int // file size, can be larger than the database size
		total int // nmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps can be  non-contigous
	}
	page struct {
		flushed uint64 // database size in number o f pages
		nfree int // number of pages taken from the free list
		nappend int //number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
}

// callback for BTree & FreeList, dereference a pointer
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		assert(page != nil)
		return BNode{page} // for new pages
	}
	return pageGetMapped(db, ptr)
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk)) / BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset + BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

// callback for Btree, allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := uint64(0)

	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree ++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for BTree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// callback for FreeList allocate a new page
func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

// callback fro FreeList reuse a page
func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

func (db *KV) Open() error {
	// open or create the db file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("openfile: %W", err)
	}

	db.fp = fp

	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil  {
		goto fail
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// btree callbaacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	return nil

fail:
	db.Close()
	return fmt.Errorf("kv open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}
	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error)

//persist the newly allocated pages after upmdates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	// update the FreeList
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	// extend the file and mmap if nee	// TODO: implemet this
	npages := int(db.page.flushed) + db.page.nappend
	if err := extendFile(db, npages); err != nil {
		return err
	}
	
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy pages to the file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).data, page)
		}
	}

	return nil
}

func syncPages(db *KV) error {
	// flush data to the disk, must be done before updating the master page
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(db.page.nappend)
	db.page.nfree = 0
	db.page.nappend = 0
	db.page.updates = map[uint64][]byte{}

	// update and flush the master page
	if err := masterStore(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}
 
// extend the map by adding new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages * BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// create the initial mmap that covers the entire file
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size() % BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20
	assert(mmapSize % BTREE_PAGE_SIZE == 0)

	for mmapSize < int(fi.Size()) {
		mmapSize += 2
	}
	// nmapSize can be larger than the file

	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("map: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

// the master page format
// it contains the poiunter to the root and other important bits
// | sig | btree_root | page_used |
// | 16B | 	   8B     |     8B    |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write
		db.page.flushed = 1 // reserved for the master page
		return nil;
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file / BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)

	if bad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.page.flushed = used
	return nil
}

// update the master page, it must be atomic
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	
	binary.LittleEndian.PutUint64(data[:16], db.tree.root)
	binary.LittleEndian.PutUint64(data[:24], db.page.flushed)

	// updating the page via mmap is not atomic, use the `pwrite` syscall instead
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// exktend the file to atleast `npages`
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// the file size is increased exponentially
		// so that we dont have to extend the file for every update
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}