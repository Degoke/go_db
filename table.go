package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// modes of the updates
const (
	MODE_UPSERT = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

const TABLE_PREFIX_MIN = 100

// table cell
type Value struct {
	Type uint32
	I64 int64
	Str []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(key string, val []byte) *Record
func (rec *Record) AddInt64(key string, val int64) *Record
func (rec *Record) Get(key string) *Value

type DB struct {
	Path string
	// internals
	kv KV
	tables map[string]*TableDef // cached table definition
}

// get a singlr row by its primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func tableDefCheck(tableDef *TableDef) error {
	// cverify te table definition
	bad := tableDef.Name == "" || len(tableDef.Cols) == 0
	bad = bad || len(tableDef.Cols) != len(tableDef.Types)
	bad = bad || !(1 <= tableDef.PKeys && int(tableDef.PKeys) <= len(tableDef.Cols))

	if bad {
		return fmt.Errorf("bad table definition: %s", tableDef.Name)
	}

	return nil
}

func (db *DB) TableNew(tdef *TableDef) (error) {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	assert(err == nil)

	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	assert(tdef.Prefix == 0)
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_METADATA, meta)
	assert(err == nil)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix + 1)
	_, err = dbUpdate(db, TDEF_METADATA, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	assert(err == nil)
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

// table definition
type TableDef struct {
	// user defined
	Name string
	Types []uint32 // column types
	Cols []string // column names
	PKeys int // the first `PKeys` columns are the primary key
	// auto assigned B-tree key prefixes for different tables
	Prefix uint32
}

type InsertReq struct {
	tree *BTree
	// out
	Added bool // added a new key
	// in
	Key []byte
	Val []byte
	Mode int
}

// internal table: metadata
var TDEF_METADATA = &TableDef{
	Prefix: 1,
	Name: "@meta",
	Types: []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols: []string{"key", "val"},
	PKeys: 1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name: "@table",
	Types: []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols: []string{"name", "def"},
	PKeys: 1,
}

// rearrange a record to the specified column type
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	assert(len(rec.Cols) == len(rec.Vals))
	out := make([]Value, len(tdef.Cols))
	for i, c := range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue // leave this column uninitialized
		}

		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] = *v
	}

	return out, nil
}

func encodeValues(out []byte, vals []Value) []byte
func decodeValues(in []byte, out []Value)

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func valuesComplete(tdef *TableDef, values []Value, n int) error {
	for i, v := range values {
		if i < n && v.Type == 0 {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != 0 {
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}
	return nil
}

// reorder a record and check for missing values
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	values, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}
	err = valuesComplete(tdef, values, n)
	if err != nil {
		return nil, err
	}
	return values, nil
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)

	return true, nil
}

// get the table definition by name
func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert (err == nil)
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil)
	return tdef
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])

	return db.kv.Update(key, val, mode)
}

// delete a record byu its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}