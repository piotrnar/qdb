package qdb

import (
	"testing"
	"time"
	"os"
	"bytes"
	mr "math/rand"
	cr "crypto/rand"
)

const (
	dbname = "test"
	oneRound = 10000
	delRound = 1000
)

func getRecSize() int {
	return mr.Intn(4096)
}


func kim(v []byte) bool {
	return (mr.Int63()&1)==0
}


func TestDatabase(t *testing.T) {
	var key KeyType
	var val, v []byte
	var db *DB
	var e error

	os.RemoveAll(dbname)
	mr.Seed(time.Now().UnixNano())

	db, e = NewDB(dbname)
	if e != nil {
		t.Error("Cannot create db")
		return
	}

	// Add oneRound random records
	for i:=0; i<oneRound; i++ {
		vlen := getRecSize()
		val = make([]byte, vlen)
		key = KeyType(mr.Int63())
		cr.Read(val[:])
		db.Put(key, val)
	}
	db.Close()

	// Reopen DB, verify, defrag and close
	db, e = NewDB(dbname)
	if e != nil {
		t.Error("Cannot reopen db")
		return
	}
	db.KeepInMem = kim
	v = db.Get(key)
	if !bytes.Equal(val[:], v[:]) {
		t.Error("Key data mismatch")
	}
	if db.Count() != oneRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Defrag()
	db.Close()


	// Reopen DB, verify, add oneRound more records and Close
	db, e = NewDB(dbname)
	if e != nil {
		t.Error("Cannot reopen db")
		return
	}
	db.KeepInMem = kim
	v = db.Get(key)
	if !bytes.Equal(val[:], v[:]) {
		t.Error("Key data mismatch")
	}
	if db.Count() != oneRound {
		t.Error("Wrong number of records", db.Count())
	}
	for i:=0; i<oneRound; i++ {
		vlen := getRecSize()
		val = make([]byte, vlen)
		key = KeyType(mr.Int63())
		cr.Read(val[:])
		db.Put(key, val)
	}
	db.Close()

	// Reopen DB, verify, defrag and close
	db, e = NewDB(dbname)
	if e != nil {
		t.Error("Cannot reopen db")
		return
	}
	db.KeepInMem = kim
	v = db.Get(key)
	if !bytes.Equal(val[:], v[:]) {
		t.Error("Key data mismatch")
	}
	if db.Count() != 2*oneRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Defrag()
	db.Close()


	// Reopen DB, verify, close...
	db, e = NewDB(dbname)
	db.KeepInMem = kim
	if e != nil {
		t.Error("Cannot reopen db")
		return
	}
	v = db.Get(key)
	if !bytes.Equal(val[:], v[:]) {
		t.Error("Key data mismatch")
	}
	if db.Count() != 2*oneRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Close()

	// Reopen, delete 100 records, close...
	db, e = NewDB(dbname)
	db.KeepInMem = kim
	if e != nil {
		t.Error("Cannot reopen db")
		return
	}

	var keys []KeyType
	db.Browse(func (key KeyType, v []byte) bool {
		keys = append(keys, key)
		return len(keys)<delRound
	})
	for i := range keys {
		db.Del(keys[i])
	}
	db.Close()

	// Reopen DB, verify, close...
	db, e = NewDB(dbname)
	db.KeepInMem = kim
	db.Load()
	if db.Count() != 2*oneRound-delRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Close()

	// Reopen DB, verify, close...
	db, e = NewDB(dbname)
	db.Defrag()
	if db.Count() != 2*oneRound-delRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Close()

	// Reopen DB, verify, close...
	db, e = NewDB(dbname)
	if db.Count() != 2*oneRound-delRound {
		t.Error("Wrong number of records", db.Count())
	}
	db.Close()

	os.RemoveAll(dbname)
}
