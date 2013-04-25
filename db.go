package qdb

import (
	"os"
//	"encoding/hex"
//	"errors"
)


const KeySize = 8


type DB struct {
	pathname string
	
	Cache map[[KeySize]byte] []byte
	file_index int // can be only 0 or 1
	version_seq uint32
	
	logfile *os.File

	nosync bool
	dirty bool
}


// Opens a new database
func NewDB(dir string) (db *DB, e error) {
	db = new(DB)
	if len(dir)>0 && dir[len(dir)-1]!='\\' && dir[len(dir)-1]!='/' {
		dir += "/"
	}
	e = os.MkdirAll(dir, 0770)
	db.pathname = dir+"qdb."
	return
}


func (db *DB) Load() {
	if db.Cache == nil {
		db.loadfiledat()
		db.loadfilelog()
	}
}


func (db *DB) Count() int {
	db.Load()
	return len(db.Cache)
}


func (db *DB) FetchAll(walk func(k, v []byte) bool) {
	db.Load()
	for k, v := range db.Cache {
		if !walk(k[:], v) {
			break
		}
	}
}


func (db *DB) Get(key [KeySize]byte) (val []byte, e error) {
	db.Load()
	val, _ = db.Cache[key]
	return
}


func (db *DB) Put(key [KeySize]byte, val []byte) (e error) {
	//println("put", hex.EncodeToString(key[:]))
	if db.nosync {
		db.dirty = true
	} else {
		db.addtolog(key[:], val)
	}
	if db.Cache != nil {
		db.Cache[key] = val
	}
	return
}


func (db *DB) Del(key [KeySize]byte) (e error) {
	//println("del", hex.EncodeToString(key[:]))
	if db.nosync {
		db.dirty = true
	} else {
		db.deltolog(key[:])
	}
	if db.Cache != nil {
		delete(db.Cache, key)
	}
	return
}


func (db *DB) Defrag() (e error) {
	db.Load()
	if db.logfile != nil {
		db.logfile.Close()
		db.logfile = nil
		e = db.savefiledat()
	}
	return
}


func (db *DB) NoSync() {
	db.nosync = true
}


func (db *DB) Sync() {
	db.nosync = false
	if db.dirty {
		db.dirty = false
		db.savefiledat()
	} else if db.logfile != nil {
		db.logfile.Sync()
	}
}


func (db *DB) Close() {
	db.Sync()
	if db.logfile != nil {
		db.logfile.Close()
	}
	db.Cache = nil
}
