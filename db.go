package qdb

import (
	"os"
	"sync"
//	"encoding/hex"
//	"errors"
)


type KeyType uint64
const KeySize = 8


type DB struct {
	pathname string
	
	mutex sync.Mutex
	cache map[KeyType] []byte
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
	db.mutex.Lock()
	db.load()
	db.mutex.Unlock()
}


func (db *DB) Count() (l int) {
	db.mutex.Lock()
	db.load()
	l = len(db.cache)
	db.mutex.Unlock()
	return 
}


func (db *DB) Browse(walk func(k KeyType, v []byte) bool) {
	db.mutex.Lock()
	db.load()
	for k, v := range db.cache {
		if !walk(k, v) {
			break
		}
	}
	db.mutex.Unlock()
}


func (db *DB) Get(key KeyType) (val []byte) {
	db.mutex.Lock()
	db.load()
	val, _ = db.cache[key]
	db.mutex.Unlock()
	return
}


func (db *DB) Put(key KeyType, val []byte) {
	//println("put", hex.EncodeToString(key[:]))
	db.mutex.Lock()
	if db.nosync {
		db.dirty = true
		db.load()
		db.cache[key] = val
		db.mutex.Unlock()
	} else {
		go func() {
			db.addtolog(key, val)
			if db.cache != nil {
				db.cache[key] = val
			}
			db.mutex.Unlock()
		}()
	}
}


func (db *DB) Del(key KeyType) {
	//println("del", hex.EncodeToString(key[:]))
	db.mutex.Lock()
	if db.nosync {
		db.dirty = true
		db.load()
		delete(db.cache, key)
		db.mutex.Unlock()
	} else {
		go func() {
			db.deltolog(key)
			if db.cache != nil {
				delete(db.cache, key)
			}
			db.mutex.Unlock()
		}()
	}
}


// Return true if defrag hes been performed, false if was not needed
func (db *DB) Defrag() (doing bool) {
	db.mutex.Lock()
	doing = db.logfile != nil
	if doing {
		go func() {
			db.load()
			db.savefiledat()
			db.logfile.Close()
			db.logfile = nil
			db.mutex.Unlock()
		}()
	} else {
		db.mutex.Unlock()
	}
	return
}


func (db *DB) NoSync() {
	db.mutex.Lock()
	db.nosync = true
	db.mutex.Unlock()
}


func (db *DB) Sync() {
	db.mutex.Lock()
	db.sync()
	db.mutex.Unlock()
}


func (db *DB) Close() {
	db.mutex.Lock()
	db.sync()
	if db.logfile != nil {
		db.logfile.Close()
	}
	db.cache = nil
	db.mutex.Unlock()
}


func (db *DB) load() {
	if db.cache == nil {
		db.loadfiledat()
		db.loadfilelog()
	}
}

func (db *DB) sync() {
	db.nosync = false
	if db.dirty {
		db.dirty = false
		db.savefiledat()
	} else if db.logfile != nil {
		db.logfile.Sync()
	}
}

