// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Qdb is a fast persistent storage database.

The records are binary blobs that can have a variable length, up to 4GB.

The key must be a unique 64-bit value, most likely a hash of the actual key.

They data is stored on a disk, in a folder specified during the call to NewDB().
There are can be three possible files in that folder
 * qdb.0, qdb.1 - these files store a compact version of the entire database
 * qdb.log - this one stores the changes since the most recent qdb.0 or qdb.1

*/
package qdb

import (
	"os"
	"sync"
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


// Creates or opens a new database in the specified folder.
func NewDB(dir string) (db *DB, e error) {
	db = new(DB)
	if len(dir)>0 && dir[len(dir)-1]!='\\' && dir[len(dir)-1]!='/' {
		dir += "/"
	}
	e = os.MkdirAll(dir, 0770)
	db.pathname = dir+"qdb."
	return
}


// Loads the data from the disk into memory (cache)
func (db *DB) Load() {
	db.mutex.Lock()
	db.load()
	db.mutex.Unlock()
}


// Returns number of records in the DB
func (db *DB) Count() (l int) {
	db.mutex.Lock()
	db.load()
	l = len(db.cache)
	db.mutex.Unlock()
	return
}


// Browses through all teh DB records calling teh walk function for each record.
// If the walk function returns false, it aborts the browsing and returns.
func (db *DB) Browse(walk func(key KeyType, value []byte) bool) {
	db.mutex.Lock()
	db.load()
	for k, v := range db.cache {
		if !walk(k, v) {
			break
		}
	}
	db.mutex.Unlock()
}


// Fetches record with a given key. Returns nil if no such record.
func (db *DB) Get(key KeyType) (value []byte) {
	db.mutex.Lock()
	db.load()
	value, _ = db.cache[key]
	db.mutex.Unlock()
	return
}


// Adds or updates record with a given key.
func (db *DB) Put(key KeyType, value []byte) {
	db.mutex.Lock()
	if db.nosync {
		db.dirty = true
		db.load()
		db.cache[key] = value
	} else {
		db.addtolog(key, value)
		if db.cache != nil {
			db.cache[key] = value
		}
	}
	db.mutex.Unlock()
}


// Removes record with a given key.
func (db *DB) Del(key KeyType) {
	//println("del", hex.EncodeToString(key[:]))
	db.mutex.Lock()
	if db.nosync {
		db.dirty = true
		db.load()
		delete(db.cache, key)
	} else {
		db.deltolog(key)
		if db.cache != nil {
			delete(db.cache, key)
		}
	}
	db.mutex.Unlock()
}


// Defragments the DB on the disk.
// Return true if defrag hes been performed, and false if was not needed.
func (db *DB) Defrag() (doing bool) {
	db.mutex.Lock()
	doing = db.logfile != nil
	if doing {
		go func() {
			db.load()
			db.logfile.Close()
			db.logfile = nil
			db.savefiledat()
			db.mutex.Unlock()
		}()
	} else {
		db.mutex.Unlock()
	}
	return
}


// Disable writing changes to disk.
func (db *DB) NoSync() {
	db.mutex.Lock()
	db.nosync = true
	db.mutex.Unlock()
}


// Write all the pending changes to disk now.
// Re enable syncing if it has been disabled.
func (db *DB) Sync() {
	db.mutex.Lock()
	db.sync()
	db.mutex.Unlock()
}


// Close the database.
// Writes all the pending changes to disk.
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
