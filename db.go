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


type oneIdx struct {
	data []byte
	fpos int64 // negative values point to the logfile
}

type DB struct {
	pathname string

	mutex sync.Mutex
	index map[KeyType] *oneIdx
	file_index int // can be only 0 or 1
	version_seq uint32

	logfile *os.File
	datfile *os.File
	lastvalidlogpos int64

	nosync bool
	dirty bool

	// If NeverKeepInMem is set to true, the engine will never keep DB records
	// in memory, but will laways need to read them from disk.
	NeverKeepInMem bool

	// Set this function if you want to be able to decide whether a specific
	// record should be kept in memory, or freed after loaded, thus will need
	// to be taken from disk whenever needed next time.
	KeepInMem func(v []byte) bool
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
	l = len(db.index)
	db.mutex.Unlock()
	return
}


// Browses through all teh DB records calling teh walk function for each record.
// If the walk function returns false, it aborts the browsing and returns.
func (db *DB) Browse(walk func(key KeyType, value []byte) bool) {
	db.mutex.Lock()
	db.load()
	for k, v := range db.index {
		d := v.data
		if d == nil {
			d = db.loadrec(v.fpos)
		}
		if !walk(k, d) {
			break
		}
	}
	db.mutex.Unlock()
}


// Fetches record with a given key. Returns nil if no such record.
func (db *DB) Get(key KeyType) (value []byte) {
	db.mutex.Lock()
	db.load()
	if idx, ok := db.index[key]; ok {
		if idx.data == nil {
			value = db.loadrec(idx.fpos)
		} else {
			value = idx.data
		}
	}
	db.mutex.Unlock()
	return
}


// Adds or updates record with a given key.
func (db *DB) Put(key KeyType, value []byte) {
	db.mutex.Lock()
	if db.nosync {
		db.dirty = true
		db.load()
		db.index[key] = &oneIdx{data:value}
	} else {
		fpos := db.addtolog(key, value)
		if db.index != nil {
			db.index[key] = &oneIdx{data:value, fpos:-fpos}
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
		delete(db.index, key)
	} else {
		db.deltolog(key)
		if db.index != nil {
			delete(db.index, key)
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
		db.logfile = nil
	}
	if db.datfile != nil {
		db.datfile.Close()
		db.datfile = nil
	}
	db.index = nil
	db.mutex.Unlock()
}


func (db *DB) load() {
	if db.index == nil {
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
