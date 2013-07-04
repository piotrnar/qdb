package qdb

import (
	"os"
	"fmt"
	"bytes"
	"errors"
	"io/ioutil"
	"encoding/binary"
)

// Opens file and checks the ffffffff-sequence-FINI marker at the end
func openAndGetSeq(fn string) (f *os.File, seq uint32) {
	var b [12]byte
	var e error
	var fpos int64

	if f, e = os.Open(fn); e != nil {
		return
	}

	if fpos, e = f.Seek(-12, os.SEEK_END); e!=nil || fpos<4 {
		println(fn, ":", "openAndGetSeq fseek error", e, fpos)
		f.Close()
		f = nil
		return
	}

	if _, e = f.Read(b[:]); e != nil {
		println(fn, ":", "openAndGetSeq read error", e.Error())
		f.Close()
		f = nil
		return
	}

	if binary.LittleEndian.Uint32(b[0:4])!=0xffffffff || string(b[8:12])!="FINI" {
		println(fn, ":", "openAndGetSeq marker error")
		f.Close()
		f = nil
		return
	}

	seq = binary.LittleEndian.Uint32(b[4:8])
	return
}


// allocate the cache map and loads it from disk
func (db *DB) loadfiledat() (e error) {
	var ks uint32

	db.index = make(map[KeyType] *oneIdx)

	f0, seq := openAndGetSeq(db.pathname+"0")
	f1, seq1 := openAndGetSeq(db.pathname+"1")

	if f0 == nil && f1 == nil {
		e = errors.New("No database")
		return
	}

	if f0!=nil && f1!=nil {
		// Both files are valid - take the one with higher sequence
		if int32(seq - seq1) >= 0 {
			f1.Close()
			os.Remove(db.pathname+"1")
			db.file_index = 0
		} else {
			f0.Close()
			f0 = f1
			os.Remove(db.pathname+"0")
			db.file_index = 1
		}
	} else if f0==nil {
		f0 = f1
		seq = seq1
		db.file_index = 1
	} else {
		db.file_index = 0
	}

	readlimit, _ := f0.Seek(-12, os.SEEK_END)
	f0.Seek(0, os.SEEK_SET)
	dat, _ := ioutil.ReadAll(f0)
	db.datfile = f0

	f := bytes.NewReader(dat)

	db.version_seq = seq

	var key KeyType
	var filepos int64
	for filepos+KeySize+4 <= readlimit {
		e = binary.Read(f, binary.LittleEndian, &key)
		if e != nil {
			break
		}
		e = binary.Read(f, binary.LittleEndian, &ks)
		if e != nil {
			break
		}
		if db.NeverKeepInMem {
			_, e = f.Seek(int64(ks), os.SEEK_CUR)
			if e != nil {
				break
			}
			db.index[key] = &oneIdx{fpos:filepos}
		} else {
			val := make([]byte, ks)
			_, e = f.Read(val[:])
			if e != nil {
				break
			}
			if db.KeepInMem==nil || db.KeepInMem(val) {
				db.index[key] = &oneIdx{data:val, fpos:filepos}
			} else {
				db.index[key] = &oneIdx{fpos:filepos}
			}
		}
		filepos += int64(KeySize+4+ks)
	}

	return
}


func (db *DB) savefiledat() {
	var f *os.File
	new_file_index := 1 - db.file_index
	fname := fmt.Sprint(db.pathname, new_file_index)

	f, _ = os.Create(fname)

	var v []byte
	var fpos int64
	for k, idx := range db.index {
		if idx.data == nil {
			v = db.loadrec(idx.fpos)
		} else {
			v = idx.data
		}

		binary.Write(f, binary.LittleEndian, k)
		binary.Write(f, binary.LittleEndian, uint32(len(v)))
		f.Write(v)

		idx.fpos = fpos
		/*if !db.NeverKeepInMem && (db.KeepInMem==nil || db.KeepInMem(v)) {
			idx.data = v
		}*/
		fpos += KeySize+4+int64(len(v))
	}

	f.Write([]byte{0xff,0xff,0xff,0xff})
	binary.Write(f, binary.LittleEndian, uint32(db.version_seq+1))
	f.Write([]byte("FINI"))
	f.Sync()

	if db.datfile!=nil {
		db.datfile.Close()
		os.Remove(fmt.Sprint(db.pathname, db.file_index))
	}
	db.datfile = f

	if db.logfile!=nil {
		db.logfile.Close()
		os.Remove(db.pathname+"log")
		db.logfile = nil
	}

	db.version_seq++
	db.file_index = new_file_index
}


func (db *DB) loadrec(fpos int64) (value []byte) {
	var u32 uint32
	if fpos< 0 {
		db.logfile.Seek(int64(-fpos)+KeySize+1, os.SEEK_SET)
		binary.Read(db.logfile, binary.LittleEndian, &u32)
		value = make([]byte, u32)
		db.logfile.Read(value)
	} else {
		db.datfile.Seek(int64(fpos)+KeySize, os.SEEK_SET)
		binary.Read(db.datfile, binary.LittleEndian, &u32)
		value = make([]byte, u32)
		db.datfile.Read(value)
	}
	return
}
