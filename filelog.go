package qdb

import (
	"os"
	"io"
	"errors"
	"hash/crc32"
	"encoding/binary"
)


func (db *DB) checklogfile() (e error) {
	if db.logfile == nil {
		db.logfile, e = os.Create(db.pathname+"log")
		if e == nil {
			binary.Write(db.logfile, binary.LittleEndian, uint32(db.version_seq))
		}
	}
	return
}


func (db *DB) loadfilelog() (e error) {
	var u32 uint32
	var cmd [1]byte
	var key KeyType
	var lastvalidpos int64
	var n int
	var val []byte

	if db.logfile != nil {
		e = errors.New("loading logfile not possible when the file is already open")
		return
	}

	db.logfile, e = os.OpenFile(db.pathname+"log", os.O_RDWR, 0660)
	if e != nil {
		return
	}

	e = binary.Read(db.logfile, binary.LittleEndian, &u32)
	if e != nil {
		goto close_and_clean
	}
	if u32 != db.version_seq {
		e = errors.New("logfile sequence mismatch")
		goto close_and_clean
	}

	// Load records
	for {
		lastvalidpos, _ = db.logfile.Seek(0, os.SEEK_CUR)
		n, e = db.logfile.Read(cmd[:])
		if n!=1 || e!=nil {
			if e==io.EOF {
				e = nil
			}
			break
		}
		e = binary.Read(db.logfile, binary.LittleEndian, &key)
		if e!=nil {
			break
		}
		crc := crc32.NewIEEE()
		crc.Write(cmd[:])
		binary.Write(crc, binary.LittleEndian, key)
		if cmd[0]==1 {
			e = binary.Read(db.logfile, binary.LittleEndian, &u32)
			if e != nil {
				break
			}
			val = make([]byte, u32)
			n, e = db.logfile.Read(val[:])
			if n != len(val) || e != nil {
				break
			}
			binary.Write(crc, binary.LittleEndian, u32)
			crc.Write(val[:])
		} else if cmd[0]!=0 {
			e = errors.New("Unexpected command in logfile")
			break
		}
		e = binary.Read(db.logfile, binary.LittleEndian, &u32)
		if e != nil {
			break
		}
		if u32 != crc.Sum32() {
			e = errors.New("CRC mismatch")
			break
		}
		if cmd[0]==1 {
			db.cache[key] = val
		} else {
			delete(db.cache, key)
		}
	}
	if e!=nil {
		println(db.pathname+"log", "-", e.Error())
	}
	db.logfile.Seek(lastvalidpos, os.SEEK_SET)
	return

close_and_clean:
	if e!=nil {
		println(db.pathname+"log", ":", e.Error())
	}
	db.logfile.Close()
	db.logfile = nil
	os.Remove(db.pathname+"log")
	return
}


// add record at the end of the log
func (db *DB) addtolog(key KeyType, val []byte) (e error) {
	e = db.checklogfile()
	if e != nil {
		return
	}
	add := [1]byte{1}

	_, e = db.logfile.Write(add[:]) // add
	if e != nil {
		return
	}

	e = binary.Write(db.logfile, binary.LittleEndian, key)
	if e != nil {
		return
	}

	e = binary.Write(db.logfile, binary.LittleEndian, uint32(len(val)))
	if e != nil {
		return
	}

	_, e = db.logfile.Write(val[:])
	if e != nil {
		return
	}

	crc := crc32.NewIEEE()
	crc.Write(add[:])
	binary.Write(crc, binary.LittleEndian, key)
	binary.Write(crc, binary.LittleEndian, uint32(len(val)))
	crc.Write(val[:])

	e = binary.Write(db.logfile, binary.LittleEndian, uint32(crc.Sum32()))
	if e != nil {
		return
	}

	return
}


// append delete record at the end of the log
func (db *DB) deltolog(key KeyType) (e error) {
	e = db.checklogfile()
	if e != nil {
		return
	}
	var del [1]byte

	_, e = db.logfile.Write(del[:]) // add
	if e != nil {
		return
	}

	e = binary.Write(db.logfile, binary.LittleEndian, key)
	if e != nil {
		return
	}

	crc := crc32.NewIEEE()
	crc.Write(del[:])
	e = binary.Write(crc, binary.LittleEndian, key)

	e = binary.Write(db.logfile, binary.LittleEndian, uint32(crc.Sum32()))
	if e != nil {
		return
	}

	return
}
