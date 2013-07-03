package qdb

import (
	"os"
	"io"
	"errors"
	"hash/crc32"
	"encoding/binary"
)


func (db *DB) checklogfile() {
	if db.logfile == nil {
		db.logfile, _ = os.Create(db.pathname+"log")
		binary.Write(db.logfile, binary.LittleEndian, uint32(db.version_seq))
	}
	return
}


func (db *DB) loadfilelog() {
	var u32 uint32
	var cmd [1]byte
	var key KeyType
	var lastvalidpos int64
	var n int
	var val []byte
	var e error

	if db.logfile != nil {
		panic("logfile already open")
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
			println(db.pathname, "- log entry CRC mismatch")
			e = errors.New("CRC mismatch")
			break
		}
		idx := db.index[key]
		if cmd[0]==1 {
			keep := !db.NeverKeepInMem && (db.KeepInMem==nil || db.KeepInMem(val))
			if idx!=nil {
				// this is a record's update
				idx.fpos = -lastvalidpos
				if keep {
					idx.data = val
				} else {
					idx.data = nil
				}
			} else {
				// the record needs to eb added
				if keep {
					db.index[key] = &oneIdx{data:val, fpos:-lastvalidpos}
				} else {
					db.index[key] = &oneIdx{fpos:-lastvalidpos}
				}
			}
		} else {
			if idx != nil {
				// we had such a record, so delete it from the map
				delete(db.index, key)
			}
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
func (db *DB) addtolog(key KeyType, val []byte) (fpos int64) {
	db.checklogfile()
	add := [1]byte{1}

	fpos, _ = db.logfile.Seek(0, os.SEEK_END)

	db.logfile.Write(add[:]) // add
	binary.Write(db.logfile, binary.LittleEndian, key)
	binary.Write(db.logfile, binary.LittleEndian, uint32(len(val)))
	db.logfile.Write(val[:])

	crc := crc32.NewIEEE()
	crc.Write(add[:])
	binary.Write(crc, binary.LittleEndian, key)
	binary.Write(crc, binary.LittleEndian, uint32(len(val)))
	crc.Write(val[:])

	binary.Write(db.logfile, binary.LittleEndian, uint32(crc.Sum32()))

	return
}


// append delete record at the end of the log
func (db *DB) deltolog(key KeyType) {
	db.checklogfile()
	var del [1]byte

	db.logfile.Seek(0, os.SEEK_END)
	db.logfile.Write(del[:]) // add
	binary.Write(db.logfile, binary.LittleEndian, key)

	crc := crc32.NewIEEE()
	crc.Write(del[:])
	binary.Write(crc, binary.LittleEndian, key)
	binary.Write(db.logfile, binary.LittleEndian, uint32(crc.Sum32()))

	return
}
