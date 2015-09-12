package flagdb

import (
	"fmt"
	"os"
)

type DB struct {
	file *os.File
}

func open(fpath string, rdonly bool) (*DB, error) {
	flags := os.O_CREATE
	if rdonly {
		flags |= os.O_RDONLY
	} else {
		flags |= os.O_RDWR
	}
	f, e := os.OpenFile(fpath, flags, 0777)
	if e != nil {
		return nil, e
	}
	return &DB{
		file: f,
	}, nil
}

func New(fpath string) (*DB, error) {
	return open(fpath, false)
}

func Read(fpath string) (*DB, error) {
	return open(fpath, true)
}

func (db *DB) Set(id int64, val byte) error {
	_, e := db.file.Seek(id, 0)
	if e != nil {
		return e
	}
	_, e = db.file.Write([]byte{val})
	return e
}

func (db *DB) Last() (int64, error) {
	info, e := db.file.Stat()
	if e != nil {
		return 0, e
	}
	return info.Size() - 1, e
}

func (db *DB) Release() error {
	return db.file.Close()
}

func (db *DB) Get(id int64) (byte, error) {
	_, e := db.file.Seek(id, 0)
	if e != nil {
		return 0, e
	}

	b2 := make([]byte, 1)
	_, e = db.file.Read(b2)
	if e != nil {
		return 0, e
	}
	if len(b2) != 1 {
		return 0, fmt.Errorf("bad")
	}
	return b2[0], nil
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}
