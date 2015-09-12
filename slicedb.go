package flagdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type SliceDB struct {
	file      *os.File
	sortError error
}

func sliceDbopen(fpath string, rdonly bool) (*SliceDB, error) {
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
	return &SliceDB{
		file: f,
	}, nil
}

func NewSliceDb(fpath string) (*SliceDB, error) {
	return sliceDbopen(fpath, false)
}

func ReadSliceDb(fpath string) (*SliceDB, error) {
	return sliceDbopen(fpath, true)
}

func (db *SliceDB) Append(val uint32) error {
	_, e := db.file.Seek(0, 2)
	if e != nil {
		return e
	}
	_, e = db.file.Write(uint32ToByte(val))
	return e
}

func (db *SliceDB) Last() (uint32, error) {
	_, e := db.file.Seek(0, 2)
	if e != nil {
		return 0, e
	}
	_, e = db.file.Seek(-4, 1)
	if e != nil {
		return 0, e
	}
	buf := make([]byte, 4)
	_, e = db.file.Read(buf)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (db *SliceDB) get(i int) ([]byte, error) {
	_, e := db.file.Seek(int64(i)*4, 0)
	if e != nil {
		return nil, e
	}
	buf := make([]byte, 4)
	_, e = db.file.Read(buf)
	if e != nil {
		return nil, e
	}
	return buf, nil
}

func (db *SliceDB) set(i int, v []byte) error {
	_, e := db.file.Seek(int64(i)*4, 0)
	if e != nil {
		return e
	}
	_, e = db.file.Write(v)
	if e != nil {
		return e
	}
	return nil
}

func (db *SliceDB) Sort() error {
	sort.Sort(db)
	if db.sortError != nil {
		return db.sortError
	}
	return nil
}

func (db *SliceDB) Len() int {
	fi, e := db.file.Stat()
	if e != nil {
		db.sortError = e
		return 0
	}
	return int(fi.Size() / 4)
}

func (db *SliceDB) Swap(i, j int) {
	iBts, e := db.get(i)
	if e != nil {
		db.sortError = e
		return
	}
	jBts, e := db.get(j)
	if e != nil {
		db.sortError = e
		return
	}
	e = db.set(i, jBts)
	if e != nil {
		db.sortError = e
		return
	}
	e = db.set(j, iBts)
	if e != nil {
		db.sortError = e
		return
	}
	fmt.Println(i, "swap", j)

}
func (db *SliceDB) Less(i, j int) bool {
	iBts, e := db.get(i)
	if e != nil {
		db.sortError = e
		return false
	}
	jBts, e := db.get(j)
	if e != nil {
		db.sortError = e
		return false
	}
	ival := binary.BigEndian.Uint32(iBts)
	jval := binary.BigEndian.Uint32(jBts)
	return ival < jval
}

func (db *SliceDB) BatchAppend(arr []uint32) error {
	_, e := db.file.Seek(0, 2)
	if e != nil {
		return e
	}
	for _, v := range arr {
		_, e = db.file.Write(uint32ToByte(v))
		if e != nil {
			return e
		}
	}
	return e
}

func (db *SliceDB) Limit(limit int64, offsets ...int64) ([]uint32, error) {
	var offset int64
	if len(offsets) != 0 {
		offset = offsets[0]
	}

	_, e := db.file.Seek(int64(offset), 0)
	if e != nil {
		return nil, e
	}

	res := []uint32{}
	buf := make([]byte, 4)
	for len(res) < int(limit) {
		_, e = db.file.Read(buf)
		if e != nil {
			if e == io.EOF {
				break
			}
			return nil, e
		}
		res = append(res, binary.BigEndian.Uint32(buf))
	}
	return res, nil
}

func uint32ToByte(i uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, i)
	return data
}
