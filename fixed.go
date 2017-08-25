package flagdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

var (
	DefaultDataSize int64 = 24

	ErrNotFound = fmt.Errorf("not found")
	ErrBreak    = fmt.Errorf("break")
)

type FixDB struct {
	file     *os.File
	dataSize int64
}

func OpenFixDB(path string) (*FixDB, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	fdb := &FixDB{
		file:     file,
		dataSize: DefaultDataSize,
	}

	return fdb, nil
}

// id + data
func (f FixDB) recordSize() int64 {
	return f.dataSize + 8
}

func (f *FixDB) Size() (int64, error) {
	stat, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size() / (f.recordSize()), nil
}

// TODO
func (f *FixDB) Insert(id int64, data []byte) error {
	return nil
}

func (f *FixDB) getID(index int64) (int64, error) {
	// TODO change to readat
	_, err := f.file.Seek(index*f.recordSize(), io.SeekStart)
	if err != nil {
		return 0, err
	}
	var id int64
	err = binary.Read(f.file, binary.BigEndian, &id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (f *FixDB) Search(id int64) (index int64, err error) {
	size, err := f.Size()
	if err != nil {
		return 0, err
	}
	var tmpID int64
	index = int64(sort.Search(int(size), func(i int) bool {
		tmpID, err = f.getID(int64(i))
		return tmpID >= id
	}))
	if err != nil {
		return 0, err
	}
	tmpID, err = f.getID(int64(index))
	if err != nil {
		return 0, err
	}
	if tmpID == id {
		return
	}
	return 0, ErrNotFound
}

func (f *FixDB) updateIndex(index int64, data []byte) (err error) {
	_, err = f.file.WriteAt(data, index*f.recordSize()+8)
	return
}

func (f *FixDB) Update(id int64, data []byte) error {
	index, err := f.Search(id)
	if err != nil {
		return err
	}

	return f.updateIndex(index, data)
}

func (f *FixDB) Iterate(fn func(id int64, data []byte) error) error {
	_, err := f.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	br := bufio.NewReaderSize(f.file, 1*1024*1024) // 1MB
	var (
		id   int64
		data = make([]byte, f.dataSize)
	)
	for {
		err = binary.Read(br, binary.BigEndian, &id)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		_, err := br.Read(data)
		if err != nil {
			return err
		}
		err = fn(id, data)
		if err != nil {
			if err == ErrBreak {
				return nil
			}
			return err
		}
	}
}

func (f *FixDB) Get(id int64) ([]byte, error) {
	data := make([]byte, f.dataSize)
	index, err := f.Search(id)
	if err != nil {
		return nil, err
	}
	_, err = f.file.ReadAt(data, index*f.recordSize()+8)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *FixDB) Len() int {
	size, err := f.Size()
	if err != nil {
		panic(err)
	}
	return int(size)
}

// TODO err
func (f *FixDB) Less(i, j int) bool {
	id1, _ := f.getID(int64(i))
	id2, _ := f.getID(int64(j))
	return id1 < id2
}

func (f *FixDB) Swap(i, j int) {
	dataI, _ := f.Get(int64(i))
	dataJ, _ := f.Get(int64(j))
	f.updateIndex(int64(i), dataJ)
	f.updateIndex(int64(j), dataI)
}

func (f *FixDB) sort() error {
	sort.Sort(f)
	return nil
}
