package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Store interface {
	Add(io.Reader) error
	Del(idx int) error
	Put(idx int, r io.Reader) error
	Get(idx int) (io.Reader, error)
}

type version uint32
type state uint8

const (
	V1_1       uint32 = 0x00000001
	magicSize         = 16
	metaSize          = 32
	indexSize         = 19
	indexBegin        = magicSize + metaSize

	stateEmpty   state = 0
	stateCreated state = 1
	stateDeleted state = 2
)

var (
	magic         = []byte{'f', '.', 's', 'f', 's'}
	ErrEmptyStore = errors.New("empty store")
)

type index struct {
	state          state
	prev           uint32
	next           uint32
	firstSegmentAt int64
	segments       uint16
}

type segment struct {
	index uint32
	prev  int64
	next  int64
}

type store struct {
	pathfile    string
	file        *file
	v           version
	indexTotal  uint32
	indexCount  uint32
	segmentSize uint16
	indexes     []index
}

func New(pathfile string) *store {
	return &store{
		pathfile: pathfile,
	}
}

func (store *store) Open() error {
	file, err := os.Open(store.pathfile)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}
	store.file = NewFile(file)
	if err := store.verifyMagicNumber(); err != nil {
		return fmt.Errorf("can't verify magic number: %w", err)
	}
	if err := store.initIndexes(); err != nil {
		return fmt.Errorf("can't init indexes: %w", err)
	}
	return nil
}

func (store *store) verifyMagicNumber() error {
	mb := make([]byte, magicSize)
	if _, err := store.file.ReadAt(0, os.SEEK_SET, mb); err != nil {
		return err
	}
	if !bytes.Equal(mb, magic) {
		return errors.New("not a .sfs file")
	}
	return nil
}

func (store *store) initIndexes() error {
	var index index
	if err := store.getRandomNonEmptyIndex(&index); err != nil {
		if errors.Is(err, ErrEmptyStore) {
			return nil
		}
		return err
	}
	store.indexes = append(store.indexes, index)
	return store.initIndexFrom(0)
}

func (store *store) initIndexFrom(idx int) error {
	ind := store.indexes[idx]
	if ind.prev != 0 {
		var prev index
		store.readIndexAt(ind.prev, &prev)
		store.indexes = append(store.indexes[:idx], prev, store.indexes[idx:]...)
	}
}

func (store *store) getRandomNonEmptyIndex(index *index) error {
	for i := 0; i < int(store.indexTotal); i++ {
		if err := store.readIndex(i, index); err != nil {
			return err
		}
		if index.state == stateCreated {
			return nil
		}
	}
	return ErrEmptyStore
}

func (store *store) readIndexAt(pos uint32, index *index) (err error) {
	if pos >= indexBegin+indexSize*store.indexTotal {
		return errors.New("index out of range")
	}
	bs := make([]byte, indexSize)
	if _, err = store.file.ReadAt(int64(pos), os.SEEK_SET, bs); err != nil {
		return
	}
	index.state = state(bs[0])
	index.prev = binary.BigEndian.Uint32(bs[1:5])
	index.next = binary.BigEndian.Uint32(bs[5:9])
	index.firstSegmentAt = int64(binary.BigEndian.Uint64(bs[9:17]))
	index.segments = binary.BigEndian.Uint16(bs[17:19])
	return
}

func (store *store) readIndex(idx int, index *index) (err error) {
	return store.readIndex(indexBegin+idx*indexSize, index)
}
