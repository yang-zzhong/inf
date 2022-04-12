package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// how content organized
// meta = version:uint32 + total:uint32 + block_size
// index = size:uint32 + offset:uint32
// meta + [512]index + [512]item

// item = content:[1024]byte + next:uint32

// +-------------------------------------------------------------------
// |+----------------+|+-----+ +-------+     | +----------+
// ||01 512 1024 1024|||21 0 | | 4392 1| ... | |010010 342|
// |+----------------+|+-----+ +-------+     | +----------+
// +-------------------------------------------------------------------

type Item interface {
	Size() uint32
	Head() uint32
	Reader() io.Reader
	Writer() io.Writer
}

type Chunk interface {
	Total() int
	At(idx int) (Item, error)
	Set(idx int, item Item) error
	Add(item Item) error
	Del(idx int) error
}

type fileChunk struct {
	pathfile string
	file     *File

	version      uint32
	block        uint16
	total        uint32
	firstEmptyAt uint32

	metaCollected bool
}

type index struct {
	state     uint8
	size      uint32
	headBlock uint32
}

type block struct {
	state   uint8
	nextPos uint32
	prevPos uint32
	next    *block
	content []byte
}

const (
	metaSize   = 14
	itemSize   = 9
	capability = 2048
	magicSize  = 7
	itemFrom   = metaSize + 2
	version    = 1
	blockSize  = 64

	empty  = uint8(0)
	filled = uint8(1)
)

var (
	ErrBrokenFile        = errors.New("broken file")
	magicNumber   []byte = []byte{'p', 'e', 'p', 'e', '.', 'c', 'h'}
)

func FileChunk(pathfile string) *fileChunk {
	return &fileChunk{
		pathfile: pathfile,
		version:  version,
		block:    blockSize}
}

func (chunk *fileChunk) Open() (err error) {
	if chunk.file != nil {
		return
	}
	var file *os.File
	file, err = os.OpenFile(chunk.pathfile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	chunk.file = NewFile(file)
	defer func() {
		if err != nil {
			chunk.file.Close()
		}
	}()
	var info os.FileInfo
	info, err = chunk.file.Stat()
	if info.Size() == 0 {
		if _, err = chunk.file.Write(magicNumber); err != nil {
			return
		}
		if err = chunk.makeMeta(); err != nil {
			return
		}
		for i := 0; i < int(chunk.total); i++ {
			if err = chunk.makeIndex(i, 0, empty, 0); err != nil {
				return
			}
		}
		return
	} else if info.Size() < 7 {
		err = errors.New("bad file format")
		return
	}
	mb := make([]byte, magicSize)
	if _, err = chunk.file.Read(mb); err != nil {
		return
	}
	if !bytes.Equal(mb, magicNumber) {
		err = errors.New("bad file format")
	}
	return chunk.collectMeta()
}

func (chunk *fileChunk) Close() error {
	if err := chunk.file.Close(); err != nil {
		return err
	}
	chunk.file = nil
	return nil
}

func (chunk *fileChunk) makeMeta() error {
	bs := make([]byte, metaSize)
	binary.BigEndian.PutUint32(bs[:4], chunk.version)
	binary.BigEndian.PutUint16(bs[4:6], chunk.block)
	binary.BigEndian.PutUint32(bs[6:10], chunk.total)
	binary.BigEndian.PutUint32(bs[10:14], chunk.firstEmptyAt)
	_, err := chunk.file.WriteAt(magicSize, os.SEEK_SET, bs)
	return err
}

func (chunk *fileChunk) makeIndex(idx int, size uint32, state uint8, headBlock uint32) (err error) {
	index := index{
		size:      size,
		state:     state,
		headBlock: headBlock,
	}
	bs := make([]byte, itemSize)
	bs[0] = byte(state)
	binary.BigEndian.PutUint32(bs[1:5], index.size)
	binary.BigEndian.PutUint32(bs[5:9], index.headBlock)
	pos := itemFrom + idx*itemSize
	if _, err = chunk.file.WriteAt(int64(pos), os.SEEK_SET, bs); err != nil {
		return err
	}
	return nil
}

func (chunk *fileChunk) readIndex(idx int, index *index) (err error) {
	pos := itemFrom + idx*itemSize
	bs := make([]byte, itemSize)
	if _, err = chunk.file.ReadAt(int64(pos), os.SEEK_SET, bs); err != nil {
		return
	}
	index.state = uint8(bs[0])
	index.size = binary.BigEndian.Uint32(bs[1:5])
	index.headBlock = binary.BigEndian.Uint32(bs[5:9])
	return
}

func (chunk *fileChunk) collectMeta() error {
	if chunk.metaCollected {
		return nil
	}
	bs := make([]byte, metaSize)
	read, err := chunk.file.ReadAt(magicSize, os.SEEK_SET, bs)
	if err != nil {
		return err
	}
	if read != metaSize {
		return ErrBrokenFile
	}
	chunk.version = binary.BigEndian.Uint32(bs[:4])
	chunk.block = binary.BigEndian.Uint16(bs[4:6])
	chunk.total = binary.BigEndian.Uint32(bs[6:10])
	chunk.firstEmptyAt = binary.BigEndian.Uint32(bs[10:14])
	chunk.metaCollected = true
	return nil
}

func (chunk *fileChunk) syncFirstEmptyAt() error {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, chunk.firstEmptyAt)
	_, err := chunk.file.WriteAt(magicSize+10, os.SEEK_SET, bs)
	return err
}

func (chunk *fileChunk) Total() uint32 {
	return chunk.total
}

func (chunk *fileChunk) At(idx int) (item Item, err error) {
	return
}

func (chunk *fileChunk) Full() bool {
	return chunk.firstEmptyAt == chunk.total-1
}

func (chunk *fileChunk) Add(item Item) error {
	if chunk.Full() {
		return errors.New("chunk fulled")
	}
	pos, err := chunk.file.EndPos()
	if err != nil {
		return err
	}
	if err := chunk.makeIndex(int(chunk.firstEmptyAt), item.Size(), filled, uint32(pos)); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			chunk.file.Truncate(int64(pos))
		}
	}()
	var read uint32
	var prevPos uint32 = 0
	for read < item.Size() {
		blk := block{
			content: make([]byte, chunk.block),
			prevPos: prevPos,
			nextPos: 0,
			state:   filled,
		}
		bs := make([]byte, chunk.block)
		bs[0] = blk.state
		binary.BigEndian.PutUint32(bs[1:5], blk.prevPos)
		rl, err := item.Reader().Read(bs[9:])
		if err != nil {
			return err
		}
		if !(rl < int(chunk.block) && read+uint32(chunk.block) > item.Size()) {
			blk.nextPos = uint32(pos + int64(chunk.block))
		}
		binary.BigEndian.PutUint32(bs[5:9], blk.nextPos)
		read += uint32(rl)
		_, err = chunk.file.WriteAt(pos, os.SEEK_SET, bs)
		if err != nil {
			return err
		}
		prevPos = uint32(pos)
		pos = int64(blk.nextPos)
	}
	chunk.firstEmptyAt += 1
	if err := chunk.syncFirstEmptyAt(); err != nil {
		return err
	}
	return nil
}
