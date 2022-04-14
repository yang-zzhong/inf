package inf

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

type readableItem struct {
	read int
}

const (
	metaSize   = 14
	itemSize   = 9
	capability = 2048
	magicSize  = 7
	itemFrom   = metaSize + magicSize
	version    = 1
	blockSize  = 64

	empty   = uint8(0)
	filled  = uint8(1)
	deleted = uint8(2)
)

var (
	ErrBrokenFile        = errors.New("broken file")
	magicNumber   []byte = []byte{'p', 'e', 'p', 'e', '.', 'c', 'h'}
)

func FileChunk(pathfile string) *fileChunk {
	return &fileChunk{
		pathfile: pathfile,
		version:  version,
		total:    capability,
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
		if err = chunk.writeMeta(); err != nil {
			return
		}
		for i := 0; i < int(chunk.total); i++ {
			idx := &index{}
			if err = chunk.writeIndex(i, idx); err != nil {
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
	return chunk.readMeta()
}

func (chunk *fileChunk) Close() error {
	if err := chunk.file.Close(); err != nil {
		return err
	}
	chunk.file = nil
	return nil
}

func (chunk *fileChunk) writeMeta() error {
	bs := make([]byte, metaSize)
	binary.BigEndian.PutUint32(bs[:4], chunk.version)
	binary.BigEndian.PutUint16(bs[4:6], chunk.block)
	binary.BigEndian.PutUint32(bs[6:10], chunk.total)
	binary.BigEndian.PutUint32(bs[10:14], chunk.firstEmptyAt)
	_, err := chunk.file.WriteAt(magicSize, os.SEEK_SET, bs)
	return err
}

func (chunk *fileChunk) readMeta() error {
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

func (chunk *fileChunk) readIndex(idx int, index *index) (err error) {
	if uint32(idx) >= chunk.total {
		return errors.New("index out of range")
	}
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

func (chunk *fileChunk) writeIndex(idx int, index *index) (err error) {
	bs := make([]byte, itemSize)
	bs[0] = byte(index.state)
	binary.BigEndian.PutUint32(bs[1:5], index.size)
	binary.BigEndian.PutUint32(bs[5:9], index.headBlock)
	var pos uint32 = uint32(itemFrom + idx*itemSize)
	if err = chunk.ensurePosValid(pos); err != nil {
		return err
	}
	if _, err = chunk.file.WriteAt(int64(pos), os.SEEK_SET, bs); err != nil {
		return err
	}
	return nil
}

func (chunk *fileChunk) readBlock(pos uint32, blk *block) error {
	bs := make([]byte, chunk.block)
	if _, err := chunk.file.ReadAt(int64(pos), os.SEEK_SET, bs); err != nil {
		return err
	}
	blk.state = bs[0]
	blk.prevPos = binary.BigEndian.Uint32(bs[1:5])
	blk.nextPos = binary.BigEndian.Uint32(bs[5:9])
	copy(blk.content, bs[9:])
	return nil
}

func (chunk *fileChunk) writeBlock(pos uint32, blk *block) error {
	bs := make([]byte, chunk.block)
	bs[0] = blk.state
	binary.BigEndian.PutUint32(bs[1:5], blk.prevPos)
	binary.BigEndian.PutUint32(bs[5:9], blk.nextPos)
	if copied := copy(bs[9:], blk.content); copied != int(chunk.block-9) {
		return errors.New("copy error")
	}
	_, err := chunk.file.WriteAt(int64(pos), os.SEEK_SET, bs)
	return err
}

func (chunk *fileChunk) ensurePosValid(pos uint32) error {
	stat, err := chunk.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < int64(pos) {
		return errors.New("pos out of the range")
	}
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
	var index index
	if err = chunk.firstValidIndexFrom(idx, &index); err != nil {
		return
	}
	return
}

func (chunk *fileChunk) firstValidIndexFrom(idx int, index *index) error {
	index.state = empty
	for index.state != filled {
		if err := chunk.readIndex(idx, index); err != nil {
			return err
		}
		idx++
	}
	return nil
}

func (chunk *fileChunk) Full() bool {
	return chunk.firstEmptyAt == chunk.total-1
}

func (chunk *fileChunk) Add(r io.Reader) error {
	if chunk.Full() {
		return errors.New("chunk fulled")
	}
	pos, err := chunk.file.EndPos()
	if err != nil {
		return err
	}
	idx := &index{
		state:     filled,
		size:      item.Size(),
		headBlock: uint32(pos),
	}
	if err := chunk.writeIndex(int(chunk.firstEmptyAt), idx); err != nil {
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
			content: make([]byte, chunk.block-9),
			prevPos: prevPos,
			nextPos: 0,
			state:   filled,
		}
		rl, err := r.Read(blk.content)
		if err != nil {
			return err
		}
		if !(rl < int(chunk.block) && read+uint32(chunk.block) > item.Size()) {
			blk.nextPos = uint32(pos + int64(chunk.block))
		}
		chunk.writeBlock(uint32(pos), &blk)
		read += uint32(rl)
		prevPos = uint32(pos)
		pos = int64(blk.nextPos)
	}
	chunk.firstEmptyAt += 1
	if err := chunk.syncFirstEmptyAt(); err != nil {
		return err
	}
	return nil
}
