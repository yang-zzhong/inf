package inf

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func testBlockStore(t *testing.T, test func(s *blockStore)) {
	s := New(FileRWSC("./block.fsf"))
	if err := s.Create(V010000, 512); err != nil {
		t.Fatalf("new file chunk error: %s", err.Error())
	}
	defer s.Close()
	test(s)
}

func cleanup(handle func()) {
	defer os.Remove("./block.fsf")
	handle()
}

func Test_version_String(t *testing.T) {
	fmt.Printf("version: %s\n", V010000.String())
}

func TestPaged(t *testing.T) {
	cleanup(func() {
		testBlockStore(t, func(s *blockStore) {
			fmt.Printf("%v\n", s)
			pages, _ := s.Acquire(5050)
			fmt.Printf("%v\n", pages)
			for i := range pages {
				pages[i].Data = []byte("hello world")
			}
			s.Put(pages)
			for i := range pages {
				s.Get(pages[i].idx, &pages[i])
				fmt.Printf("%s - %d\n", pages[i].Data, pages[i].Next)
			}
			for i := 1; i <= 2; i++ {
				if err := s.Erase(i); err != nil {
					panic(err)
				}
				fmt.Printf("%v\n", s)
			}
			pages, _ = s.Acquire(512)
			fmt.Printf("%v\n", pages)
		})
	})
}

func Test_blockStore_Acquire(t *testing.T) {
	cleanup(func() {
		testBlockStore(t, func(s *blockStore) {
			pages, _ := s.Acquire(300)
			if len(pages) != 1 {
				t.Fatalf("acquire error when 1 block")
			}
			if pages[0].idx != 1 {
				t.Fatalf("acquire index error when 1 block")
			}
			pages, _ = s.Acquire(505)
			if len(pages) != 1 {
				t.Fatalf("acquire error when 1 block with boundary reached")
			}
			pages, _ = s.Acquire(520)
			if len(pages) != 2 {
				t.Fatalf("acquire error when 2 block")
			}
			if pages[0].Next != pages[1].Index() {
				t.Fatalf("acquire's block next error when 2 block acquired")
			}
		})
	})
}

func Test_blockStore_Get(t *testing.T) {
	cleanup(func() {
		testBlockStore(t, func(s *blockStore) {
			var block Block
			if err := s.Get(0, &block); err != nil {
				t.Fatalf("get super block error")
			}
			if block.Type != TypeSuper {
				t.Fatalf("super block's type error")
			}
		})
	})
}

func Test_blockStore(t *testing.T) {
	cleanup(func() {
		testBlockStore(t, func(s *blockStore) {
			var testData = []byte("An SSTable provides a persistent, ordered immutable map from keys to values, where both keys and values are arbitrary byte strings. Operations are provided to look up the value associated with a specified key, and to iterate over all key/value pairs in a specified key range. Internally, each SSTable contains a sequence of blocks (typically each block is 64KB in size, but this is configurable). A block index (stored at the end of the SSTable) is used to locate blocks; the index is loaded into memory when the SSTable is opened. A lookup can be performed with a single disk seek: we first find the appropriate block by performing a binary search in the in-memory index, and then reading the appropriate block from disk. Optionally, an SSTable can be completely mapped into memory, which allows us to perform lookups and scans without touching disk.")
			blocks, err := s.Acquire(len(testData))
			if err != nil {
				t.Fatalf("acquire blocks error: %s", err.Error())
			}
			for i := range blocks {
				start := i * int(blocks[i].Size())
				var end int
				if i == len(blocks)-1 {
					end = len(testData)
				} else {
					end = (i + 1) * int(blocks[i].Size())
				}
				blocks[i].Data = testData[start:end]
			}
			if err := s.Put(blocks); err != nil {
				t.Fatalf("put data error: %s", err.Error())
			}
			blocks, err = s.From(1)
			if err != nil {
				t.Fatalf("get block 1 error: %s", err.Error())
			}
			var buf bytes.Buffer
			if _, err := s.WriteTo(&buf, 1); err != nil {
				t.Fatalf("write to error: %s", err.Error())
			}
			if !bytes.Equal(testData, buf.Bytes()) {
				t.Fatalf("read error")
			}
		})
	})
}
