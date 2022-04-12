package store

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	gomock "github.com/golang/mock/gomock"
)

func testChunk(t *testing.T, test func(chunk *fileChunk)) {
	chunk := FileChunk("./testdata/chunk")
	if err := chunk.Open(); err != nil {
		t.Fatalf("new file chunk error: %s", err.Error())
	}
	defer chunk.Close()
	test(chunk)
}

func cleanup(handle func()) {
	defer os.Remove("./testdata/chunk")
	handle()
}

func Test_FileChunk_syncFirstEmptyAt(t *testing.T) {
	cleanup(func() {
		testChunk(t, func(chunk *fileChunk) {
			chunk.firstEmptyAt = 32
			chunk.syncFirstEmptyAt()
			chunk.file.Sync()
		})
		testChunk(t, func(chunk *fileChunk) {
			fmt.Printf("%v\n", chunk)
			if chunk.firstEmptyAt != 32 {
				t.Fatalf("sync total err")
			}
		})
	})
}

func Test_FileChunk_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var bs = make([]byte, 512)
	rand.Read(bs)
	reader := bytes.NewBuffer(bs)
	cleanup(func() {
		testChunk(t, func(chunk *fileChunk) {
			item := NewMockItem(ctrl)
			item.EXPECT().Size().Return(uint32(512)).AnyTimes()
			item.EXPECT().Reader().Return(reader).AnyTimes()
			if err := chunk.Add(item); err != nil {
				t.Fatalf("chunk add error: %s", err.Error())
			}
		})
		testChunk(t, func(chunk *fileChunk) {
			info, _ := chunk.file.Stat()
			fmt.Printf("size: %d\n", info.Size())
		})
	})
}
