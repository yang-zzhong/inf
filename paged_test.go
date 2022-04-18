package inf

import (
	"fmt"
	"os"
	"testing"
)

func testPaged(t *testing.T, test func(s *pagedStore)) {
	s := New(FileRWSC("./testdata/chunk"))
	if err := s.Create(V010000, 512); err != nil {
		t.Fatalf("new file chunk error: %s", err.Error())
	}
	defer s.Close()
	test(s)
}

func cleanup(handle func()) {
	defer os.Remove("./testdata/chunk")
	handle()
}

func Test_version_String(t *testing.T) {
	fmt.Printf("version: %s\n", V010000.String())
}

func TestPaged(t *testing.T) {
	cleanup(func() {
		testPaged(t, func(s *pagedStore) {
			fmt.Printf("%v\n", s)
			pages := s.Acquire(5)
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
				if err := s.Free(i); err != nil {
					panic(err)
				}
				fmt.Printf("%v\n", s)
			}
			pages = s.Acquire(5)
			fmt.Printf("%v\n", pages)
		})
	})
}
