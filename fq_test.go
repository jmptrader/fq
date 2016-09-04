package fq

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewQueue(t *testing.T) {
	w, r, cleanup, err := newReaderWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tests := []string{
		"hello world!",
		"the second line",
		"the last thing I'm going to write",
	}
	for _, test := range tests {
		_, err = w.Write([]byte(test))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i <= len(tests); i++ {
		b, err := r.Read()
		if err == io.EOF {
			if i == len(tests) {
				break
			} else {
				t.Fatalf("unexpected EOF")
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if tests[i] != string(b) {
			t.Errorf("expected:%s, got %s", tests[i], string(b))
		}
	}

	b, err := r.ReadAt(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != tests[1] {
		t.Errorf("ReadAt expected: %s, got %s", tests[1], string(b))
	}
}

func TestReadPast(t *testing.T) {
	w, r, cleanup, err := newReaderWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	_, err = w.Write([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = r.ReadAt(3)
	if err != io.EOF {
		t.Fatal("expected an 'io.EOF'")
	}
	if r.Offset() != 0 {
		t.Fatal("expected offset to not change")
	}
}

func TestMultipleReaders(t *testing.T) {
	w, r, cleanup, err := newReaderWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	text := "hello world"
	for i := 0; i < 100; i++ {
		_, err := w.Write([]byte(text))
		if err != nil {
			t.Fatalf("error writing to writer: %s", err)
		}
	}

	fname := r.log.(*os.File).Name()
	readers := []*Reader{r}
	for i := 0; i < 5; i++ {
		rl, err := os.Open(fname)
		if err != nil {
			t.Fatalf("error opening log file: %s", err)
		}
		defer rl.Close()
		ri, err := os.Open(fname + indexPostfix)
		if err != nil {
			t.Fatalf("error opening index file: %s", err)
		}
		defer ri.Close()
		r, err := NewReader(rl, ri)
		if err != nil {
			t.Fatalf("error opening reader: %s", err)
		}
		readers = append(readers, r)
	}

	readParallel := func(done chan error, r *Reader, n int) {
		defer close(done)
		i := 0
		for {
			resp, err := r.Read()
			if err != nil && err != io.EOF {
				done <- err
				return
			}
			if err == io.EOF {
				if i != 100 {
					done <- fmt.Errorf("reader %d failed to read all messages", n)
				}
				break
			}
			if string(resp) != text {
				done <- fmt.Errorf("unexptected text: %s at count %d", string(resp), i)
			}
			i++
		}
	}

	completed := make([]chan error, 0, len(readers))
	for ri, reader := range readers {
		done := make(chan error, 1)
		completed = append(completed, done)
		go readParallel(done, reader, ri)
	}

	for i := range readers {
		err := <-completed[i]
		if err != nil {
			t.Errorf("reader %d failed to complete: %s", i, err)
		}
	}
}

func newReaderWriter() (w io.Writer, r *Reader, cleanup func(), err error) {
	wf, err := ioutil.TempFile(os.TempDir(), "fq-")
	if err != nil {
		return
	}

	wlf, err := os.OpenFile(wf.Name()+indexPostfix, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		wf.Close()
		os.Remove(wf.Name())
		return
	}

	rf, err := os.Open(wf.Name())
	if err != nil {
		wf.Close()
		os.Remove(wf.Name())
		wlf.Close()
		os.Remove(wlf.Name())
		return
	}

	rlf, err := os.Open(wlf.Name())
	if err != nil {
		wf.Close()
		rf.Close()
		os.Remove(wf.Name())
		wlf.Close()
		os.Remove(wlf.Name())
		return
	}

	cleanup = func() {
		wf.Close()
		rf.Close()
		wlf.Close()
		rlf.Close()
		os.Remove(wf.Name())
		os.Remove(wlf.Name())
	}
	w, _ = NewWriter(wf, wlf)
	r, _ = NewReader(rf, rlf)
	return
}
