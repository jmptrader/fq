package fq

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewWriter(t *testing.T) {
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

	for _, test := range tests {
		b, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if test != string(b) {
			t.Errorf("expected:%s, got %s", test, string(b))
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

func newReaderWriter() (w io.Writer, r *Reader, cleanup func(), err error) {
	wf, err := ioutil.TempFile(os.TempDir(), "fq-")
	if err != nil {
		return
	}

	wlf, err := ioutil.TempFile(os.TempDir(), "fq-")
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
