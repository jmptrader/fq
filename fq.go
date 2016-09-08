package fq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const indexPostfix = ".index"

type Writer struct {
	sync.Mutex
	index io.WriteSeeker
	log   io.WriteSeeker

	close func()
}

// NewWriter creates a new fq queue for writing.
//
// This creates two files, one with the name provided and another with .index postfixed.
func NewWriter(name string) (*Writer, error) {
	l, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}
	i, err := os.OpenFile(name+indexPostfix, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		log:   l,
		index: i,
		close: func() {
			l.Close()
			i.Close()
		},
	}
	return w, nil
}

func (w *Writer) Close() {
	if w.close != nil {
		w.close()
	}
}

// Write writes len(b) bytes from b to the queue.
func (w *Writer) Write(b []byte) (int, error) {
	w.Lock()
	defer w.Unlock()
	current, err := w.log.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// Store current position in index
	err = writeInt64(current, w.index)
	if err != nil {
		return -1, err
	}
	// Store length of bytes in log followed by content
	err = writeInt64(int64(len(b)), w.log)
	if err != nil {
		//TODO: erase index
		return -1, err
	}
	n, err := w.log.Write(b)
	if err != nil {
		//TODO: erase index and may be clean up?
		return -1, err
	}
	if n != len(b) {
		return -1, errors.New("could not write all content")
	}
	return n, nil
}

func writeInt64(n int64, w io.Writer) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	written, err := w.Write(b)
	if err != nil {
		return err
	}
	if written != len(b) {
		return fmt.Errorf("cannot write %d: %d written", len(b), n)
	}
	return nil
}

type Reader struct {
	log    io.ReadSeeker
	index  io.ReadSeeker
	offset int64

	l sync.Mutex
}

// NewFileReader opens fq queue for reading
func NewFileReader(name string) (*Reader, error) {
	l, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	i, err := os.Open(name + indexPostfix)
	if err != nil {
		return nil, err
	}

	return NewReader(l, i)
}

// NewReader opens fq queue for reading given log and index readers
func NewReader(log, index io.ReadSeeker) (*Reader, error) {
	return &Reader{
		log:   log,
		index: index,
	}, nil
}

// Offset returns the current offset of the queue
func (r *Reader) Offset() int64 {
	return r.offset
}

// Read reads the next message in the queue.
//
// Read returns an io.EOF when there are no more messages to read.
func (r *Reader) Read() ([]byte, error) {
	r.l.Lock()
	defer r.l.Unlock()
	return r.read()
}

// ReadAt reads the message at the given offset
//
// ReadAt returns an io.EOF when an incorrect offset is provided.
func (r *Reader) ReadAt(offset int64) ([]byte, error) {
	r.l.Lock()
	defer r.l.Unlock()

	original := r.offset
	r.offset = offset
	ret, err := r.read()
	if err != nil {
		r.offset = original
	}
	return ret, err
}

func (r *Reader) read() ([]byte, error) {
	offset, err := r.logOffset(r.offset)
	if err != nil {
		return nil, err
	}
	_, err = r.log.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	size, err := readInt64(r.log)
	if err != nil {
		return nil, err
	}
	b := make([]byte, size)
	n, err := io.ReadFull(r.log, b)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, errors.New("not enough data")
	}
	r.offset++
	return b, nil
}

func (r *Reader) logOffset(offset int64) (int64, error) {
	err := r.seekIndex(offset)
	if err != nil {
		return -1, err
	}
	return readInt64(r.index)
}

func readInt64(r io.Reader) (int64, error) {
	b := make([]byte, 8)
	n, err := io.ReadFull(r, b)
	if err != nil {
		return -1, err
	}
	if n != 8 {
		return -1, errors.New("not enough data")
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (r *Reader) seekIndex(offset int64) error {
	_, err := r.index.Seek(8*offset, io.SeekStart)
	return err
}
