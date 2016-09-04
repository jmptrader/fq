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

type writer struct {
	sync.Mutex
	index io.WriteSeeker
	log   io.WriteSeeker
}

func NewWriter(name string) (io.Writer, error) {
	l, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}
	i, err := os.OpenFile(name+indexPostfix, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}
	w := &writer{
		log:   l,
		index: i,
	}
	return w, nil
}

func (w *writer) Write(b []byte) (int, error) {
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
}

func NewReader(name string) (*Reader, error) {
	l, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	i, err := os.Open(name + indexPostfix)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		log:   l,
		index: i,
	}
	return r, nil
}

func (r *Reader) Read() ([]byte, error) {
	return r.read()
}

func (r *Reader) ReadAt(offset int64) ([]byte, error) {
	r.offset = offset
	return r.read()
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
