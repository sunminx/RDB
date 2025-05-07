package rio

import (
	"bufio"
	"errors"
	"os"
)

var (
	ErrCannotOpenFile = errors.New("target file can not be opened")
)

type Reader struct {
	rd   *bufio.Reader
	name string
}

func NewReader(name string) (*Reader, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, ErrCannotOpenFile
	}
	rd := bufio.NewReader(file)
	return &Reader{
		rd:   rd,
		name: name,
	}, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.rd.Read(p)
}

type Writer struct {
	wr   *bufio.Writer
	name string
}

func NewWriter(name string) (*Writer, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, ErrCannotOpenFile
	}
	wr := bufio.NewWriter(file)
	return &Writer{
		wr:   wr,
		name: name,
	}, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.wr.Write(p)
}
