package rio

import (
	"bufio"
	"errors"
	"os"

	"github.com/sunminx/RDB/pkg/util"
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
	wr              *bufio.Writer
	name            string
	updateCksum     bool
	updateCksumFn   updateCksumFn
	processBytes    int
	maxProcessChunk int
}

// updateCksumFn Only calculate the checksum based on the incoming byte array and update the cksum,
// without modifying the original byte array.
type updateCksumFn func(*Writer, []byte, int)

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
	processBytes := w.processBytes
	ln := len(p)
	for ln > 0 {
		chunk := util.Cond(w.maxProcessChunk < ln, w.maxProcessChunk, ln)
		if w.updateCksum {
			w.updateCksumFn(w, p, chunk)
		}
		n, err := w.wr.Write(p)
		if err != nil {
			return n, err
		}
		p = p[chunk:]
		ln -= chunk
		w.processBytes += chunk
	}
	return (w.processBytes - processBytes), nil
}
