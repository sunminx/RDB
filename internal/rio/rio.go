package rio

import (
	"bufio"
	"io"
	"os"

	. "github.com/sunminx/RDB/pkg/util"
)

type Reader struct {
	*os.File
	rd *bufio.Reader
}

func NewReader(file *os.File) (*Reader, error) {
	rd := bufio.NewReader(file)
	return &Reader{
		File: file,
		rd:   rd,
	}, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.rd.Read(p)
}

func (r *Reader) ReadLine() ([]byte, bool, error) {
	return r.rd.ReadLine()
}

func (r *Reader) Tell() int64 {
	pos, err := r.Seek(0, 1)
	if err != nil {
		return -1
	}
	return pos
}

func (r *Reader) EOF() bool {
	_, err := io.ReadAll(r.File)
	return err == io.EOF
}

func (r *Reader) Reset() error {
	_, err := r.Seek(0, 0)
	if err != nil {
		return err
	}
	r.rd.Reset(r.File)
	return nil
}

type Writer struct {
	wr              *bufio.Writer
	updateCksum     bool
	updateCksumFn   updateCksumFn
	processBytes    int
	maxProcessChunk int
}

// updateCksumFn Only calculate the checksum based on the incoming byte array and update the cksum,
// without modifying the original byte array.
type updateCksumFn func(*Writer, []byte, int)

func NewWriter(file *os.File) (*Writer, error) {
	wr := bufio.NewWriter(file)
	return &Writer{
		wr:              wr,
		processBytes:    0,
		maxProcessChunk: 1024,
	}, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	processBytes := w.processBytes
	ln := len(p)
	for ln > 0 {
		chunk := Cond(w.maxProcessChunk < ln, w.maxProcessChunk, ln)
		if w.updateCksum {
			w.updateCksumFn(w, p, chunk)
		}
		n, err := w.wr.Write(p[:chunk])
		if err != nil {
			return n, err
		}
		p = p[n:]
		ln -= n
		w.processBytes += n
	}
	return (w.processBytes - processBytes), nil
}
