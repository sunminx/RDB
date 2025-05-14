package rio

import (
	"bufio"
	"os"

	"github.com/sunminx/RDB/pkg/util"
)

type Reader struct {
	rd *bufio.Reader
}

func NewReader(file *os.File) (*Reader, error) {
	rd := bufio.NewReader(file)
	return &Reader{
		rd: rd,
	}, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.rd.Read(p)
}

func (r *Reader) ReadLine() ([]byte, bool, error) {
	return r.rd.ReadLine()
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
		chunk := util.Cond(w.maxProcessChunk < ln, w.maxProcessChunk, ln)
		if w.updateCksum {
			w.updateCksumFn(w, p, chunk)
		}
		n, err := w.wr.Write(p)
		w.wr.Flush()
		if err != nil {
			return n, err
		}
		p = p[chunk:]
		ln -= chunk
		w.processBytes += chunk
	}
	return (w.processBytes - processBytes), nil
}
