package list

// list is merely a declaration reflecting which interfaces are provided.
// do not attempt to reference it.
type list interface {
	Push([]byte)
	PushLeft([]byte)
	Pop() []byte
	PopLeft() []byte
	Index(int64) []byte
	Len() int64
	Range(int64, int64) [][]byte
	Trim(int64, int64)
	Rem([]byte, int64) int64
	RemLeft([]byte, int64) int64
}
