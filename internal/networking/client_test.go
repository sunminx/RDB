package networking

import (
	"testing"

	obj "github.com/sunminx/RDB/internal/object"
)

func NewMockClient(buf []byte) *Client {
	return &Client{
		Conn:         nil,
		fd:           0,
		srv:          newMockServer(),
		flag:         none,
		querybuf:     make([]byte, 0),
		multibulklen: 0,
		bulklen:      -1,
		argc:         0,
		argv:         nil,
		reply:        make([]byte, 0, 16*1024),
	}
}

func newMockServer() *Server {
	return &Server{
		Requirepass: true,
	}
}

func TestProcessInline(t *testing.T) {
	testcases := []struct {
		input []byte
		want  []*obj.Robj
	}{
		{input: []byte("$12\r\nset name jim\r\n"),
			want: []*obj.Robj{obj.New("set", obj.TypeString, obj.EncodingRaw),
				obj.New("name", obj.TypeString, obj.EncodingRaw),
				obj.New("jim", obj.TypeString, obj.EncodingRaw)}},
	}

	for _, tc := range testcases {
		client := NewMockClient(tc.input)
		client.processInlineBuffer()
		t.Log(client.argc)
	}
}

func TestProcessMultibulkBuffer(t *testing.T) {
	testcases := []struct {
		input []byte
		want  []*obj.Robj
	}{
		{input: []byte("*3\r\n$3\r\nset\r\n$4\r\nname\r\n$3\r\njim\r\n"),
			want: []*obj.Robj{obj.New("set", obj.TypeString, obj.EncodingRaw),
				obj.New("name", obj.TypeString, obj.EncodingRaw),
				obj.New("jim", obj.TypeString, obj.EncodingRaw)}},
	}

	for _, tc := range testcases {
		client := NewMockClient(tc.input)
		client.processMultibulkBuffer()
		for _, arg := range client.argv {
			t.Log(string(arg))
		}
	}
}
