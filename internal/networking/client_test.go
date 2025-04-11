package networking

import (
	"testing"

	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/sds"
)

func NewMockClient(buf []byte) *Client {
	return &Client{
		Conn:         nil,
		fd:           0,
		server:       NewMockServer(),
		flags:        ClientNone,
		querybuf:     *sds.New(buf),
		multibulklen: 0,
		bulklen:      0,
		argc:         0,
		argv:         nil,
		reply:        make([]byte, 0, 16*1024),
	}
}

func NewMockServer() *Server {
	return &Server{
		requirepass: true,
	}
}

func TestProcessInline(t *testing.T) {
	testcases := []struct {
		input []byte
		want  []*dict.Robj
	}{
		{input: []byte("$12\r\nset name jim\r\n"),
			want: []*dict.Robj{dict.NewRobj([]byte("set")),
				dict.NewRobj([]byte("name")),
				dict.NewRobj([]byte("jim"))}},
	}

	for _, tc := range testcases {
		client := NewMockClient(tc.input)
		client.processInlineBuffer()
		t.Log(client.argc)
	}
}
