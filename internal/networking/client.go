package networking

import (
	"fmt"
	"strconv"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/sds"
)

// Client flags
type ClientFlag int

const (
	ClientSave ClientFlag = 1 << iota
	ClientMaster
	ClientMonitor
	ClientMulti
	ClientBlocked
	ClientDirtyCas
	ClientCloseAfterReply

	ClientNone = 0
)

type Client struct {
	gnet.Conn
	fd     int
	server *Server

	flags         ClientFlag
	authenticated bool

	querybuf     sds.SDS
	multibulklen int
	bulklen      int

	argc int
	argv []*dict.Robj

	reply []byte
}

func NewClient(conn gnet.Conn) *Client {
	return &Client{
		Conn: conn,
		fd:   conn.Fd(),
	}
}

var (
	ProtoInlineMaxSize = 1024 * 64
)

// processInlineBuffer
func (c *Client) processInlineBuffer() bool {
	var newline []byte

	newline, ok := c.querybuf.SplitNewLine()
	if !ok {
		return false
	}

	if newline == nil {
		if len(c.querybuf) > ProtoInlineMaxSize {
			c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
			c.setProtocolError()
		}
		return false
	}
	fmt.Println(string(newline))
	c.argv = splitArgs(newline, ' ')
	if len(c.argv) == 0 {
		c.AddReplyError([]byte("Protocol error: unbalanced quotes in request"))
		c.setProtocolError()
	}
	c.argc = len(c.argv)
	return true
}

func splitArgs(bytes []byte, sep byte) []*dict.Robj {
	res := make([]*dict.Robj, 0)
	i := 0
	for j := 0; j < len(bytes); j++ {
		if bytes[j] == sep {
			res = append(res, dict.NewRobj(sds.New(bytes[i:j])))
			i = j
		}
	}
	res = append(res, dict.NewRobj(sds.New(bytes[i:])))
	return res
}

const maxMulitbulksWhileUnauth = 10
const maxBulksWhileUnauth = 16384
const protoMaxBulkLen = 1024

// processMulitbulkBuffer
func (c *Client) processMulitbulkBuffer() bool {
	var newline []byte
	var ok bool

	if c.multibulklen == 0 {
		newline, ok = c.querybuf.SplitNewLine()
		if !ok {
			return false
		}
		if newline == nil {
			if len(c.querybuf) > ProtoInlineMaxSize {
				c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
				c.setProtocolError()
			}
			return false
		}

		if newline[0] != '*' {
			return false
		}

		ll, err := strconv.Atoi(string(newline[1:]))
		if err != nil || ll > 1024*1024 {
			c.AddReplyError([]byte("Protocol error: invalid multibulk length"))
			c.setProtocolError()
			return false
		}

		if ll > maxMulitbulksWhileUnauth && c.server.requirepass && !c.authenticated {
			c.AddReplyError([]byte("Protocol error: unauthenticated multibulk length"))
			c.setProtocolError()
			return false
		}

		if ll <= 0 {
			return true
		}

		c.multibulklen = ll
		c.argv = make([]*dict.Robj, ll)
	}

	for c.multibulklen > 0 {
		// new bulk
		if c.bulklen == -1 {
			newline, ok = c.querybuf.SplitNewLine()
			if !ok {
				return false
			}
			if newline == nil {
				if len(c.querybuf) > ProtoInlineMaxSize {
					c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
					c.setProtocolError()
					return false
				}
				break
			}

			// in request, only bulk string in multibulk allowed
			if newline[0] != '$' {
				c.AddReplyError([]byte(fmt.Sprintf(`Protocol error: expected '$', got '%c'`, newline[0])))
				c.setProtocolError()
				return false
			}
			ll, err := strconv.Atoi(string(newline[1:]))
			if err != nil || ll > ProtoInlineMaxSize {
				c.AddReplyError([]byte("Protocol error: invalid bulk length"))
				c.setProtocolError()
				return false
			}
			if ll > maxBulksWhileUnauth && c.server.requirepass && !c.authenticated {
				c.AddReplyError([]byte("Protocol error: unauthenticated bulk length"))
				c.setProtocolError()
			}

			c.bulklen = ll
		}

		// read bulk argument
		if c.querybuf.Len() < c.bulklen {
			// not enough data
			break
		}

		c.argv[c.argc] = dict.NewRobj(c.querybuf.DupLine())
		c.argc += 1
		c.bulklen = -1
		c.multibulklen -= 1
	}

	if c.multibulklen == 0 {
		return true
	}
	return false
}

func (c *Client) setProtocolError() {
	c.flags |= ClientCloseAfterReply
}

func (c *Client) AddReply(val dict.Robj) {

}

func (c *Client) AddReplyError(err []byte) {
	if len(err) == 0 || err[0] != '-' {
		c.reply = append(c.reply, []byte("-ERR ")...)
	}

	c.reply = append(c.reply, err...)
	c.reply = append(c.reply, []byte("\r\n")...)
}

func (c *Client) AddReplyStatus(status []byte) {
	c.reply = append(c.reply, '+')
	c.reply = append(c.reply, status...)
	c.reply = append(c.reply, []byte("\r\n")...)
}

func (c *Client) addReplyToBuffer(buf []byte) {
	c.reply = append(c.reply, buf...)
}
