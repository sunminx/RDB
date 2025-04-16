package networking

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/db"
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
	ClientCloseASAP

	ClientNone = 0
)

type reqType int

const (
	protoReqNone reqType = iota
	protoReqMultibulk
	protoReqInline
)

type Client struct {
	gnet.Conn
	*db.DB
	fd  int
	Srv *Server

	flags         ClientFlag
	authenticated bool

	querybuf     sds.SDS
	reqtype      reqType
	multibulklen int
	bulklen      int

	argc int
	argv []dict.Robj

	reply []byte

	Cmd cmd.Command
}

func NewClient(conn gnet.Conn, db *db.DB) *Client {
	return &Client{
		Conn:          conn,
		DB:            db,
		fd:            conn.Fd(),
		querybuf:      sds.NewEmpty(),
		multibulklen:  0,
		bulklen:       -1,
		reqtype:       protoReqNone,
		argc:          0,
		argv:          make([]dict.Robj, 0),
		reply:         make([]byte, 0),
		authenticated: true,
	}
}

func nilClient() *Client {
	cli := new(Client)
	cli.fd = -1
	return cli
}

func (c *Client) setServer(srv *Server) {
	c.Srv = srv
}

func (c *Client) Key() string {
	return c.argvByIdx(1)
}

func (c *Client) argvByIdx(n int) string {
	if c.argc < n-1 {
		return ""
	}

	argvn := c.argv[n].Val().(sds.SDS)
	return string(argvn.Bytes())
}

func (c *Client) Argv() []dict.Robj {
	return c.argv
}

func (c *Client) LookupKey(key string) (dict.Robj, bool) {
	obj, err := c.LookupKeyRead(key)
	if err != nil {
		return obj, false
	}
	return obj, true
}

var (
	ProtoInlineMaxSize = 1024 * 64
)

func (c *Client) processInputBuffer() {
	for c.querybuf.Len() > 0 {
		if c.reqtype == protoReqNone {
			if c.querybuf.FirstByte() == '*' {
				c.reqtype = protoReqMultibulk
			} else {
				c.reqtype = protoReqInline
			}
		}

		if c.reqtype == protoReqInline {
			if !c.processInlineBuffer() {
				break
			}
		} else if c.reqtype == protoReqMultibulk {
			if !c.processMultibulkBuffer() {
				break
			}
		} else {
			panic("unknown request type")
		}

		if c.argc == 0 {
			c.argv = make([]dict.Robj, 0)
			c.multibulklen = 0
			c.bulklen = -1
			c.reqtype = protoReqNone
		} else {
			c.processCommand()
		}
	}
}

// processInlineBuffer
func (c *Client) processInlineBuffer() bool {
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
	c.argv = splitArgs(newline, ' ')
	if len(c.argv) == 0 {
		c.AddReplyError([]byte("Protocol error: unbalanced quotes in request"))
		c.setProtocolError()
	}
	c.argc = len(c.argv)
	return true
}

func splitArgs(bytes []byte, sep byte) []dict.Robj {
	res := make([]dict.Robj, 0)
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

var protoMaxBulkLen = 1024 * 1024 * 512

// processMulitbulkBuffer
func (c *Client) processMultibulkBuffer() bool {
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

		if ll > maxMulitbulksWhileUnauth && c.Srv.Requirepass && !c.authenticated {
			c.AddReplyError([]byte("Protocol error: unauthenticated multibulk length"))
			c.setProtocolError()
			return false
		}

		if ll <= 0 {
			return true
		}

		c.multibulklen = ll
		c.argv = make([]dict.Robj, ll)
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
			if err != nil || ll > protoMaxBulkLen {
				c.AddReplyError([]byte("Protocol error: invalid bulk length"))
				c.setProtocolError()
				return false
			}
			if ll > maxBulksWhileUnauth && c.Srv.Requirepass && !c.authenticated {
				c.AddReplyError([]byte("Protocol error: unauthenticated bulk length"))
				c.setProtocolError()
			}

			c.bulklen = ll
		}

		// Read bulk argument
		if c.querybuf.Len() < c.bulklen+2 {
			// Not enough data (+2 == trailing \r\n)
			break
		}

		s := c.querybuf.DupLine()
		if s.Len() != c.bulklen {
			c.AddReplyError([]byte("Protocol error: incorrect bulk length"))
			c.setProtocolError()
			return false
		}

		c.argv[c.argc] = dict.NewRobj(s)
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

func (c *Client) processCommand() {
	name := c.argvByIdx(0)
	name = strings.ToLower(name)
	if name == "quit" {
		c.flags |= ClientCloseASAP
		return
	}
	cmd, ok := LookupCommand(name)
	if !ok {
		c.AddReplyError([]byte(fmt.Sprintf(`unknown command %q`, "xxx")))
		goto clean
	}
	c.Cmd = cmd
	c.call()
clean:
	c.argc = 0
	return
}

func (c *Client) call() {
	_ = c.Cmd.Proc(c)
}

func (c *Client) AddReply(obj dict.Robj) {
	switch obj.Type() {
	case dict.ObjString:
		c.AddReplySds(obj.Val().(sds.SDS))
	default:
	}
}

func (c *Client) AddReplySds(s sds.SDS) {
	c.reply = append(c.reply, s.Bytes()...)
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

func (c *Client) AddReplyInt64(n int64) {
	if n == 0 {
		c.addReplyBytes(common.Shared["czero"])
	} else if n == 1 {
		c.addReplyBytes(common.Shared["cone"])
	} else {
		c.addReplyInt64WithPrefix(n, []byte{':'})
	}
	return
}

func (c *Client) addReplyInt64WithPrefix(n int64, prefix []byte) {
	s := string(prefix) + strconv.FormatInt(n, 10) + "\r\n"
	c.addReplyString(s)
}

func (c *Client) AddReplyBulk(obj dict.Robj) {
	c.addReplyBulkLen(obj)
	c.AddReply(obj)
	c.addReplyBytes(common.Shared["crlf"])
}

func (c *Client) addReplyBulkLen(obj dict.Robj) {
	s := obj.Val().(sds.SDS)
	slen := strconv.Itoa(s.Len())
	c.addReplyString("$" + slen + "\r\n")
}

func (c *Client) addReplyString(s string) {
	c.reply = append(c.reply, []byte(s)...)
}

func (c *Client) addReplyBytes(b []byte) {
	c.reply = append(c.reply, b...)
}
