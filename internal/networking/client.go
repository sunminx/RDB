package networking

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/db"
	obj "github.com/sunminx/RDB/internal/object"
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
	srv *Server

	flags         ClientFlag
	authenticated bool

	querybuf     sds.SDS
	reqtype      reqType
	multibulklen int
	bulklen      int

	argc int
	argv []sds.SDS

	reply []byte

	Cmd             cmd.Command
	LastInteraction int64
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
		argv:          make([]sds.SDS, 0),
		reply:         make([]byte, 0),
		authenticated: true,
	}
}

func nilClient() *Client {
	cli := new(Client)
	cli.fd = -1
	return cli
}

func (c *Client) isNil() bool {
	return c.fd == -1
}

func (c *Client) Key() string {
	return c.argvByIdx(1)
}

func (c *Client) argvByIdx(n int) string {
	if c.argc < n-1 {
		return ""
	}

	return string(c.argv[n].Bytes())
}

func (c *Client) Argv() []sds.SDS {
	return c.argv
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
			c.argv = make([]sds.SDS, 0)
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

func splitArgs(bytes []byte, sep byte) []sds.SDS {
	res := make([]sds.SDS, 0)
	i := 0
	for j := 0; j < len(bytes); j++ {
		if bytes[j] == sep {
			res = append(res, sds.New(bytes[i:j]))
			i = j
		}
	}
	res = append(res, sds.New(bytes[i:]))
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

		if ll > maxMulitbulksWhileUnauth && c.srv.Requirepass && !c.authenticated {
			c.AddReplyError([]byte("Protocol error: unauthenticated multibulk length"))
			c.setProtocolError()
			return false
		}

		if ll <= 0 {
			return true
		}

		c.multibulklen = ll
		c.argv = make([]sds.SDS, ll)
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
			if ll > maxBulksWhileUnauth && c.srv.Requirepass && !c.authenticated {
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

		c.argv[c.argc] = s
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
	cmd, ok := c.srv.LookupCommand(name)
	if !ok {
		var args string
		for i := 1; i < c.argc; i++ {
			args += fmt.Sprintf("%q, ", c.argv[i].String())
			if len(args) >= 128 {
				args = args[:128]
				break
			}
		}
		c.AddReplyErrorFormat(`unknown command %q, with args beginning with: %s`,
			name, args)
		goto clean
	} else if (cmd.Arity > 0 && cmd.Arity != c.argc) || (c.argc < -cmd.Arity) {
		c.AddReplyErrorFormat(`wrong number of arguments for %q command`, name)
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

func (c *Client) AddReply(robj obj.Robj) {
	if robj.SDSEncodedObject() {
		c.AddReplySds(robj.Val().(sds.SDS))
	} else {
		num := robj.Val().(int64)
		c.addReplyString(strconv.FormatInt(num, 10))
	}
	return
}

func (c *Client) AddReplySds(s sds.SDS) {
	c.reply = append(c.reply, s.Bytes()...)
}

func (c *Client) AddReplyRaw(bytes []byte) {
	c.reply = append(c.reply, bytes...)
}

func (c *Client) AddReplyError(err []byte) {
	if len(err) == 0 || err[0] != '-' {
		c.reply = append(c.reply, []byte("-ERR ")...)
	}

	c.reply = append(c.reply, err...)
	c.reply = append(c.reply, []byte("\r\n")...)
}

func (c *Client) AddReplyErrorFormat(format string, args ...any) {
	err := fmt.Sprintf(format, args...)
	c.AddReplyError([]byte(err))
}

func (c *Client) AddReplyStatus(status []byte) {
	c.reply = append(c.reply, '+')
	c.reply = append(c.reply, status...)
	c.reply = append(c.reply, []byte("\r\n")...)
}

func (c *Client) AddReplyInt64(n int64) {
	if n == 0 {
		c.AddReplyRaw(common.Shared["czero"])
	} else if n == 1 {
		c.AddReplyRaw(common.Shared["cone"])
	} else {
		c.addReplyInt64WithPrefix(n, []byte{':'})
	}
	return
}

func (c *Client) addReplyInt64WithPrefix(n int64, prefix []byte) {
	s := string(prefix) + strconv.FormatInt(n, 10) + "\r\n"
	c.addReplyString(s)
}

func (c *Client) AddReplyBulk(robj obj.Robj) {
	c.addReplyBulkLen(robj)
	c.AddReply(robj)
	c.AddReplyRaw(common.Shared["crlf"])
}

func (c *Client) addReplyBulkLen(robj obj.Robj) {
	var slen string
	if robj.SDSEncodedObject() {
		s := robj.Val().(sds.SDS)
		slen = strconv.Itoa(s.Len())
	} else {
		n := robj.Val().(int64)

		_len := 1
		if n < 0 {
			_len += 1
			n = -n
		}

		for n = n / 10; n != 0; n = n / 10 {
			_len += 1
		}
		slen = strconv.Itoa(_len)
	}
	c.addReplyString("$" + slen + "\r\n")
}

func (c *Client) addReplyString(s string) {
	c.reply = append(c.reply, []byte(s)...)
}

func (c *Client) handleTimeout(now int64) bool {
	timeouted := c.srv.MaxIdleTime > 0 && (now-c.LastInteraction) > c.srv.MaxIdleTime
	if timeouted {
		c.free()
		c.srv.delClient(c.fd)
	}
	return timeouted
}

func (c *Client) free() {
	c.Close()
}
