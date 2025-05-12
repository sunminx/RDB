package networking

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/db"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

// Client flags
type flag int

const (
	Save flag = 1 << iota
	master
	monitor
	multi
	blocked
	dirtyCas
	closeAfterReply
	closeASAP
	queueCall
	none = 0
)

type reqType int

const (
	reqNone reqType = iota
	reqMultibulk
	reqInline
)

type Client struct {
	gnet.Conn
	*db.DB
	fd              int
	srv             *Server
	flag            flag
	authenticated   bool
	querybuf        []byte
	reqtype         reqType
	multibulklen    int
	bulklen         int
	argc            int
	argv            [][]byte
	cmd             cmd.Command
	multiState      *multiState
	reply           []byte
	lastInteraction int64
	cmdLock         sync.RWMutex
}

type multiState struct {
	commands []multiCmd
	cnt      int64
}

type multiCmd struct {
	cmd  cmd.Command
	argc int
	argv [][]byte
}

func newMultiState() *multiState {
	return &multiState{
		commands: make([]multiCmd, 0),
		cnt:      0,
	}
}

func NewClient(conn gnet.Conn, db *db.DB) *Client {
	return &Client{
		Conn:          conn,
		DB:            db,
		fd:            conn.Fd(),
		querybuf:      make([]byte, 0),
		multibulklen:  0,
		bulklen:       -1,
		reqtype:       reqNone,
		argc:          0,
		argv:          make([][]byte, 0),
		reply:         make([]byte, 0),
		authenticated: true,
	}
}

func nilClient() *Client {
	cli := new(Client)
	cli.fd = -1
	return cli
}

func (c *Client) Key() string {
	return c.argvByIdx(1)
}

func (c *Client) argvByIdx(n int) string {
	if c.argc < n-1 {
		return ""
	}
	return string(c.argv[n])
}

func (c *Client) Argv() [][]byte {
	return c.argv
}

func (c *Client) Multi() bool {
	return c.checkFlag(multi)
}

func (c *Client) SetMulti() {
	c.setFlag(multi)
}

func (c *Client) checkFlag(flag flag) bool {
	return c.flag&flag != 0
}

func (c *Client) setFlag(flag flag) {
	c.flag |= flag
}

var (
	ProtoInlineMaxSize = 1024 * 64
)

const (
	execed  = true
	nonExec = false
)

// processInputBuffer process the query buffer for client 'c'.
func (c *Client) processInputBuffer() bool {
	for len(c.querybuf) > 0 {
		if c.reqtype == reqNone {
			if c.querybuf[0] == '*' {
				c.reqtype = reqMultibulk
			} else {
				c.reqtype = reqInline
			}
		}

		if c.reqtype == reqInline {
			if !c.processInlineBuffer() {
				// Even if the request verification is illegal and the command is prematurely terminated.
				// this processing will be regarded as completed.
				break
			}
		} else if c.reqtype == reqMultibulk {
			if !c.processMultibulkBuffer() {
				// Even if the request verification is illegal and the command is prematurely terminated.
				// this processing will be regarded as completed.
				break
			}
		} else {
			panic("unknown request type")
		}

		if c.argc == 0 {
			c.argv = make([][]byte, 0)
			c.multibulklen = 0
			c.bulklen = -1
			c.reqtype = reqNone
		} else {
			// As long as the execution of one command is terminated, it is considered that the execution
			// conditions of the subsequent commands of this client are not met, and the execution is stopped.
			if !c.processCommand() {
				return nonExec
			}
		}
	}
	return execed
}

// processInlineBuffer
func (c *Client) processInlineBuffer() bool {
	var line []byte
	line, c.querybuf = splitLine(c.querybuf)
	if line == nil {
		return false
	}
	if line == nil {
		if len(c.querybuf) > ProtoInlineMaxSize {
			c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
			c.setProtocolError()
		}
		return false
	}
	c.argv = splitArgs(line, ' ')
	if len(c.argv) == 0 {
		c.AddReplyError([]byte("Protocol error: unbalanced quotes in request"))
		c.setProtocolError()
	}
	c.argc = len(c.argv)
	return true
}

const maxMulitbulksWhileUnauth = 10
const maxBulksWhileUnauth = 16384

var protoMaxBulkLen = 1024 * 1024 * 512

// processMulitbulkBuffer
func (c *Client) processMultibulkBuffer() bool {
	var line []byte
	if c.multibulklen == 0 {
		line, c.querybuf = splitLine(c.querybuf)
		if line == nil {
			return false
		}
		if line == nil {
			if len(c.querybuf) > ProtoInlineMaxSize {
				c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
				c.setProtocolError()
			}
			return false
		}

		if line[0] != '*' {
			return false
		}

		ll, err := strconv.Atoi(string(line[1:]))
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
		c.argv = make([][]byte, ll)
	}

	for c.multibulklen > 0 {
		// new bulk
		if c.bulklen == -1 {
			line, c.querybuf = splitLine(c.querybuf)
			if line == nil {
				return false
			}
			if line == nil {
				if len(c.querybuf) > ProtoInlineMaxSize {
					c.AddReplyError([]byte("Protocol error: too big mbulk count string"))
					c.setProtocolError()
					return false
				}
				break
			}

			// in request, only bulk string in multibulk allowed
			if line[0] != '$' {
				c.AddReplyError([]byte(fmt.Sprintf(`Protocol error: expected '$', got '%c'`, line[0])))
				c.setProtocolError()
				return false
			}
			ll, err := strconv.Atoi(string(line[1:]))
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
		if len(c.querybuf) < c.bulklen+2 {
			// Not enough data (+2 == trailing \r\n)
			break
		}

		var arg []byte
		arg, c.querybuf = splitLine(c.querybuf)
		if len(arg) != c.bulklen {
			c.AddReplyError([]byte("Protocol error: incorrect bulk length"))
			c.setProtocolError()
			return false
		}

		c.argv[c.argc] = arg
		c.argc += 1
		c.bulklen = -1
		c.multibulklen -= 1
	}
	return c.multibulklen == 0
}

func splitLine(b []byte) ([]byte, []byte) {
	idx := slices.Index(b, '\n')
	if idx == -1 {
		return nil, b
	}
	if b[idx-1] == '\r' {
		idx -= 1
	}
	line := b[:idx]
	// skip '\r\n'
	b = b[idx+2:]
	return line, b
}

func splitArgs(bytes []byte, sep byte) [][]byte {
	res := make([][]byte, 0)
	i := 0
	for j := 0; j < len(bytes); j++ {
		if bytes[j] == sep {
			res = append(res, bytes[i:j])
			i = j
		}
	}
	res = append(res, bytes[i:])
	return res
}

func (c *Client) setProtocolError() {
	c.flag |= closeAfterReply
}

func (c *Client) processCommand() bool {
	name := c.argvByIdx(0)
	name = strings.ToLower(name)
	if name == "quit" {
		c.flag |= closeASAP
		return true
	}
	command, ok := c.srv.LookupCommand(name)
	if !ok {
		var args string
		for i := 1; i < c.argc; i++ {
			args += fmt.Sprintf("%q, ", string(c.argv[i]))
			if len(args) >= 128 {
				args = args[:128]
				break
			}
		}
		c.AddReplyErrorFormat(`unknown command %q, with args beginning with: %s`,
			name, args)
		goto clean
	} else if (command.Arity > 0 && command.Arity != c.argc) || (c.argc < -command.Arity) {
		c.AddReplyErrorFormat(`wrong number of arguments for %q command`, name)
		goto clean
	}

	c.cmd = command
	if c.flag&multi != 0 && c.cmd.Name != "exec" {
		c.queueMultiCommand()
		c.AddReplyRaw([]byte("+QUEUED\r\n"))
		return execed
	} else {
		if !c.call() {
			return nonExec
		}
	}

clean:
	c.argc = 0
	return execed
}

func (c *Client) queueMultiCommand() {
	multiState := c.multiState
	if multiState == nil {
		multiState = newMultiState()
		c.multiState = multiState
	}
	multiCmd := multiCmd{c.cmd, c.argc, c.argv}
	multiState.commands = append(multiState.commands, multiCmd)
	multiState.cnt += 1
	c.argc = 0
}

func (c *Client) MultiExec() {
	multiState := c.multiState
	c.addReplyMultibulkLen(int64(multiState.cnt))
	var i int64
	for ; i < multiState.cnt; i++ {
		multiCmd := multiState.commands[i]
		c.argc = multiCmd.argc
		c.argv = multiCmd.argv
		multiCmd.cmd.Proc(c)
	}
	c.multiState = nil
	c.flag &= ^multi
}

func (c *Client) call() bool {
	if c.cmdLock.TryLock() {
		_ = c.cmd.Proc(c)
		c.flag &= ^queueCall
		c.cmdLock.Unlock()
		c.srv.UnlockNotice <- struct{}{}
		return execed
	} else {
		c.flag |= queueCall
		c.srv.RunnableClientCh <- c
		return nonExec
	}
}

func (c *Client) Wake() {
	// Wake triggers a OnTraffic event for the current connection.
	c.Conn.Wake(nil)
}

// AddReply output the complete value to client.
func (c *Client) AddReply(robj *obj.Robj) {
	switch robj.Type() {
	case obj.TypeString:
		{
			var reply []byte
			if robj.CheckEncoding(obj.EncodingRaw) {
				reply = robj.Val().(sds.SDS)
			} else if robj.CheckEncoding(obj.EncodingInt) {
				num := robj.Val().(int64)
				reply = []byte(strconv.FormatInt(num, 10))
			}
			c.AddReplyStatus(reply)
		}
	case obj.TypeList:
	case obj.TypeHash:
	default:
	}
}

// AddReply output a byte slices to client. You need to complete the encode in advance.
func (c *Client) AddReplyRaw(bytes []byte) {
	c.reply = append(c.reply, bytes...)
}

// AddReplyError output simple errors to client. eg: "-Err message\r\n".
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

// AddReplyStatus output simple strings to client. eg: "+OK\r\n".
func (c *Client) AddReplyStatus(status []byte) {
	c.reply = append(c.reply, '+')
	c.reply = append(c.reply, status...)
	c.reply = append(c.reply, []byte("\r\n")...)
}

// AddReplyInt64 output a signed, base-10, 64-bit integer to client. eg: ":0\r\n".
func (c *Client) AddReplyInt64(n int64) {
	if n == 0 {
		c.AddReplyRaw(common.Shared["czero"])
	} else if n == 1 {
		c.AddReplyRaw(common.Shared["cone"])
	} else {
		s := ":" + strconv.FormatInt(n, 10) + "\r\n"
		c.AddReplyRaw([]byte(s))
	}
}

// AddReplyBulk output bulk strings to client. bulk strings represents a single binary
// string. The string can be of any size. eg: "$6\r\nfoobar\r\n".
func (c *Client) AddReplyBulk(robj *obj.Robj) {
	if robj.CheckEncoding(obj.EncodingRaw) {
		s := robj.Val().(sds.SDS)
		c.AddReplyRaw([]byte("$" + strconv.Itoa(s.Len()) + "\r\n"))
		c.AddReplyRaw(s.Bytes())
	} else {
		val := robj.Val().(int64)
		n := val
		ln := 1
		if n < 0 {
			ln += 1
			n = -n
		}

		for n = n / 10; n != 0; n = n / 10 {
			ln += 1
		}
		c.AddReplyRaw([]byte("$" + strconv.Itoa(ln) + "\r\n"))
		c.AddReplyRaw([]byte(strconv.FormatInt(val, 10)))
	}
	c.AddReplyRaw(common.Shared["crlf"])
}

// AddReplyMultibulk output arrays to client. eg: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".
func (c *Client) AddReplyMultibulk(robjs []*obj.Robj) {
	c.addReplyMultibulkLen(int64(len(robjs)))
	for _, robj := range robjs {
		c.AddReplyBulk(robj)
	}
	return
}

func (c *Client) addReplyMultibulkLen(ln int64) {
	c.AddReplyRaw([]byte(fmt.Sprintf("*%d\r\n", ln)))
}

func (c *Client) handleTimeout(now int64) bool {
	timeouted := c.srv.MaxIdleTime > 0 && (now-c.lastInteraction) > c.srv.MaxIdleTime
	if timeouted {
		c.free()
		c.srv.delClient(c.fd)
	}
	return timeouted
}

func (c *Client) free() {
	c.Close()
}
