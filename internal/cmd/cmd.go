package cmd

import (
	"time"

	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/sds"
)

const (
	OK  = true
	ERR = false
)

type client interface {
	Key() string
	Argv() []sds.SDS
	LookupKeyRead(string) (dict.Robj, bool)
	LookupKeyWrite(string) (dict.Robj, bool)
	SetKey(string, dict.Robj)
	SetExpire(string, time.Duration)
	DelKey(string)
	AddReply(dict.Robj)
	AddReplyRaw([]byte)
	AddReplyStatus([]byte)
	AddReplyError([]byte)
	AddReplyErrorFormat(string, ...any)
	AddReplyInt64(int64)
	AddReplyBulk(dict.Robj)
}

type CommandProc func(client) bool

type Command struct {
	Name         string
	Proc         CommandProc
	Arity        int
	SFlags       string
	Flags        int
	FirstKey     int
	LastKey      int
	KeyStep      int
	MicroSeconds int64
	Calls        int64
}

var EmptyCommand = Command{"", nil, 0, "", 0, 0, 0, 0, 0, 0}

var CommandTable []Command = []Command{
	{"get", GetCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"set", SetCommand, -3, "wm", 0, 1, 1, 1, 0, 0},
	{"del", DelCommand, -2, "w", 0, 1, -1, 1, 0, 0},
	{"exists", ExistsCommand, -2, "rF", 0, 1, -1, 1, 0, 0},
	{"incr", IncrCommand, 2, "wmF", 0, 1, 1, 1, 0, 0},
	{"decr", DecrCommand, 2, "wmF", 0, 1, 1, 1, 0, 0},
	{"append", AppendCommand, 3, "wmF", 0, 1, 1, 1, 0, 0},
	{"strlen", StrlenCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"setex", SetexCommand, 4, "wmF", 0, 1, 1, 1, 0, 0},
}
