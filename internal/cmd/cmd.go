package cmd

import (
	"github.com/sunminx/RDB/internal/dict"
)

const (
	OK  = true
	ERR = false
)

type client interface {
	Key() string
	Argv() []dict.Robj
	LookupKey(string) (dict.Robj, bool)
	SetKey(string, dict.Robj)
	DelKey(string)
	AddReply(dict.Robj)
	AddReplyStatus([]byte)
	AddReplyInt64(int64)
	AddReplyError([]byte)
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
}
