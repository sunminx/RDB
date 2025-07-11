package cmd

import (
	"time"

	obj "github.com/sunminx/RDB/internal/object"
)

const (
	OK  = true
	ERR = false
)

type client interface {
	Key() string
	Argv() [][]byte
	Multi() bool
	SetMulti()
	MultiExec()
	Size() int64
	SetExpire(time.Duration, string) bool
	Expire(string) time.Duration
	Get(string) (*obj.Robj, bool)
	Set(time.Duration, string, *obj.Robj) bool
	Del(string)
	Empty() int
	AddDirty(int)
	AddReply(*obj.Robj)
	AddReplyRaw([]byte)
	AddReplyStatus([]byte)
	AddReplyError([]byte)
	AddReplyErrorFormat(string, ...any)
	AddReplyInt64(int64)
	AddReplyUint64(uint64)
	AddReplyBulk(*obj.Robj)
	AddReplyMultibulk([]*obj.Robj)
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
	{"setnx", SetNxCommand, 3, "wmF", 0, 1, 1, 1, 0, 0},
	{"expire", ExpireCommand, 3, "wF", 0, 1, 1, 1, 0, 0},
	{"expireat", ExpireAtCommand, 3, "wF", 0, 1, 1, 1, 0, 0},
	{"pexpire", PExpireCommand, 3, "wF", 0, 1, 1, 1, 0, 0},
	{"pexpireat", PExpireAtCommand, 3, "wF", 0, 1, 1, 1, 0, 0},
	{"del", DelCommand, -2, "w", 0, 1, -1, 1, 0, 0},
	{"exists", ExistsCommand, -2, "rF", 0, 1, -1, 1, 0, 0},
	{"incr", IncrCommand, 2, "wmF", 0, 1, 1, 1, 0, 0},
	{"decr", DecrCommand, 2, "wmF", 0, 1, 1, 1, 0, 0},
	{"append", AppendCommand, 3, "wmF", 0, 1, 1, 1, 0, 0},
	{"strlen", StrlenCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"setex", SetExCommand, 4, "wmF", 0, 1, 1, 1, 0, 0},
	{"rpush", RPushCommand, -3, "wmF", 0, 1, 1, 1, 0, 0},
	{"lpush", LPushCommand, -3, "wmF", 0, 1, 1, 1, 0, 0},
	{"rpop", RPopCommand, 2, "wF", 0, 1, 1, 1, 0, 0},
	{"lpop", LPopCommand, 2, "wF", 0, 1, 1, 1, 0, 0},
	{"llen", LLenCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"lindex", LIndexCommand, 3, "r", 0, 1, 1, 1, 0, 0},
	{"ltrim", LTrimCommand, 4, "w", 0, 1, 1, 1, 0, 0},
	{"lset", LSetCommand, 4, "wm", 0, 1, 1, 1, 0, 0},
	{"hset", HSetCommand, 4, "wmF", 0, 1, 1, 1, 0, 0},
	{"hmset", HMSetCommand, -4, "wmF", 0, 1, 1, 1, 0, 0},
	{"hget", HGetCommand, 3, "rF", 0, 1, 1, 1, 0, 0},
	{"hdel", HDelCommand, -3, "wF", 0, 1, 1, 1, 0, 0},
	{"hlen", HLenCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"hexists", HExistsCommand, 3, "rF", 0, 1, 1, 1, 0, 0},
	{"multi", MultiCommand, 1, "sF", 0, 0, 0, 0, 0, 0},
	{"exec", ExecCommand, 1, "sM", 0, 0, 0, 0, 0, 0},
	{"flushdb", FlushAllCommand, -1, "w", 0, 0, 0, 0, 0, 0},
	{"flushall", FlushAllCommand, -1, "w", 0, 0, 0, 0, 0, 0},
	{"dbsize", DbSizeCommand, 1, "rF", 0, 0, 0, 0, 0, 0},
}
