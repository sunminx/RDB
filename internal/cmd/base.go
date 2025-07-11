package cmd

import (
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/common"
)

func CommandCommand(cli client) bool {
	argv := cli.Argv()
	argc := len(argv)

	if argc >= 2 {

	} else if argc == 1 {

	}

	return OK
}

func MultiCommand(cli client) bool {
	if cli.Multi() {
		cli.AddReplyError([]byte("MULTI calls can not be nested"))
		return ERR
	}
	cli.SetMulti()
	cli.AddReplyStatus(common.Reply["ok"])
	return OK
}

func ExecCommand(cli client) bool {
	if !cli.Multi() {
		cli.AddReplyError([]byte("EXEC without MULTI"))
		return ERR
	}
	cli.MultiExec()
	return OK
}

func FlushAllCommand(cli client) bool {
	_ = cli.Empty()
	cli.AddReplyStatus(common.Reply["ok"])
	return OK
}

func DbSizeCommand(cli client) bool {
	cli.AddReplyInt64(cli.Size())
	return OK
}

func ExpireCommand(cli client) bool {
	now := time.Now()
	return expireGenericCommand(cli, &now, unitSeconds)
}

func ExpireAtCommand(cli client) bool {
	return expireGenericCommand(cli, nil, unitSeconds)
}

func PExpireCommand(cli client) bool {
	now := time.Now()
	return expireGenericCommand(cli, &now, unitMilliseconds)
}

func PExpireAtCommand(cli client) bool {
	return expireGenericCommand(cli, nil, unitMilliseconds)
}

const (
	unitSeconds int = iota + 1
	unitMilliseconds
)

func expireGenericCommand(cli client, basetime *time.Time, unit int) bool {
	key := cli.Key()
	if _, exists := cli.Get(key); !exists {
		cli.AddReplyRaw(common.Reply["czero"])
		return ERR
	}

	expire := int64(cli.Expire(key))
	if expire != -1 && time.Now().UnixMilli()-expire > 0 {
		cli.Del(key)
		cli.AddDirty(1)
		cli.AddReplyRaw(common.Reply["cnone"])
		return ERR
	}

	var err error
	expire, err = strconv.ParseInt(string(cli.Argv()[2]), 10, 64)
	if err != nil {
		cli.AddReplyError([]byte("invalid expire param"))
		return ERR
	}

	if unit == unitSeconds {
		expire *= 1e3
	}

	// Basetime is not nil that means expire param is not a timestamp.
	if basetime != nil {
		expire += basetime.UnixMilli()
	}

	cli.AddDirty(1)

	// Invalid expire timestamp will cause the key to be deleted immediately.
	if ok := cli.SetExpire(time.Duration(expire), key); ok {
		cli.AddReplyRaw(common.Reply["cone"])
	} else {
		cli.AddReplyRaw(common.Reply["czero"])
	}
	return OK
}
