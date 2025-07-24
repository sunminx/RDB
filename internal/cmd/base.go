package cmd

import (
	"strconv"
	"time"
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
	cli.AddReplyRaw(shared.RespStrOK)
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
	cli.AddReplyRaw(shared.RespStrOK)
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
	unitNone int = iota
	unitSeconds
	unitMilliseconds
)

func expireGenericCommand(cli client, basetime *time.Time, unit int) bool {
	key := cli.Key()
	if _, exists := cli.Get(key); !exists {
		cli.AddReplyRaw(shared.RespIntZero)
		return ERR
	}

	expires := int64(cli.Expires(key))
	if expires != -1 && time.Now().After(time.UnixMilli(expires)) {
		cli.Del(key)
		cli.AddDirty(1)
		cli.AddReplyRaw(shared.RespIntOne)
		return ERR
	}

	var err error
	expires, err = strconv.ParseInt(string(cli.Argv()[2]), 10, 64)
	if err != nil {
		cli.AddReplyError([]byte("invalid expire param"))
		return ERR
	}

	if unit == unitSeconds {
		expires *= 1e3
	}

	// Basetime is not nil that means expire param is not a timestamp.
	if basetime != nil {
		expires += basetime.UnixMilli()
	}

	cli.AddDirty(1)

	// Invalid expire timestamp will cause the key to be deleted immediately.
	if ok := cli.SetExpire(time.Duration(expires), key); ok {
		cli.AddReplyRaw(shared.RespIntOne)
	} else {
		cli.AddReplyRaw(shared.RespIntZero)
	}
	return OK
}
