package cmd

import (
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/common"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

func GetCommand(cli client) bool {
	robj, ok := cli.LookupKeyRead(cli.Key())
	if !ok {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return OK
	}
	//if robj.Type() != dict.ObjString {
	//	cli.AddReplyError(common.Shared["wrongtypeerr"])
	//	return ERR
	//}

	cli.AddReplyBulk(robj)
	return OK
}

func SetCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	for i := 2; i < len(argv); i++ {
		_ = setGenericCommand(cli, key, argv[i], nil)
	}
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func SetexCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	_ = setGenericCommand(cli, key, argv[3], argv[2])
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func AppendCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, ok := cli.LookupKeyRead(key)
	if !ok {
		_ = setGenericCommand(cli, key, argv[2], nil)
		return OK
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	sds.Append(val, argv[2])
	_ = setGenericCommand(cli, key, val.Val().(sds.SDS), nil)

	cli.AddReplyBulk(val)
	return OK
}

func setGenericCommand(cli client, key string, val, expire []byte) bool {
	var milliseconds int64
	if expire != nil {
		var err error
		milliseconds, err = strconv.ParseInt(string(expire), 10, 64)
		if err != nil {
			cli.AddReplyErrorFormat(`invalid expire time in %s`, key)
			return OK
		}
		milliseconds *= 1000
	}

	cli.SetKey(key, sds.NewRobj(val))
	if milliseconds > 0 {
		now := time.Now().UnixMilli()
		cli.SetExpire(key, time.Duration(now+milliseconds))
	}
	cli.AddDirty(1)
	return OK
}

func StrlenCommand(cli client) bool {
	key := cli.Key()
	val, ok := cli.LookupKeyRead(key)
	if !ok {
		cli.AddReplyRaw(common.Shared["czero"])
		return OK
	}
	if !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return OK
	}
	cli.AddReplyInt64(sds.Len(val))
	return OK
}

func DelCommand(cli client) bool {
	var numdel int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		cli.DelKey(string(argv[i]))
		numdel += 1
	}
	cli.AddReplyInt64(numdel)
	cli.AddDirty(1)
	return OK
}

func ExistsCommand(cli client) bool {
	var numexists int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		if _, ok := cli.LookupKeyRead(string(argv[i])); ok {
			numexists += 1
		}
	}

	cli.AddReplyInt64(numexists)
	return OK
}

func IncrCommand(cli client) bool {
	return incrdecrCommand(cli, cli.Key(), 1)
}

func DecrCommand(cli client) bool {
	return incrdecrCommand(cli, cli.Key(), -1)
}

func incrdecrCommand(cli client, key string, n int64) bool {
	val, ok := cli.LookupKeyWrite(key)
	if !ok || !val.CheckType(obj.TypeString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	cli.AddReplyInt64(sds.Incr(val, n))
	cli.AddDirty(1)
	return OK
}
