package cmd

import (
	"strconv"
	"time"

	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/dict"
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

var emptySDS = sds.NewEmpty()

func SetCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	for i := 2; i < len(argv); i++ {
		_ = setGenericCommand(cli, key, argv[i], emptySDS)
	}
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func SetexCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	_ = setGenericCommand(cli, key, argv[3], argv[2])
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func AppendCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	robj, ok := cli.LookupKeyRead(key)
	if !ok {
		_ = setGenericCommand(cli, key, argv[2], emptySDS)
		return OK
	}
	if !robj.CheckType(dict.ObjString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	val := robj.Val().(sds.SDS)
	val.Cat(argv[2])
	robj.SetVal(val)
	_ = setGenericCommand(cli, key, val, emptySDS)

	cli.AddReplyBulk(robj)
	return OK
}

func StrlenCommand(cli client) bool {
	key := cli.Key()
	robj, ok := cli.LookupKeyRead(key)
	if !ok {
		cli.AddReplyRaw(common.Shared["czero"])
		return OK
	}
	if !robj.CheckType(dict.ObjString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return OK
	}
	cli.AddReplyInt64(robj.StringObjectLen())
	return OK
}

func setGenericCommand(cli client, key string, val, expire sds.SDS) bool {
	var milliseconds int64

	if !expire.IsEmpty() {
		var err error
		milliseconds, err = strconv.ParseInt(expire.String(), 10, 64)
		if err != nil {
			cli.AddReplyErrorFormat(`invalid expire time in %s`, key)
			return OK
		}
	}

	cli.SetKey(key, dict.NewRobj(val))
	if !expire.IsEmpty() {
		now := time.Now().UnixMilli()
		cli.SetExpire(key, time.Duration(now+milliseconds))
	}
	return OK
}

func DelCommand(cli client) bool {
	var numdel int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		cli.DelKey(argv[i].String())
		numdel += 1
	}
	cli.AddReplyInt64(numdel)
	return OK
}

func ExistsCommand(cli client) bool {
	var numexists int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		if _, ok := cli.LookupKeyRead(argv[i].String()); ok {
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
	if !ok || !val.CheckType(dict.ObjString) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	num, ok := val.Int64Val()
	if !ok {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	num += n

	cli.SetKey(key, dict.NewRobj(num))
	cli.AddReplyInt64(num)
	return OK
}
