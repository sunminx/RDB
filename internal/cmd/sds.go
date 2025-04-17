package cmd

import (
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

func SetCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()
	for i := 2; i < len(argv); i++ {
		cli.SetKey(key, argv[i])
	}
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func DelCommand(cli client) bool {
	var numdel int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		argvi := argv[i].Val().(sds.SDS)
		cli.DelKey(argvi.String())
		numdel += 1
	}
	cli.AddReplyInt64(numdel)
	return OK
}

func ExistsCommand(cli client) bool {
	var numexists int64
	argv := cli.Argv()
	for i := 1; i < len(argv); i++ {
		argvi := argv[i].Val().(sds.SDS)
		if _, ok := cli.LookupKeyRead(argvi.String()); ok {
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
