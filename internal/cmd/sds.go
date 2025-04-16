package cmd

import (
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/sds"
)

func GetCommand(cli client) bool {
	robj, ok := cli.LookupKey(cli.Key())
	if !ok {
		cli.AddReply(dict.NewRobj(common.Shared["nullbulk"]))
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
