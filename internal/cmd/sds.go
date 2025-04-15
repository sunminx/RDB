package cmd

import (
	"github.com/sunminx/RDB/internal/common"
)

func GetCommand(cli client) bool {
	robj := cli.LookupKey(cli.Key())
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
