package cmd

import (
	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/networking"
	"github.com/sunminx/RDB/internal/sds"
)

func GetCommand(cli *networking.Client) bool {
	robj := cli.Srv.DB.LookupKeyReadOrReply(cli, cli.argv[1])
	if robj == nil {
		return OK
	}

	if robj.Type() != dict.ObjString {
		cli.AddReply(networking.Shared["wrongtypeerr"])
		return ERR
	}

	cli.AddReplyBulk(robj)
	return OK
}

func SetCommand(cli *networking.Client) bool {
	key := string(cli.argv[1].Val().(*sds.SDS).Bytes())
	for i := 2; i < cli.argc; i++ {
		cli.Srv.DB.Add(key, argv[i])
	}
	cli.AddReplyStatus(networking.Shared["ok"])
	return OK
}
