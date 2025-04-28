package cmd

import (
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/hash"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

func HSetCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()

	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.ObjHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		val = hash.NewRobj(hash.NewZipmap())
	}

	hash.Set(val, argv[2], argv[3])
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func HGetCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()

	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.ObjHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		val = hash.NewRobj(hash.NewZipmap())
	}

	hval, exists := hash.Get(val, argv[2])
	if !exists {

	}
	cli.AddReplyBulk(sds.NewRobj(sds.New(hval)))
	return OK
}
