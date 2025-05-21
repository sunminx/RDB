package cmd

import (
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/hash"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

func HSetCommand(cli client) bool {
	return genericHSetCommand(cli)
}

func HMSetCommand(cli client) bool {
	return genericHSetCommand(cli)
}

func genericHSetCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		val = hash.NewRobj(hash.NewZipmap())
	}

	setedNum := 0
	for i := 2; i < len(argv); i += 2 {
		hash.Set(val, argv[i], argv[i+1])
		setedNum++
	}

	cli.SetKey(key, val)
	cli.AddReplyStatus(common.Shared["ok"])
	cli.AddDirty(setedNum)
	return OK
}

func HGetCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	}

	hval, exists := hash.Get(val, argv[2])
	if !exists {
	}
	cli.AddReplyBulk(sds.NewRobj(sds.New(hval)))
	return OK
}

func HDelCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	}

	deletedNum := 0
	for i := 2; i < len(argv); i++ {
		hash.Del(val, argv[i])
		deletedNum++
	}
	cli.AddReplyInt64(int64(deletedNum))
	cli.AddDirty(deletedNum)
	return OK
}

func HLenCommand(cli client) bool {
	key := cli.Key()
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	}

	cli.AddReplyInt64(hash.Len(val))
	return OK
}

func HExistsCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	}

	if hash.Exists(val, argv[2]) {
		cli.AddReplyRaw(common.Shared["cone"])
	} else {
		cli.AddReplyRaw(common.Shared["czero"])
	}
	return OK
}
