package cmd

import (
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
	val, exists := cli.Get(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyRaw(shared.RespErrWrongType)
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

	cli.Set(-1, key, val)
	cli.AddDirty(setedNum)
	cli.AddReplyRaw(shared.RespStrOK)
	return OK
}

func HGetCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.Get(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyRaw(shared.RespErrWrongType)
			return ERR
		}
	} else {
		cli.AddReplyRaw(shared.RespBulkNull)
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
	val, exists := cli.Get(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyRaw(shared.RespErrWrongType)
			return ERR
		}
	} else {
		cli.AddReplyRaw(shared.RespBulkNull)
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
	val, exists := cli.Get(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyRaw(shared.RespErrWrongType)
			return ERR
		}
	} else {
		cli.AddReplyRaw(shared.RespBulkNull)
		return ERR
	}

	cli.AddReplyInt64(hash.Len(val))
	return OK
}

func HExistsCommand(cli client) bool {
	key, argv := cli.Key(), cli.Argv()
	val, exists := cli.Get(key)
	if exists {
		if !val.CheckType(obj.TypeHash) {
			cli.AddReplyRaw(shared.RespErrWrongType)
			return ERR
		}
	} else {
		cli.AddReplyRaw(shared.RespBulkNull)
		return ERR
	}

	if hash.Exists(val, argv[2]) {
		cli.AddReplyRaw(shared.RespIntOne)
	} else {
		cli.AddReplyRaw(shared.RespIntZero)
	}
	return OK
}
