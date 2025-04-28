package cmd

import (
	"strconv"

	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/list"
	obj "github.com/sunminx/RDB/internal/object"
	"github.com/sunminx/RDB/internal/sds"
)

const (
	listHead int8 = 0
	listTail int8 = 1
)

func RPushCommand(cli client) bool {
	return pushGenericCommand(cli, listTail)
}

func LPushCommand(cli client) bool {
	return pushGenericCommand(cli, listHead)
}

func pushGenericCommand(cli client, where int8) bool {
	key := cli.Key()
	argv := cli.Argv()

	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(obj.ObjList) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		}
	} else {
		val = list.NewRobj(list.NewQuicklist())
	}

	var pushednum int64
	for i := 2; i < len(argv); i++ {
		if where == listHead {
			list.PushLeft(val, argv[i])
		} else if where == listTail {
			list.Push(val, argv[i])
		}
		pushednum++
	}
	if !exists {
		cli.SetKey(key, val)
	}
	cli.AddReplyInt64(pushednum)
	return OK
}

func RPopCommand(cli client) bool {
	return popGenericCommand(cli, listTail)
}

func LPopCommand(cli client) bool {
	return popGenericCommand(cli, listHead)
}

func popGenericCommand(cli client, where int8) bool {
	key := cli.Key()

	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(obj.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	if where == listHead {
		list.PopLeft(val)
		cli.AddReplyInt64(1)
	} else if where == listTail {
		list.Pop(val)
		cli.AddReplyInt64(1)
	} else {
		cli.AddReplyInt64(0)
	}
	return OK
}

func LIndexCommand(cli client) bool {
	key := cli.Key()
	argv := cli.Argv()

	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(obj.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	idx, err := strconv.ParseInt(string(argv[2]), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	entry, ok := list.Index(val, idx)
	if !ok {
		cli.AddReplyError([]byte("value is out of range"))
		return ERR
	}
	cli.AddReplyBulk(sds.NewRobj(sds.New(entry)))
	return OK
}

func LLenCommand(cli client) bool {
	key := cli.Key()

	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(obj.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	llen := list.Len(val)
	cli.AddReplyInt64(llen)
	return OK
}

func LTrimCommand(cli client) bool {
	key := cli.Key()
	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(obj.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	argv := cli.Argv()
	var start, end int64
	var err error
	start, err = strconv.ParseInt(string(argv[2]), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	end, err = strconv.ParseInt(string(argv[3]), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	list.Trim(val, start, end)
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func LSetCommand(cli client) bool {
	key := cli.Key()
	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(obj.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	argv := cli.Argv()
	index, err := strconv.ParseInt(string(argv[2]), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}

	list.Set(val, index-1, argv[3])
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}
