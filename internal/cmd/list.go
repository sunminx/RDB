package cmd

import (
	"strconv"

	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/dict"
	"github.com/sunminx/RDB/internal/list"
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

	var ql *list.Quicklist
	val, exists := cli.LookupKeyRead(key)
	if exists {
		if !val.CheckType(dict.ObjList) {
			cli.AddReplyError(common.Shared["wrongtypeerr"])
			return ERR
		} else {
			ql = val.Val().(*list.Quicklist)
		}
	} else {
		ql = list.NewQuicklist()
	}

	var pushednum int64
	for i := 2; i < len(argv); i++ {
		if where == listHead {
			ql.PushLeft(argv[i].Bytes())
		} else if where == listTail {
			ql.Push(argv[i].Bytes())
		}
		pushednum++
	}
	if !exists {
		cli.SetKey(key, dict.NewRobj(ql))
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
	} else if !val.CheckType(dict.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	ql := val.Val().(*list.Quicklist)
	if where == listHead {
		ql.PopLeft()
		cli.AddReplyInt64(1)
	} else if where == listTail {
		ql.Pop()
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
	} else if !val.CheckType(dict.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	idx, err := strconv.ParseInt(argv[2].String(), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	ql := val.Val().(*list.Quicklist)
	entry, ok := ql.Index(idx)
	if !ok {
		cli.AddReplyError([]byte("value is out of range"))
		return ERR
	}
	cli.AddReplyBulk(dict.NewRobj(entry))
	return OK
}

func LLenCommand(cli client) bool {
	key := cli.Key()

	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(dict.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	ql := val.Val().(*list.Quicklist)
	llen := ql.Len()
	cli.AddReplyInt64(llen)
	return OK
}

func LTrimCommand(cli client) bool {
	key := cli.Key()
	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(dict.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}
	argv := cli.Argv()
	var start, end int64
	var err error
	start, err = strconv.ParseInt(argv[2].String(), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	end, err = strconv.ParseInt(argv[3].String(), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}
	ql := val.Val().(*list.Quicklist)
	ql.Trim(start, end)
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func LSetCommand(cli client) bool {
	key := cli.Key()
	val, exists := cli.LookupKeyRead(key)
	if !exists {
		cli.AddReplyRaw(common.Shared["nullbulk"])
		return ERR
	} else if !val.CheckType(dict.ObjList) {
		cli.AddReplyError(common.Shared["wrongtypeerr"])
		return ERR
	}

	argv := cli.Argv()
	index, err := strconv.ParseInt(argv[2].String(), 10, 64)
	if err != nil {
		cli.AddReplyError(common.Shared["invalidindex"])
		return ERR
	}

	ql := val.Val().(*list.Quicklist)
	ql.ReplaceAtIndex(index-1, argv[3].Bytes())
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}
