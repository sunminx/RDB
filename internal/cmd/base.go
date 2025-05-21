package cmd

import "github.com/sunminx/RDB/internal/common"

func CommandCommand(cli client) bool {
	argv := cli.Argv()
	argc := len(argv)

	if argc >= 2 {

	} else if argc == 1 {

	}

	return OK
}

func MultiCommand(cli client) bool {
	if cli.Multi() {
		cli.AddReplyError([]byte("MULTI calls can not be nested"))
		return ERR
	}
	cli.SetMulti()
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}

func ExecCommand(cli client) bool {
	if !cli.Multi() {
		cli.AddReplyError([]byte("EXEC without MULTI"))
		return ERR
	}
	cli.MultiExec()
	return OK
}

func FlushAllCommand(cli client) bool {
	_ = cli.Empty()
	cli.AddReplyStatus(common.Shared["ok"])
	return OK
}
