package cmd

func CommandCommand(cli client) bool {
	argv := cli.Argv()
	argc := len(argv)

	if argc >= 2 {

	} else if argc == 1 {

	}

	return OK
}
