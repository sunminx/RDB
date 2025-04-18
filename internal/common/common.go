package common

import "fmt"

var Shared map[string][]byte = map[string][]byte{
	"wrongtypeerr": []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
	"crlf":         []byte("\r\n"),
	"ok":           []byte("OK"),
	"czero":        []byte(":0\r\n"),
	"cone":         []byte(":1\r\n"),
	"nullbulk":     []byte("$-1\r\n"),
}

func Logo(args ...any) string {
	var format string

	format += "                                        \n"
	format += "ooooooooo.   oooooooooo.   oooooooooo.  \n"
	format += "`888   `Y88. `888'   `Y8b  `888'   `Y8b \n"
	format += " 888   .d88'  888      888  888     888 	RDB %s 64 bit\n"
	format += " 888ooo88P'   888      888  888oooo888' \n"
	format += " 888`88b.     888      888  888    `88b \n"
	format += " 888  `88b.   888     d88'  888    .88P \n"
	format += "o888o  o888o o888bood8P'   o888bood8P'  \n"
	format += "                                        \n"

	return fmt.Sprintf(format, args...)
}
