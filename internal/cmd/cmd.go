package cmd

import "github.com/sunminx/RDB/internal/networking"

const (
	OK  = true
	ERR = false
)

type CommandProc func(networking.Client) bool

type Command struct {
	Name         string
	Proc         CommandProc
	Arity        int
	SFlags       string
	Flags        int
	FirstKey     int
	LastKey      int
	KeyStep      int
	MicroSeconds int64
	Calls        int64
}

var commandTable []Command = []Command{
	{"get", GetCommand, 2, "rF", 0, 1, 1, 1, 0, 0},
	{"set", SetCommand, -3, "wm", 0, 1, 1, 1, 0, 0},
}

const emptyCommand = Command{"", nil, 0, "", 0, 0, 0, 0, 0, 0}

func LookupCommand(name string) (Command, bool) {
	for _, cmd := range commandTable {
		if name == cmd.Name {
			return cmd, true
		}
	}
	return emptyCommand, false
}
