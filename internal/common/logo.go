package common

import "fmt"

func Logo(args ...any) string {
	var format = "\n"

	format += ` ____    ____    ____  ` + "\n"
	format += `|  _ \  |  _ \  | __ ) ` + "\t\tRDB %s 64 bit\n"
	format += `| |_) | | | | | |  _ \ ` + "\n"
	format += `|  _ <  | |_| | | |_) |` + "\n"
	format += `|_| \_\ |____/  |____/ ` + "\n"
	format += `                       ` + "\n"

	return fmt.Sprintf(format, args...)
}
