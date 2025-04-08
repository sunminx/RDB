package conf

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/sunminx/RDB/internal/server"
)

// Load load RDB config file
func Load(server *server.Server, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open configfile %s: %w", filename, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, " ") {
			continue
		}

		args, valid := splitArgs(line)
		if !valid {
			continue
		}

		args[0] = strings.ToLower(args[0])
		switch {
		case args[0] == "ip" && len(args) == 2:
			server.Ip = args[0]
		case args[0] == "port" && len(args) == 2:
			if port, err := strconv.Atoi(args[0]); err == nil {
				server.Port = port
			}

		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("scan configfile %s: %w", filename, err)
	}
	return nil
}

func splitArgs(line string) ([]string, bool) {
	line = strings.Trim(line, " ")
	args := strings.Split(line, " ")
	if len(args) < 3 {
		return args, false
	}
	return args, true
}
