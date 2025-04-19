package conf

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/sunminx/RDB/internal/networking"
)

// Load load RDB config file
func Load(server *networking.Server, filename string) {
	var err error
	file, err := os.Open(filename)
	if err != nil {
		err = fmt.Errorf("open configfile %s: %w", filename, err)
		goto loaderr
	}
	defer file.Close()
	{
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
			case args[0] == "timeout" && len(args) == 2:
				var timeout int64
				timeout, err = strconv.ParseInt(args[1], 10, 64)
				if err != nil || timeout < 0 {
					err = errors.New("invalid timeout value")
					goto loaderr
				}
				server.MaxIdleTime = timeout
			case args[0] == "tcp-keepalive" && len(args) == 2:
				var tcpKeepalive int
				tcpKeepalive, err = strconv.Atoi(args[1])
				if err != nil || tcpKeepalive < 0 {
					err = errors.New("invalid tcp-keepalive value")
					goto loaderr
				}
				server.TcpKeepalive = tcpKeepalive
			case args[0] == "protected-mode" && len(args) == 2:
				yesorno, valid := yesnotoi(args[1])
				if !valid {
					err = errors.New(`argument must be 'yes' or 'no'`)
					goto loaderr
				}
				server.ProtectedMode = yesorno
			case args[0] == "bind" && len(args) == 2:
				server.Ip = args[1]
			case args[0] == "port" && len(args) == 2:
				if port, err := strconv.Atoi(args[1]); err == nil {
					server.Port = port
				}
			case args[0] == "loglevel" && len(args) == 2:
				server.LogLevel = args[1]
			case args[0] == "logfile" && len(args) == 2:
				server.LogPath = args[1]
			default:
			}
		}

		if err := scanner.Err(); err != nil && err != io.EOF {
			err = fmt.Errorf("scan configfile %s: %w", filename, err)
			goto loaderr
		}
		return
	}
	server.ProtoAddr = fmt.Sprintf("tcp://%s:%d", server.Ip, server.Port)

loaderr:
	slog.Error(err.Error())
	panic("load rdb.conf")
}

func splitArgs(line string) ([]string, bool) {
	line = strings.Trim(line, " ")
	args := strings.Split(line, " ")
	if len(args) < 2 {
		return args, false
	}
	return args, true
}

func yesnotoi(arg string) (bool, bool) {
	if strings.EqualFold(arg, "yes") {
		return true, true
	}
	if strings.EqualFold(arg, "no") {
		return false, true
	}
	return false, false

}
