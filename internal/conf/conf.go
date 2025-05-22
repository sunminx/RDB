package conf

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
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

			argv, valid := splitArgs(line)
			if !valid {
				continue
			}

			argv[0] = strings.ToLower(argv[0])
			switch {
			case argv[0] == "daemonize" && len(argv) == 2:
				if argv[1] == "yes" {
					server.Daemonize = true
				}
			case argv[0] == "timeout" && len(argv) == 2:
				var timeout int64
				timeout, err = strconv.ParseInt(argv[1], 10, 64)
				if err != nil || timeout < 0 {
					err = errors.New("invalid timeout value")
					goto loaderr
				}
				server.MaxIdleTime = timeout
			case argv[0] == "tcp-keepalive" && len(argv) == 2:
				var tcpKeepalive int
				tcpKeepalive, err = strconv.Atoi(argv[1])
				if err != nil || tcpKeepalive < 0 {
					err = errors.New("invalid tcp-keepalive value")
					goto loaderr
				}
				server.TcpKeepalive = tcpKeepalive
			case argv[0] == "protected-mode" && len(argv) == 2:
				yesorno, valid := yesnotoi(argv[1])
				if !valid {
					err = errors.New(`argument must be 'yes' or 'no'`)
					goto loaderr
				}
				server.ProtectedMode = yesorno
			case argv[0] == "bind" && len(argv) == 2:
				server.Ip = argv[1]
			case argv[0] == "port" && len(argv) == 2:
				if port, err := strconv.Atoi(argv[1]); err == nil {
					server.Port = port
				}
			case argv[0] == "loglevel" && len(argv) == 2:
				server.LogLevel = argv[1]
			case argv[0] == "logfile" && len(argv) == 2:
				server.LogPath = argv[1]
			case argv[0] == "dbfilename" && len(argv) == 2:
				server.RdbFilename = argv[1]
			case argv[0] == "save":
				if len(argv) == 3 {
					seconds, err := strconv.Atoi(argv[1])
					if err != nil {
						goto loaderr
					}
					changes, err := strconv.Atoi(argv[2])
					if err != nil {
						goto loaderr
					}
					if seconds < 1 || changes < 0 {
						goto loaderr
					}
					saveParam := networking.SaveParam{Seconds: seconds, Changes: changes}
					server.SaveParams = append(server.SaveParams, saveParam)
				} else if len(argv) == 2 && argv[1] == "" {
					server.SaveParams = nil
				}
			case argv[0] == "appendonly" && len(argv) == 2:
				if argv[1] == "yes" {
					server.AofState = 1
				}
			case argv[0] == "auto-aof-rewrite-percentage" && len(argv) == 2:
				if perc, err := strconv.Atoi(argv[1]); err != nil {
					server.AofRewritePerc = perc
				}
			case argv[0] == "auto-aof-rewrite-min-size" && len(argv) == 2:
				re := regexp.MustCompile(`(\d+)([k|kb|m|mb|g|gb])`)
				mt := re.FindStringSubmatch(argv[1])
				if len(mt) == 3 {
					v, _ := strconv.Atoi(mt[1])
					unit := mt[2]
					if unit == "k" {
						server.AofRewriteMinSize = 1000 * v
					} else if unit == "kb" {
						server.AofRewriteMinSize = 1024 * v
					} else if unit == "m" {
						server.AofRewriteMinSize = 1000 * 1000 * v
					} else if unit == "mb" {
						server.AofRewriteMinSize = 1024 * 1024 * v
					} else if unit == "g" {
						server.AofRewriteMinSize = 1000 * 1000 * 1000 * v
					} else if unit == "gb" {
						server.AofRewriteMinSize = 1024 * 1024 * 1024 * v
					}
				}
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
	argv := strings.Split(line, " ")
	if len(argv) < 2 {
		return argv, false
	}
	return argv, true
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
