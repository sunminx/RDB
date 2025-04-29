package networking

import (
	"fmt"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
)

type Server struct {
	gnet.BuiltinEventEngine
	MaxIdleTime   int64
	TcpKeepalive  int
	ProtectedMode bool
	TcpBacklog    int
	Ip            string
	Port          int
	ProtoAddr     string
	MaxFd         int
	Clients       []*Client
	cmds          []cmd.Command
	Requirepass   bool
	DB            *db.DB
	CronLoops     int64
	Hz            int
	LogLevel      string
	LogPath       string
	Version       string
}

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	fd := conn.Fd()
	if s.MaxFd <= fd {
		s.MaxFd = 2 * fd
		oldClients := s.Clients
		s.Clients = initClients(s.MaxFd)
		copy(s.Clients, oldClients)
	}

	cli := NewClient(conn, s.DB)
	cli.srv = s
	cli.LastInteraction = time.Now().UnixMilli()
	s.Clients[fd] = cli
	return nil, gnet.None
}

func (s *Server) OnClose(conn gnet.Conn, err error) (action gnet.Action) {
	fd := conn.Fd()
	if fd > s.MaxFd {
		s.Clients[fd] = nilClient()
	}

	if err != nil {
	}

	return gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) gnet.Action {
	if conn.Fd() > s.MaxFd {
		return gnet.Close
	}

	return s.readQuery(conn)
}

func (s *Server) OnTick() (time.Duration, gnet.Action) {
	s.cron()
	return time.Duration(1000/s.Hz) * time.Millisecond, gnet.None
}

var protoIOBufLen = 1024 * 16

func (s *Server) readQuery(conn gnet.Conn) gnet.Action {
	cli := s.Clients[conn.Fd()]
	if cli == nil || cli.fd == -1 {
		return gnet.Close
	}
	cli.LastInteraction = time.Now().UnixMilli()

	buf, err := conn.Next(-1)
	if err != nil {
		return gnet.Close
	}

	cli.querybuf = append(cli.querybuf, buf...)
	cli.processInputBuffer()

	if (cli.flags & ClientCloseASAP) != 0 {
		return gnet.Close
	}

	if len(cli.reply) > 0 {
		conn.Write(cli.reply)
		cli.reply = make([]byte, 0)
	}

	if (cli.flags & ClientCloseAfterReply) != 0 {
		return gnet.Close
	}
	return gnet.None
}

const defMaxFd = 1024

func NewServer() *Server {
	return &Server{
		MaxIdleTime:   0,
		TcpKeepalive:  300,
		ProtectedMode: false,
		TcpBacklog:    512,
		Ip:            "0.0.0.0",
		Port:          6379,
		ProtoAddr:     fmt.Sprintf("tcp://%s:%d", "0.0.0.0", 6379),
		DB:            db.New(),
		MaxFd:         defMaxFd,
		Clients:       initClients(defMaxFd),
		CronLoops:     0,
		Hz:            100,
		LogLevel:      "notice",
		LogPath:       "/dev/null",
		Version:       "0.0.1",
	}
}

func initClients(n int) []*Client {
	clis := make([]*Client, n+1, n+1)
	for i := 0; i < n+1; i++ {
		clis[i] = nilClient()
	}
	return clis
}

func (s *Server) SetCommandTable(cmds []cmd.Command) {
	s.cmds = cmds
}

func (s *Server) LookupCommand(name string) (cmd.Command, bool) {
	for _, command := range s.cmds {
		if name == command.Name {
			return command, true
		}
	}
	return cmd.EmptyCommand, false
}

const (
	activeExpireCycleSlowTimePerc = 25
)

func (s *Server) cron() {
	s.databasesCron()
	s.clientsCron()
}

func (s *Server) databasesCron() {
	// delete expired key
	expireTimeLimit := 1000000 * activeExpireCycleSlowTimePerc / s.Hz / 100
	s.DB.ActiveExpireCycle(time.Duration(expireTimeLimit))
}

func (s *Server) clientsCron() {
	now := time.Now()
	for i := s.MaxFd; i >= 0; i-- {
		cli := s.Clients[i]
		if cli.isNil() {
			continue
		}
		if cli.handleTimeout(now.UnixMilli()) {
			continue
		}
	}
}

func (s *Server) delClient(fd int) {
	s.Clients = append(s.Clients[:fd], s.Clients[fd+1:]...)
}
