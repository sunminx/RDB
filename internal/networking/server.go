package networking

import (
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/sds"
)

type Server struct {
	gnet.BuiltinEventEngine
	MaxIdleTime   int
	TcpKeepalive  int
	ProtectedMode bool
	TcpBacklog    int
	Ip            string
	Port          int
	MaxFd         int
	Clients       []*Client
	Requirepass   bool
	DB            *db.DB
	CronLoops     int64
	Hz            int
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
	cli.setServer(s)
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

	buf, err := conn.Next(-1)
	if err != nil {
		return gnet.Close
	}

	cli.querybuf.Cat(sds.New(buf))
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
		DB:            db.New(),
		MaxFd:         defMaxFd,
		Clients:       initClients(defMaxFd),
		CronLoops:     0,
		Hz:            100,
	}
}

func initClients(n int) []*Client {
	clis := make([]*Client, n, n)
	for i := 0; i < n; i++ {
		clis[i] = nilClient()
	}
	return clis
}

func LookupCommand(name string) (cmd.Command, bool) {
	for _, command := range cmd.CommandTable {
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
}

func (s *Server) databasesCron() {
	// delete expired key
	expireTimeLimit := 1000000 * activeExpireCycleSlowTimePerc / s.Hz / 100
	s.DB.ActiveExpireCycle(time.Duration(expireTimeLimit))
}
