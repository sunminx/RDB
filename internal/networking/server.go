package networking

import (
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
}

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	fd := conn.Fd()
	if s.MaxFd <= fd {
		s.MaxFd = 2 * fd
		oldClients := s.Clients
		s.Clients = initClients(s.MaxFd)
		copy(s.Clients, oldClients)
	}

	s.Clients[fd] = NewClient(conn)
	return nil, gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) gnet.Action {
	if conn.Fd() > s.MaxFd {
		return gnet.Close
	}

	return s.readQuery(conn)
}

var protoIOBufLen = 1024 * 16

func (s *Server) readQuery(conn gnet.Conn) gnet.Action {
	cli := s.Clients[conn.Fd()]
	if cli == nil || cli.fd == -1 {
		return gnet.Close
	}

	buf, err := conn.Next(protoIOBufLen)
	if err != nil {
		return gnet.Close
	}

	cli.querybuf.Cat(sds.New(buf))
	cli.processInputBuffer()
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
