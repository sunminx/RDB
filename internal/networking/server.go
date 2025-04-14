package networking

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/db"
)

type Server struct {
	gnet.BuiltinEventEngine
	MaxIdleTime   int
	TcpKeepalive  int
	ProtectedMode bool
	TcpBacklog    int
	Ip            string
	Port          int
	Clients       []*Client
	Requirepass   bool
	DB            *db.DB
}

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action Action) {
	fd := conn.Fd()
	if cap[s.Clients] <= fd {
		oldClients := s.Clients
		s.Clients = make([]*Client, 0, fd*2)
		copy(s.Clients, oldClients)
	}
	s.Clients[fd] = NewClient(conn)
	return gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) gnet.Action {
	buf, _ := conn.Next(-1)
	conn.Write(buf)
	return gnet.None
}

func NewServer() *Server {
	return &Server{
		MaxIdleTime:   0,
		TcpKeepalive:  300,
		ProtectedMode: false,
		TcpBacklog:    512,
		Ip:            "0.0.0.0",
		Port:          6379,
		DB:            db.New(),
	}
}
