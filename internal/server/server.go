package server

import "github.com/panjf2000/gnet/v2"

type Server struct {
	gnet.BuiltinEventEngine
	ip   string
	port int
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)
	c.Write(buf)
	return gnet.None
}

func New(ip string, port int) *Server {
	return &Server{
		ip:   ip,
		port: port,
	}
}
