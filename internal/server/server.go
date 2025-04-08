package server

import "github.com/panjf2000/gnet/v2"

type Server struct {
	gnet.BuiltinEventEngine
	Ip   string
	Port int
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)
	c.Write(buf)
	return gnet.None
}
