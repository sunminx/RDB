package server

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/dict"
)

type Client struct {
	gnet.Conn
	fd int
}

func NewClient(conn gnet.Conn) *Client {
	return &Client{
		Conn: conn,
		fd:   conn.Fd(),
	}
}

func (c *Client) AddReply(val dict.Robj) {

}
