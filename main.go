package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/conf"
	"github.com/sunminx/RDB/internal/server"
)

func main() {
	var confPath string
	flag.StringVar(&confPath, "conf", "rdb.conf", "--conf rdb.conf")

	server := new(server.Server)
	if err := conf.Load(server, confPath); err != nil {
		fmt.Errorf("load configfile: %w", err)
	}
	log.Fatal(gnet.Run(server, fmt.Sprintf("tcp://%s:%d", server.Ip, server.Port), gnet.WithReusePort(true)))
}
