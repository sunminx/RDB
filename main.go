package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/conf"
	"github.com/sunminx/RDB/internal/networking"
)

func main() {
	var configfile string
	flag.StringVar(&configfile, "conf", "rdb.conf", "--conf rdb.conf")

	server := networking.NewServer()
	conf.Load(server, configfile)
	log.Fatal(gnet.Run(server,
		fmt.Sprintf("tcp://%s:%d", server.Ip, server.Port),
		gnet.WithReusePort(true)))
}
