package main

import (
	"fmt"
	"log"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/server"
)

func main() {
	ip := "127.0.0.1"
	port := 6379
	server := server.New(ip, port)
	log.Fatal(gnet.Run(server, fmt.Sprintf("tcp://%s:%d", ip, port), gnet.WithReusePort(true)))
}
