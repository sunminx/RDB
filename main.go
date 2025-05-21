package main

import (
	"flag"
	"log"
	"log/slog"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/common"
	"github.com/sunminx/RDB/internal/conf"
	"github.com/sunminx/RDB/internal/dump"
	"github.com/sunminx/RDB/internal/networking"
	"github.com/sunminx/RDB/pkg/rlog"
)

func main() {
	var configfile string
	flag.StringVar(&configfile, "conf", "rdb.conf", "--conf rdb.conf")

	server := networking.NewServer()
	server.Init()
	server.Dumper = dump.New()
	conf.Load(server, configfile)
	//slog.SetLogLoggerLevel(common.ToSlogLevel(server.LogLevel))

	slog.Info(common.Logo(server.Version))
	server.LoadDataFromDisk()

	var opts = []gnet.Option{
		gnet.WithReusePort(true),
		gnet.WithTicker(true),
		gnet.WithLogger(rlog.New()),
		gnet.WithLogLevel(common.ToGnetLevel(server.LogLevel)),
		gnet.WithLogPath(server.LogPath),
	}
	log.Fatal(gnet.Run(server, server.ProtoAddr, opts...))
}
