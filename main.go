package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

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
	//initLog()
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

func initLog() {
	logFile, err := os.OpenFile("rdb.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("can't open log file", "err", err)
		os.Exit(1)
	}
	defer logFile.Close()

	handler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
}
