package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"syscall"

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
	conf.Load(server, configfile)

	if server.Daemonize {
		subprocess := flag.Bool("subprocess", false, "flag subprocess")
		flag.Parse()
		// Determine whether it is a parent process or a child process
		// through the subprocess startup flag.
		if *subprocess == false {
			if server.LogPath == "" {
				slog.Info("the logfile configuration is necessary in daemonize mode")
				os.Exit(1)
			}
			daemonize()
		}
	}

	server.Init()
	server.Dumper = dump.New()

	var logFile *os.File
	if server.LogPath != "" {
		logFile = initLog(server.LogPath, server.LogLevel)
		defer logFile.Close()
	} else {
		slog.Info(common.Logo(server.Version))
	}

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

func daemonize() {
	name, err := os.Executable()
	if err != nil {
		slog.Info("can't find executable filepath", "err", err)
		os.Exit(1)
	}
	args := os.Args[1:]
	args = append(args, "-subprocess")
	cmd := exec.Command(name, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err = cmd.Start(); err != nil {
		slog.Error("cant't starts the specified command", "err", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func initLog(logPath, level string) *os.File {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("can't open log file", "err", err)
		os.Exit(1)
	}

	handler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level: common.ToSlogLevel(level),
	})
	slog.SetDefault(slog.New(handler))
	return logFile
}
