package main

import (
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
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

	subprocess := flag.Bool("subprocess", false, "flag subprocess")
	flag.Parse()
	// Determine whether it is a parent process or a child process
	// through the subprocess startup flag.
	if server.Daemonize && *subprocess == false {
		if server.LogPath == "" {
			slog.Info("the logfile configuration is necessary in daemonize mode")
			os.Exit(1)
		}
		daemonize()
	}

	var logFile *os.File
	if *subprocess && server.LogPath != "" {
		var err error
		logFile, err = initLog(server.LogPath, server.LogLevel)
		if err != nil {
			panic("can't init logger, err=" + err.Error())
		}
		defer logFile.Close()
	} else {
		slog.Info(common.Logo(server.Version))
	}

	server.Init()
	server.Dumper = dump.New()

	registerSignalHandler(server)

	server.LoadDataFromDisk()
	server.OpenAofFileIfNeeded()

	opts := []gnet.Option{
		gnet.WithReusePort(true),
		gnet.WithTicker(true),
		gnet.WithLogger(rlog.New()),
		gnet.WithLogLevel(common.ToGnetLevel(server.LogLevel)),
		gnet.WithLogPath(server.LogPath),
	}
	_ = gnet.Run(server, server.ProtoAddr, opts...)
	slog.Info("Bye bye ...")
}

func registerSignalHandler(server *networking.Server) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalCh
		slog.Info("we received SIGINT or SIGTERM, and exit after finish tail-in work")
		server.Shutdown.Store(true)
	}()
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

func initLog(path, level string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	handler := slog.NewTextHandler(file, &slog.HandlerOptions{
		Level: common.ToSlogLevel(level),
	})
	slog.SetDefault(slog.New(handler))
	return file, nil
}
