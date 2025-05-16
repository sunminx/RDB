package networking

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/debug"
)

var assert = debug.Assert

type Server struct {
	gnet.BuiltinEventEngine
	MaxIdleTime         int64
	TcpKeepalive        int
	ProtectedMode       bool
	TcpBacklog          int
	Ip                  string
	Port                int
	ProtoAddr           string
	MaxFd               int
	Clients             []*Client
	cmds                []cmd.Command
	Requirepass         bool
	DB                  *db.DB
	CronLoops           int64
	Hz                  int
	LogLevel            string
	LogPath             string
	Version             string
	RunnableClientCh    chan *Client
	CmdLock             *sync.RWMutex
	UnlockNotice        chan struct{}
	Dumper              dumper
	RdbVersion          int
	RdbFilename         string
	RdbChildType        int
	RdbChildRunning     atomic.Bool
	RdbSaveTimeStart    int64
	RdbSaveTimeUsed     int64
	BackgroundDoneChan  chan uint8
	SaveParams          []SaveParam
	UnixTime            int64
	LastSave            int64
	Dirty               int
	AofFilename         string
	AofDirname          string
	AofLoadTruncated    bool
	AofUseRdbPreamble   bool
	AofRewriteTimeStart int64
	LoadingLoadedBytes  int64
}

type SaveParam struct {
	Seconds int
	Changes int
}

type dumper interface {
	RdbSaveBackground(string, *Server) bool
	RdbSaveBackgroundDoneHandler(*Server)
	AofRewriteBackground(string, *Server) bool
	AofRewriteBackgroundDoneHandler(*Server)
}

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	fd := conn.Fd()
	if s.MaxFd <= fd {
		s.MaxFd = 2 * fd
		oldClients := s.Clients
		s.Clients = initClients(s.MaxFd)
		copy(s.Clients, oldClients)
	}

	cli := NewClient(conn, s.DB)
	cli.Server = s
	cli.cmdLock = s.CmdLock
	cli.lastInteraction = time.Now().UnixMilli()
	s.Clients[fd] = cli
	return nil, gnet.None
}

func (s *Server) OnClose(conn gnet.Conn, err error) (action gnet.Action) {
	fd := conn.Fd()
	if fd > s.MaxFd {
		s.Clients[fd] = nilClient()
	}

	if err != nil {
	}

	return gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) gnet.Action {
	if conn.Fd() > s.MaxFd {
		return gnet.Close
	}

	return s.processTrafficEvent(conn)
}

func (s *Server) OnTick() (time.Duration, gnet.Action) {
	s.cron()
	return time.Duration(1000/s.Hz) * time.Millisecond, gnet.None
}

var protoIOBufLen = 1024 * 16

func (s *Server) processTrafficEvent(conn gnet.Conn) gnet.Action {
	cli := s.Clients[conn.Fd()]
	if cli == nil || cli.fd == -1 {
		return gnet.Close
	}
	cli.lastInteraction = time.Now().UnixMilli()

	if cli.flag&queueCall == 0 {
		if !s.readQuery(cli) {
			// Failed to locked the cmdLock for call command.
			// It has been added to the waiting queue and is waiting to be notified.
			return gnet.None
		}
	} else {
		// Be notified. The requested data has been parsed.
		// Try to execute the command immediately.
		if !cli.call() {
			return gnet.None
		}
	}

	if (cli.flag & closeASAP) != 0 {
		return gnet.Close
	}

	if len(cli.reply) > 0 {
		conn.Write(cli.reply)
		cli.reply = make([]byte, 0)
	}

	if (cli.flag & closeAfterReply) != 0 {
		return gnet.Close
	}
	return gnet.None
}

func (s *Server) readQuery(cli *Client) bool {
	conn := cli.Conn
	buf, err := conn.Next(-1)
	if err != nil {
		return true
	}

	cli.querybuf = append(cli.querybuf, buf...)
	return cli.processInputBuffer()
}

const defMaxFd = 1024

func NewServer() *Server {
	return &Server{
		MaxIdleTime:   0,
		TcpKeepalive:  300,
		ProtectedMode: false,
		TcpBacklog:    512,
		Ip:            "0.0.0.0",
		Port:          6379,
		ProtoAddr:     fmt.Sprintf("tcp://%s:%d", "0.0.0.0", 6379),
		DB:            db.New(),
		MaxFd:         defMaxFd,
		Clients:       initClients(defMaxFd),
		CronLoops:     0,
		Hz:            100,
		LogLevel:      "notice",
		LogPath:       "/dev/null",
		Version:       "0.0.1",
		RdbVersion:    9,
	}
}

func initClients(n int) []*Client {
	clis := make([]*Client, n+1, n+1)
	for i := 0; i < n+1; i++ {
		clis[i] = nilClient()
	}
	return clis
}

func (s *Server) Init() {
	s.cmds = cmd.CommandTable
	s.CmdLock = &sync.RWMutex{}
	s.UnlockNotice = make(chan struct{})
	s.RunnableClientCh = make(chan *Client, 1024)

	s.BackgroundDoneChan = make(chan uint8, 1)

	// Receive the message that the lock of command execution is released.
	// check if there are any clients currently blocking and waiting to execute command,
	// and wake up the client that is blocking and waiting first.
	go func() {
		for {
			select {
			case <-s.UnlockNotice:
				select {
				case rcmd := <-s.RunnableClientCh:
					rcmd.Wake()
				default:
				}
			}
		}
	}()
}

func (s *Server) LookupCommand(name string) (cmd.Command, bool) {
	for _, command := range s.cmds {
		if name == command.Name {
			return command, true
		}
	}
	return cmd.EmptyCommand, false
}

const (
	activeExpireCycleSlowTimePerc = 25
)

const (
	DoneRdbBgsave uint8 = 1
)

func (s *Server) cron() {
	now := time.Now()
	s.UnixTime = now.UnixMilli()

	s.databasesCron()
	s.clientsCron()

	// Check if a background saving or AOF rewrite in progress terminated.
	if s.isBgsaveOrAofRewriteRunning() {
		select {
		case t := <-s.BackgroundDoneChan:
			if t == DoneRdbBgsave {
				s.Dumper.RdbSaveBackgroundDoneHandler(s)
			}
		default:
		}
	} else {
		// If there is not a background saving/rewrite in progress check if
		// we have to save/rewrite now.
		for _, sp := range s.SaveParams {
			if s.Dirty >= sp.Changes &&
				int(s.UnixTime-s.LastSave) > 1e3*sp.Seconds {

				slog.Info(fmt.Sprintf("%d changes in %d seconds. Saving...",
					sp.Changes, sp.Seconds))
				_ = s.Dumper.RdbSaveBackground(s.RdbFilename, s)
				break
			}
		}
	}
}

func (s *Server) databasesCron() {
	// delete expired key
	expireTimeLimit := 1000000 * activeExpireCycleSlowTimePerc / s.Hz / 100
	s.DB.ActiveExpireCycle(time.Duration(expireTimeLimit))
}

func (s *Server) clientsCron() {
	now := time.Now()
	for i := s.MaxFd; i >= 0; i-- {
		cli := s.Clients[i]
		if cli.fd == -1 {
			continue
		}
		if cli.handleTimeout(now.UnixMilli()) {
			continue
		}
	}
}

func (s *Server) delClient(fd int) {
	s.Clients = append(s.Clients[:fd], s.Clients[fd+1:]...)
}

func (s *Server) isBgsaveOrAofRewriteRunning() bool {
	return s.RdbChildRunning.Load()
}
