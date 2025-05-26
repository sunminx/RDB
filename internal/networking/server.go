package networking

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/sunminx/RDB/internal/cmd"
	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/debug"
	"github.com/sunminx/RDB/pkg/util"
)

var assert = debug.Assert

type Server struct {
	gnet.BuiltinEventEngine
	Dumper
	Daemonize              bool
	MaxIdleTime            int64
	TcpKeepalive           int
	ProtectedMode          bool
	TcpBacklog             int
	Ip                     string
	Port                   int
	ProtoAddr              string
	MaxFd                  int
	Clients                []*Client
	cmds                   []cmd.Command
	Requirepass            bool
	DB                     *db.DB
	CronLoops              int64
	Hz                     int
	LogLevel               string
	LogPath                string
	Version                string
	RunnableClientCh       chan *Client
	CmdLock                *sync.RWMutex
	UnlockNotice           chan struct{}
	RdbVersion             int
	RdbFilename            string
	RdbChildType           int
	RdbChildRunning        atomic.Bool
	RdbSaveTimeStart       int64
	RdbSaveTimeUsed        int64
	BackgroundDoneChan     chan uint8
	SaveParams             []SaveParam
	UnixTime               int64
	LastSave               int64
	Dirty                  int
	DirtyBeforeBgsave      int
	AofFile                *os.File
	AofBuf                 []byte
	AofLastWriteStatus     bool
	AofFsync               int
	AofFsyncInProgress     atomic.Bool
	AofFsyncPostponedStart int64
	AofChildRunning        atomic.Bool
	AofFilename            string
	AofDirname             string
	AofLoadTruncated       bool
	AofUseRdbPreamble      bool
	AofRewriteTimeStart    int64
	AofState               uint8
	AofRewriteBaseSize     int64
	AofCurrSize            int64
	AofRewriteMinSize      int64
	AofRewritePerc         int64
	AofLastFsync           int64
	AofLastIncrFsyncOffset int64
	AofLastIncrSize        int64
	LoadingLoadedBytes     int64
	Shutdown               bool
}

const (
	AofOff         = 0
	AofOn          = 1
	AofWaitRewrite = 2
)

const (
	ChildInRunning    = true
	ChildNotInRunning = false
)

type SaveParam struct {
	Seconds int
	Changes int
}

type Dumper interface {
	RdbLoad(*Server) bool
	RdbSaveBackground(*Server) bool
	RdbSaveBackgroundDoneHandler(*Server)
	AofLoad(*Server) bool
	AofRewriteBackground(*Server) bool
	AofRewriteBackgroundDoneHandler(*Server)
	AofOpenOnServerStart(*Server)
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

// The default capacity of the aof output buffer.
const defAofBufCapacity = 1024 * 1024 * 10

func NewServer() *Server {
	start := time.Now()
	return &Server{
		Daemonize:     false,
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
		LogPath:       "",
		Version:       "0.0.1",
		RdbVersion:    9,
		LastSave:      start.UnixMilli(),
		RdbFilename:   "dump.rdb",
		AofState:      AofOff,
		AofBuf:        make([]byte, 0, defAofBufCapacity),
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

func (s *Server) LoadDataFromDisk() {
	start := time.Now()
	if s.AofState == AofOn {
		if s.AofLoad(s) {
			slog.Info("AOF loaded from disk", "timecost(s)", time.Since(start).Milliseconds())
		}
	} else {
		if s.RdbLoad(s) {
			slog.Info("DB loaded from disk", "timecost(s)", time.Since(start).Milliseconds())
		}
	}
}

func (s *Server) OpenAofFileIfNeeded() {
	if s.AofState == AofOn {
		s.AofOpenOnServerStart(s)
	}
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
	DoneAofBgsave uint8 = 2
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
				s.RdbSaveBackgroundDoneHandler(s)
			} else if t == DoneAofBgsave {
				s.AofRewriteBackgroundDoneHandler(s)
			}
		default:
		}
	} else {
		// If there is not a background saving/rewrite in progress check if
		// we have to save/rewrite now.
		for _, sp := range s.SaveParams {
			if s.Dirty >= sp.Changes &&
				int(s.UnixTime-s.LastSave) > 1000*sp.Seconds &&
				s.DB.InNormalState() {
				// We reached the given amount of changes.
				slog.Info(fmt.Sprintf("%d changes in %d seconds. Saving...\n",
					sp.Changes, sp.Seconds))
				_ = s.RdbSaveBackground(s)
				break
			}
		}

		if !s.isBgsaveOrAofRewriteRunning() && s.DB.InNormalState() &&
			s.AofState == AofOn &&
			s.AofRewritePerc > 0 && s.AofCurrSize > s.AofRewriteMinSize {
			// Calculate whether the growth rate of the current AOF file size
			// after the last rewrite exceeds the threshold.
			base := util.Cond(s.AofRewriteBaseSize > 0, s.AofRewriteBaseSize, 1)
			growth := (s.AofCurrSize*100)/base - 100
			if growth >= s.AofRewritePerc {
				slog.Info(fmt.Sprintf("starting automatic rewriting of AOF on %d%% growth\n", growth))
				_ = s.AofRewriteBackground(s)
			}

		}
	}

	// After the db persistence is completed, move the key-val pair in sdbs[1] step by step to sdbs[0].
	if util.TryLockWithTimeout(s.CmdLock, 20*time.Millisecond) {
		_ = s.DB.MergeIfNeeded(100 * time.Millisecond)
		s.CmdLock.Unlock()
	}

	if s.AofState != AofOff {
		s.flushAppendOnlyFile(false)
	}

}

const (
	aofFsyncNo     = 0
	aofFsyncSec    = 1
	aofFsyncAlways = 2
)

const (
	aofWriteErr = false
	aofWriteOk  = true
)

func (s *Server) flushAppendOnlyFile(force bool) {
	if len(s.AofBuf) > 0 {
		n, err := s.AofFile.Write(s.AofBuf)
		if err != nil {
			if err != syscall.EINTR {
				if s.AofFsync == aofFsyncAlways {
					slog.Error("can't recover from AOF write error" +
						"when the AOF fsync policy is 'always'. Exiting...")
					os.Exit(1)
				}
				s.AofLastWriteStatus = aofWriteErr
			}
		} else {
			if s.AofLastWriteStatus == aofWriteErr {
				slog.Info("AOF write error looks solved, Redis can write again")
				s.AofLastWriteStatus = aofWriteOk
			}
		}
		s.AofCurrSize += int64(n)
		s.AofLastIncrSize += int64(n)
		s.AofBuf = s.AofBuf[n:]
	}

	if s.AofFsync == aofFsyncNo {
		return
	}
	if s.AofFsync == aofFsyncSec && !force {
		if s.AofFsyncInProgress.Load() {
			if s.AofFsyncPostponedStart == 0 {
				s.AofFsyncPostponedStart = s.UnixTime
				return
			} else if s.UnixTime-s.AofRewriteTimeStart < 2*int64(time.Second) {
				return
			}
		}
	}

	s.AofFsyncPostponedStart = 0

	if s.AofFsync == aofFsyncAlways {
		s.AofFile.Sync()
	} else if s.AofFsync == aofFsyncSec && s.UnixTime-s.AofLastFsync > int64(time.Second) {
		go func(fsyncFlag atomic.Bool) {
			fsyncFlag.Store(true)
			defer fsyncFlag.Store(false)
			s.AofFile.Sync()

		}(s.AofFsyncInProgress)
	}
	s.AofLastFsync = s.UnixTime
	s.AofLastIncrFsyncOffset = s.AofLastIncrSize
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

func (s *Server) doAfterShutdown() {
	os.Exit(0)
}

func (s *Server) isBgsaveOrAofRewriteRunning() bool {
	return s.RdbChildRunning.Load() || s.AofChildRunning.Load()
}
