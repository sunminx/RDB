package networking

import (
	"context"
	"errors"
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
	. "github.com/sunminx/RDB/pkg/util"
)

var assert = debug.Assert

type Server struct {
	gnet.BuiltinEventEngine
	Dumper
	Ctx                    context.Context
	CancelFunc             context.CancelFunc
	CancelCalled           bool
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
	MasterReplOffset       int64
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
	Shutdown               atomic.Bool
	ShutdownTimeout        int64
	ShutdownStartTime      int64

	// status indicates what status the server is in.
	status serverStatus

	// el is the eventLoop which can used to push cron to task queue.
	el gnet.EventLoop

	// runner is a implements of the gnet.Runnable, which contains cron task.
	runner *CronRunner

	// wakeupRunner is used to avoid adding multiple crons to the task queue of eventLoop.
	// Only when the previous cron finishes execution can a new cron be added.
	wakeupRunner atomic.Int32
}

type serverStatus int8

const (
	// running indicates we are in the main-loop.
	running serverStatus = iota + 1

	// shutdown indicates we received the SIGINT or SIGTERM signal.
	shutdown

	// terminated indicates whether the post-processing work after
	// the server is taken shutdown has been completed.
	terminated
)

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
	RdbSave(*Server) bool
	RdbSaveBackground(*Server) bool
	RdbSaveBackgroundDoneHandler(*Server)
	AofLoad(*Server) bool
	AofRewriteBackground(*Server) bool
	AofRewriteBackgroundDoneHandler(*Server)
	AofOpenOnServerStart(*Server)
	FlushAofManifest(*Server) error
}

var rejectConnResp = []byte("connection refused.")

func (s *Server) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	// When the server is ready to shutdown, the new connection will be refused.
	if s.Shutdown.Load() {
		conn.Write(rejectConnResp)
		return nil, gnet.Close
	}

	// Once there is a client connection, we have to get the eventLoop refer and
	// the cron need to be pushed to the task queue of eventLoop.

	// The purpose of pushing the cron to task queue is to execute the cron after
	// the network events is completed in every loop.
	s.initCronRunner(conn.EventLoop())

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

var once sync.Once

// initCronRunner initialize fields of el & runner at once.
func (s *Server) initCronRunner(el gnet.EventLoop) {
	once.Do(func() {
		runner := CronRunner{server: s}
		s.el = el
		s.runner = &runner
	})
}

func (s *Server) OnClose(conn gnet.Conn, err error) (action gnet.Action) {
	fd := conn.Fd()
	if fd < s.MaxFd {
		s.Clients[fd].fd = -1
	}

	if err != nil {
	}

	return gnet.None
}

func (s *Server) OnTraffic(conn gnet.Conn) gnet.Action {
	if conn.Fd() > s.MaxFd {
		return gnet.Close
	}
	action := s.processTrafficEvent(conn)
	return action
}

func (s *Server) OnTick() (time.Duration, gnet.Action) {
	// When at least one client is connected, the cron are registered
	// in the task queue of the EventLoop, and the scheduled tasks
	// are executed after each loop of network event is completed.
	if s.el != nil {
		if s.wakeupRunner.CompareAndSwap(0, 1) {
			s.el.Execute(context.Background(), s.runner)
		}
	} else {
		s.cron()
	}

	if s.status == terminated {
		return time.Second, gnet.Shutdown
	}
	// This interval determines the frequency at which cron is added
	// to the task queue of the EventLoop.
	// Polling network event in gnet is non-blocking.
	return time.Duration(1000/s.Hz) * time.Millisecond, gnet.None
}

func (s *Server) OnShutdown(_ gnet.Engine) {
	s.status = terminated
}

var protoIOBufLen = 1024 * 16

// processTrafficEvent read request from conn and write response to conn.
func (s *Server) processTrafficEvent(conn gnet.Conn) gnet.Action {
	cli := s.Clients[conn.Fd()]
	if cli == nil || cli.fd == -1 {
		return gnet.Close
	}

	cli.state = runnableState
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

	cli.state = idleState

	// If the server is going shutdown, we will write close the connection after output response.
	if s.Shutdown.Load() || (cli.flag&closeAfterReply) != 0 {
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

// NewServer create a new Server instance with default value for fields.
func NewServer() *Server {
	start := time.Now()
	ctx, cancelFunc := context.WithCancel(context.Background())
	n := atomic.Int32{}
	return &Server{
		Ctx:                ctx,
		CancelFunc:         cancelFunc,
		wakeupRunner:       n,
		Daemonize:          false,
		MaxIdleTime:        0,
		TcpKeepalive:       300,
		ProtectedMode:      false,
		TcpBacklog:         512,
		Ip:                 "0.0.0.0",
		Port:               6379,
		ProtoAddr:          fmt.Sprintf("tcp://%s:%d", "0.0.0.0", 6379),
		DB:                 db.New(),
		MaxFd:              defMaxFd,
		Clients:            initClients(defMaxFd),
		CronLoops:          0,
		Hz:                 100,
		LogLevel:           "notice",
		LogPath:            "",
		Version:            "0.0.1",
		RdbVersion:         9,
		LastSave:           start.UnixMilli(),
		RdbFilename:        "dump.rdb",
		AofState:           AofOff,
		AofBuf:             make([]byte, 0, defAofBufCapacity),
		AofLastWriteStatus: aofWriteOk,
	}
}

func initClients(n int) []*Client {
	clients := make([]*Client, n+1, n+1)
	for i := 0; i < n+1; i++ {
		clients[i] = nilClient()
	}
	return clients
}

// Init is used to initialize partial field of server.
func (s *Server) Init() {
	s.cmds = cmd.CommandTable
	s.CmdLock = &sync.RWMutex{}
	s.UnlockNotice = make(chan struct{})
	s.RunnableClientCh = make(chan *Client, 1024)
	s.BackgroundDoneChan = make(chan uint8, 1)
	s.status = running

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

// LoadDataFromDisk rebuild DB by load RDB or AOF file during the server startup.
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

// OpenAofFileIfNeeded open AOF incr file in appendonly mode in order to
// persistent the write command during the running phase.
func (s *Server) OpenAofFileIfNeeded() {
	if s.AofState == AofOn {
		s.AofOpenOnServerStart(s)
	}
}

// LoopupCommand search command by the name. it returns false when
// no command has been found.
func (s *Server) LookupCommand(name string) (cmd.Command, bool) {
	for _, command := range s.cmds {
		if name == command.Name {
			return command, true
		}
	}
	return cmd.EmptyCommand, false
}

// A CronRunner is a implements of gnet.Runnable which used to wrap a task
// that can be pushed to task queue.
type CronRunner struct {
	server *Server
}

// Run will be called after be poped from task queue.
func (c *CronRunner) Run(_ context.Context) error {
	c.server.cron()
	return nil
}

const (
	activeExpireCycleSlowTimePerc = 25
)

const (
	DoneRdbBgsave uint8 = 1
	DoneAofBgsave uint8 = 2
)

// cron is a scheduled task used for processing some work that is conducive to server stability.
func (s *Server) cron() {
	s.UnixTime = time.Now().UnixMilli()

	// Handle background operations on Redis databases.
	s.databasesCron()

	// We need to do a few operations on clients asynchronously.
	s.clientsCron()

	// Shutting down in a safe way when we received SIGTERM or SIGINT.
	if s.Shutdown.Load() && !s.isShutdownInited() {
		slog.Info("the shutdown is started", "startTime", s.UnixTime)
		if s.prepareForShutdown() {
			s.status = terminated
			return
		}
	} else if s.isShutdownInited() {
		if s.UnixTime-s.ShutdownStartTime > s.ShutdownTimeout ||
			s.isReadyShutdown() {
			slog.Info("start do the work before shutdown")
			if s.finishShutdown() {
				slog.Info("the work before shutdown is completed")
				s.status = terminated
				return
			}
		}
	}

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
			base := Cond(s.AofRewriteBaseSize > 0, s.AofRewriteBaseSize, 1)
			growth := (s.AofCurrSize*100)/base - 100
			if growth >= s.AofRewritePerc {
				slog.Info(fmt.Sprintf("starting automatic rewriting of AOF on %d%% growth\n", growth))
				_ = s.AofRewriteBackground(s)
			}

		}
	}

	// After the db persistence is completed, move the key-val pair in sdbs[1] step by step to sdbs[0].
	if TryLockWithTimeout(s.CmdLock, 20*time.Millisecond) {
		_ = s.DB.MergeIfNeeded(100 * time.Millisecond)
		s.CmdLock.Unlock()
	}

	if s.AofState != AofOff {
		s.flushAppendOnlyFile(false)
	}

	if s.el != nil {
		s.wakeupRunner.Store(0)
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

// flushAppendOnlyFile output the aof buffer and flush the kernel buffer to stable storage.
//
// Note that the flushing is not done every time and the rate depends on the AofSync.
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
				slog.Warn("flush aof file failed", "err", err)
				s.AofLastWriteStatus = aofWriteErr
			}
		}
		if s.AofLastWriteStatus == aofWriteErr {
			slog.Info("AOF write error looks solved, Redis can write again")
			s.AofLastWriteStatus = aofWriteOk
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

var errWaitBeforeDoFinishShutdown = errors.New("waiting for replicas before shutting down")

// 1. send GET ACK command to all replicas for get ack offset (repl_ack_off).
// 2. stop Event_loop
func (s *Server) prepareForShutdown() bool {
	s.ShutdownStartTime = s.UnixTime

	for _, cli := range s.Clients {
		if cli.fd != -1 && cli.state == idleState {
			cli.fd = -1
		}
	}
	return false
}

func (s *Server) isShutdownInited() bool {
	return s.ShutdownStartTime != 0
}

// The work that needs to be completed before the server exit.
// 1. Check and Log ack offset of all replicas.
// 2. Kill child process for RDB bgsave if it's exists.
// 3. Kill child process for AOF rewrite if it's exists.
// 4. Flush AOF file if needed.
// 5. Create a new RDB file before exiting.
// 6. Update AOF manifest file.
// 7. Flush all slaves Output buffer.
func (s *Server) finishShutdown() bool {
	if !s.isAllClientFreed() {
		slog.Warn("there are still active clients, so try to finish shutdown later")
		return false
	}

	if !s.CancelCalled && s.RdbChildRunning.Load() {
		s.killPersistingChildRoutine()
		slog.Info("a RDB bgsave child routine had been canceled")
	}

	if !s.CancelCalled && s.AofChildRunning.Load() {
		s.killPersistingChildRoutine()
		slog.Info("a AOF rewrite child routine had been canceled")
	}

	if s.CancelCalled && (s.RdbChildRunning.Load() || s.AofChildRunning.Load()) {
		slog.Info("wait RDB save or AOF rewrite child routine exit")
		return false
	}

	if s.AofState != AofOff {
		slog.Info("flush forcely AOF file")
		s.flushAppendOnlyFile(true)
	}

	if s.SaveParams != nil && len(s.SaveParams) > 0 {
		slog.Info("save a new RDB file")
		// Before reaching this point, there are no client requests and
		// no async persisting.
		if !s.Dumper.RdbSave(s) {
			slog.Error("error save RDB file")
			return false
		}
	}

	if s.AofState != AofOff {
		slog.Info("flush AOF manifest file")
		if err := s.Dumper.FlushAofManifest(s); err != nil {
			slog.Error("error flush AOF manifest file", "err", err)
			return false
		}
	}
	return true
}

func (s *Server) isAllClientFreed() bool {
	for _, cli := range s.Clients {
		if cli.fd != -1 {
			return false
		}
	}
	return true
}

func (s *Server) killPersistingChildRoutine() {
	if s.CancelFunc != nil {
		s.CancelFunc()
		s.CancelFunc = nil
		s.CancelCalled = true
	}
}

func (s *Server) isReadyShutdown() bool {
	return s.isAllClientFreed()
}

func (s *Server) isBgsaveOrAofRewriteRunning() bool {
	return s.RdbChildRunning.Load() || s.AofChildRunning.Load()
}
