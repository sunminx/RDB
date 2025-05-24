package dump

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/sunminx/RDB/internal/db"
	"github.com/sunminx/RDB/internal/networking"
	"github.com/sunminx/RDB/pkg/util"
)

const (
	// saved indicates that data persistence has been completed.
	saved = true
	// nosave indicates that an unexpect occurred and not saved.
	nosave = false
)

type Dumper struct {
	// waitResetDBState indicates that both persistence and done-handler of persistence
	// have been completed, and it is waiting to reset the persisting state of DB.
	waitResetDBState bool
}

const (
	waiting = true
	notWait = false
)

func New() Dumper {
	return Dumper{waitResetDBState: notWait}
}

// RDB active child save type.
const (
	rdbChildTypeNone   = 0
	rdbChildTypeDisk   = 1 // rdb is written to disk.
	rdbChildTypeSocket = 2 // rdb is written to slave socket.
)

func (_ Dumper) RdbLoad(server *networking.Server) bool {
	filename := server.RdbFilename
	file, err := os.Open(filename)
	if err != nil {
		return false
	}
	defer file.Close()

	rdber, err := newRdbSaver(file, 'r', server.DB, newRdberInfo(server))
	if err != nil {
		slog.Warn("can't create rdber for load", "err", err)
		return false
	}
	if err := rdber.load(); err != nil {
		slog.Warn("failed load RDB file", "err", err)
		return false
	}
	slog.Info("load RDB file success")
	return true
}

func (_ Dumper) RdbSaveBackground(server *networking.Server) bool {
	if !server.RdbChildRunning.CompareAndSwap(
		networking.ChildNotInRunning, networking.ChildInRunning) {
		return nosave
	}
	locked := util.TryLockWithTimeout(server.CmdLock, 100*time.Millisecond)
	if !locked {
		slog.Warn("exit bgsave RDB file because of db can't locked")
		return nosave
	}
	server.DB.SetState(db.InPersistState)
	server.CmdLock.Unlock()
	now := time.Now()
	go rdbSave(server)
	slog.Info("background saving started")
	server.DirtyBeforeBgsave = server.Dirty
	server.RdbSaveTimeStart = now.UnixMilli()
	server.RdbChildType = rdbChildTypeDisk
	return saved
}

type rdberInfo struct {
	version int
}

func rdbSave(server *networking.Server) bool {
	filename := server.RdbFilename
	tempfile := fmt.Sprintf("temp-%d.rdb", os.Getgid())
	file, err := os.Create(tempfile)
	if err != nil {
		slog.Warn("failed opening the RDB file for save",
			"filename", filename, "err", err)
		return nosave
	}
	defer file.Close()

	rdber, err := newRdbSaver(file, 'w', server.DB, newRdberInfo(server))
	if err != nil {
		slog.Warn("can't create rdber for save", "err", err)
		return nosave
	}
	if err = rdber.save(); err != nil {
		slog.Warn("failed save db by rdber", "err", err)
		return nosave
	}

	if err = file.Sync(); err != nil {
		slog.Warn("write error saving DB on disk", "err", err)
		return nosave
	}
	if err = file.Close(); err != nil {
		slog.Warn("write error saving DB on disk", "err", err)
		return nosave
	}

	if err = os.Rename(tempfile, filename); err != nil {
		slog.Warn("error renmae the temp DB file",
			"tempfile", tempfile, "filename", filename, "err", err)
		os.Remove(tempfile)
		return nosave
	}
	slog.Info("DB saved on disk")
	server.BackgroundDoneChan <- networking.DoneRdbBgsave
	return saved
}

func newRdberInfo(server *networking.Server) rdberInfo {
	return rdberInfo{
		version: server.RdbVersion,
	}
}

func (d Dumper) RdbSaveBackgroundDoneHandler(server *networking.Server) {
	if d.waitResetDBState {
		switch server.RdbChildType {
		case rdbChildTypeDisk:
			rdbBgsaveDoneHandlerDisk(server)
		default:
		}
	}
	locked := util.TryLockWithTimeout(server.CmdLock, 100*time.Millisecond)
	if !locked {
		d.waitResetDBState = waiting
		return
	}
	d.waitResetDBState = notWait
	server.DB.SetState(db.InMergeState)
	server.LastSave = time.Now().UnixMilli()
	server.Dirty -= server.DirtyBeforeBgsave
	server.RdbChildRunning.Store(networking.ChildNotInRunning)
	server.CmdLock.Unlock()
}

func rdbBgsaveDoneHandlerDisk(server *networking.Server) {
	now := time.Now()
	server.RdbChildType = rdbChildTypeNone
	server.LastSave = now.UnixMilli()
	server.RdbSaveTimeUsed = server.LastSave - server.RdbSaveTimeStart
	server.RdbSaveTimeStart = -1
}

const (
	rewrited  = true
	noRewrite = false
)

func (_ Dumper) AofLoad(server *networking.Server) bool {
	filename := aofManifestFilename(server.AofFilename)
	filepath := makePath(server.AofDirname, filename)
	file, err := os.Open(filename)
	if err != nil {
		slog.Info("quit AOF load", "filepath", filepath, "reason", err)
		return false
	}
	defer file.Close()

	am, err := createAofManifest(file)
	if err == nil {
		return aofLoadChunkMode(am, server)
	}
	if err != errManifestFileNotFound {
		slog.Warn("failed load AOF file", "err", err)
		return false
	}
	return aofLoadUnChunkMode(server)
}

func loadingAbsProgress(server *networking.Server, pos int64) {
	server.LoadingLoadedBytes += pos
}

// aofLoadChunkMode load aof file stored by chunk mode (since redis 7.x).
func aofLoadChunkMode(am *aofManifest, server *networking.Server) bool {
	if am == nil || am.baseAofInfo == nil || len(am.incrAofInfos) == 0 {
		return false
	}

	var (
		fileNum     = am.fileNum()
		currFileIdx = 0
		baseSize    = int64(0)
		totalSize   = int64(0)
		file        *os.File
		err         error
	)

	aof := newAofer(server.DB)
	aof.fakeCli.Server = server
	if am.baseAofInfo != nil {
		currFileIdx++
		filename := am.baseAofInfo.name
		file, err = os.Open(filename)
		if err != nil {
			slog.Warn("failed create temp AOF file", "err", err)
			return false
		}
		defer file.Close()

		baseSize = getAppendOnlyFileSize(file)
		totalSize += baseSize

		if err := aof.setFile(file, 'r'); err != nil {
			slog.Warn("failed loading base AOF file", "err", err)
			return false
		}
		now := time.Now()
		slog.Info("load AOF base file start")
		ret := aof.loadSingleFile(filename, server)
		if ret == aofOk || (ret == aofTruncated && currFileIdx == fileNum) {
			slog.Info("DB loaded from base AOF file",
				"filename", filename, "timecost", time.Since(now)/1e9)
		} else if ret == aofTruncated && currFileIdx < fileNum {
			slog.Warn("fatal error: the truncated file is not the last file")
			return false
		} else if ret == aofOpenErr || ret == aofFailed {
			slog.Warn("failed load AOF base file")
			return false
		}
	}

	for _, incrAofInfo := range am.incrAofInfos {
		currFileIdx++
		filename := incrAofInfo.name
		file, err = os.Open(filename)
		if err != nil {
			slog.Warn("failed create temp aof file", "err", err)
		}
		defer file.Close()

		totalSize += getAppendOnlyFileSize(file)

		if err := aof.setFile(file, 'r'); err != nil {
			slog.Warn("failed loading incr aof file", "err", err)
			return false
		}
		now := time.Now()
		ret := aof.loadSingleFile(filename, server)
		if ret == aofOk || (ret == aofTruncated && currFileIdx == fileNum) {
			slog.Info("DB loaded from incr aof file",
				"filename", filename, "timecost", time.Since(now)/1e9)
		}

		if ret == aofTruncated && currFileIdx < fileNum {
			ret = aofFailed
			slog.Warn("fatal error: the truncated file is not the last file")
		}

		if ret == aofOpenErr || ret == aofFailed {
			return false
		}
	}

	server.AofCurrSize = totalSize
	server.AofRewriteBaseSize = baseSize
	return true
}

// aofLoadUnChunkMode load aof file stored by aof-use-rdb-preamble or only aof.
func aofLoadUnChunkMode(server *networking.Server) bool {
	aof := newAofer(server.DB)
	filename := server.AofFilename
	file, err := os.Create(filename)
	if err != nil {
		slog.Warn("failed create temp aof file", "err", err)
	}
	defer file.Close()

	if err := aof.setFile(file, 'r'); err != nil {
		slog.Warn("can't open aof file", "err", err)
		return false
	}
	ret := aof.loadSingleFile(filename, server)
	return ret == aofOk || ret == aofTruncated
}

func getAppendOnlyFileSize(file *os.File) int64 {
	fileInfo, _ := file.Stat()
	return fileInfo.Size()
}

func (_ Dumper) AofRewriteBackground(server *networking.Server) bool {
	if !server.AofChildRunning.CompareAndSwap(
		networking.ChildNotInRunning, networking.ChildInRunning) {
		return nosave
	}
	locked := util.TryLockWithTimeout(server.CmdLock, 100*time.Millisecond)
	if !locked {
		slog.Warn("exit rewrite AOF file because of db can't locked")
		return false
	}
	server.DB.SetState(db.InPersistState)
	server.CmdLock.Unlock()
	now := time.Now()
	go aofRewrite("", server)
	slog.Info("background saving started")
	server.AofRewriteTimeStart = now.UnixMilli()
	return true
}

func aofRewrite(filepath string, server *networking.Server) bool {
	tempFilename := fmt.Sprintf("temp-rewriteaof-%d.aof", os.Getpid())
	tempFilepath := makePath(server.AofDirname, tempFilename)
	file, err := os.Create(tempFilepath)
	if err != nil {
		slog.Warn("failed create temp AOF file", "err", err)
	}
	defer file.Close()

	if server.AofUseRdbPreamble {
		rdber, err := newRdbSaver(file, 'w', server.DB, newRdberInfo(server))
		if err != nil {
			slog.Warn("cannot create rdber for saving")
			return false
		}
		if err = rdber.save(); err != nil {
			slog.Warn("failed save db by rdber", "err", err)
			return false
		}
	} else {
		aofer := newAofer(server.DB)
		if err := aofer.setFile(file, 'w'); err != nil {
			slog.Warn("failed init aofer", "err", err)
			return false
		}
		if err := aofer.rewrite(0); err != nil {
			slog.Warn("failed rewrite aof", "err", err)
			return false
		}
	}

	if err := file.Sync(); err != nil {
		slog.Warn("failed sync rewritten temp aof file", "err", err)
		return false
	}
	if filepath != "" {
		if err = os.Rename(tempFilepath, filepath); err != nil {
			slog.Warn("failed rename temp file", "filepath", filepath, "err", err)
			return false
		}
	}
	server.BackgroundDoneChan <- networking.DoneAofBgsave
	return true
}

func (d Dumper) AofRewriteBackgroundDoneHandler(server *networking.Server) {
	if !d.waitResetDBState {
		filepath := makePath(
			server.AofDirname,
			aofManifestFilename(server.AofFilename),
		)
		file, err := os.Open(filepath)
		if err != nil {
			slog.Warn("failed open AOF manifest file",
				"filepath", filepath, "err", err)
		}
		defer file.Close()

		am, err := createAofManifest(file)
		if err != nil {
			slog.Warn("failed create aofManifest instance", "err", err)
			return
		}

		tempBaseFilename := fmt.Sprintf("temp-rewriteaof-%d.aof", os.Getpid())
		baseFilename := am.nextBaseAofName(server)
		baseFilepath := makePath(server.AofDirname, baseFilename)
		if err := os.Rename(tempBaseFilename, baseFilepath); err != nil {
			slog.Warn("failed trying to rename temporary AOF base file", "err", err)
			return
		}

		baseFile, err := os.Open(baseFilepath)
		if err != nil {
			slog.Warn("can't open AOF base file for refresh rewrite_base_size", "err", err)
		} else {
			baseFileInfo, err := baseFile.Stat()
			if err != nil {
				slog.Warn("can't call to stat function on AOF base file for refresh rewrite_base_size",
					"err", err)
			} else {
				server.AofRewriteBaseSize = baseFileInfo.Size()
			}
			baseFile.Close()
		}

		if server.AofState == networking.AofWaitRewrite {
			tempIncrFilename := tempIncrAofName(server.AofFilename)
			incrFilename := am.nextIncrAofName(server)
			if err := os.Rename(
				makePath(server.AofDirname, tempIncrFilename),
				makePath(server.AofDirname, incrFilename)); err != nil {
				slog.Warn("failed trying to rename tempory AOF incr file", "err", err)
				return
			}

		}

		am.moveIncrAofToHist()

		if err := am.persist(server); err != nil {
			slog.Warn("failed persist new AOF manifest file", "err", err)
			return
		}

		am.deleteAofHistFiles(server)
	}

	locked := util.TryLockWithTimeout(server.CmdLock, 100*time.Millisecond)
	if !locked {
		d.waitResetDBState = waiting
		return
	}
	d.waitResetDBState = notWait
	server.DB.SetState(db.InMergeState)
	server.AofChildRunning.Store(networking.ChildNotInRunning)
	slog.Info("Background AOF rewrite signal handler done")
	server.CmdLock.Unlock()
}

func (_ Dumper) AofOpenOnServerStart(server *networking.Server) {
	var am *aofManifest
	amFilepath := makePath(server.AofDirname, aofManifestFilename(server.AofFilename))
	amFile, err := os.Open(amFilepath)
	if err != nil {
		if strings.Index(err.Error(), "no such file or directory") != -1 {
			am = newAofManifest()
		} else {
			slog.Error("can't create AOF manifest file on server start", "err", err)
			os.Exit(1)
		}
	}
	defer amFile.Close()

	if am == nil {
		am, err = createAofManifest(amFile)
		if err != nil {
			slog.Error("failed open AOF manifest file on server start", "err", err)
		}
	}

	if am.baseAofInfo == nil {
		aofBaseFilename := am.nextBaseAofName(server)
		aofBaseFilepath := makePath(server.AofDirname, aofBaseFilename)
		if !aofRewrite(aofBaseFilepath, server) {
			slog.Error("failed rewrite AOF file when database is empty")
			os.Exit(1)
		}
	}

	var aofIncrFilename string
	if am.currIncrFileSeq != 0 {
		idx := am.currIncrFileSeq - 1
		aofIncrFilename = am.incrAofInfos[idx].name
	} else {
		aofIncrFilename = am.nextIncrAofName(server)
	}
	aofIncrFilepath := makePath(server.AofDirname, aofIncrFilename)
	aofIncrFile, err := os.OpenFile(aofIncrFilepath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("can't open Aof incr file on server start", "err", err)
		os.Exit(1)
	}
	server.AofFile = aofIncrFile

	if err := am.persist(server); err != nil {
		slog.Error("can't write AOF manifest file on server start", "err", err)
		os.Exit(1)
	}
}
