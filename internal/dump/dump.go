package dump

import (
	"fmt"
	"log/slog"
	"os"
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

func (d Dumper) RdbSaveBackground(server *networking.Server) bool {
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
		slog.Warn("failed opening the RDB file for saving",
			"filename", filename, "err", err)
		return nosave
	}
	defer file.Close()

	rdber, err := newRdbSaver(file, server.DB, newRdberInfo(server))
	if err != nil {
		slog.Warn("cannot create rdber for saving")
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

func AofLoad(server *networking.Server) bool {
	filepath := makePath(server.AofDirname,
		aofManifestFilename(server.AofFilename))
	file, err := os.Open(filepath)
	if err != nil {
		slog.Warn("failed open AOF manifest file",
			"filepath", filepath, "err", err)
		return false
	}
	defer file.Close()

	am, err := createAofManifest(file)
	if err == nil {
		return aofLoadChunkMode(am, server)
	}
	if err != errManifestFileNotFound {
		slog.Warn("failed load aof file", "err", err)
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
	ret := aofOk
	fileNum := am.fileNum()
	currFileIdx := 0
	aof := newAofer(server.DB)
	if am.baseAofInfo != nil {
		currFileIdx++
		filename := am.baseAofInfo.name
		file, err := os.Create(filename)
		if err != nil {
			slog.Warn("failed create temp aof file", "err", err)
		}
		defer file.Close()

		if err := aof.setFile(file, 'r'); err != nil {
			slog.Warn("failed loading base aof file", "err", err)
			ret = aofOpenErr
			goto cleanup
		}
		now := time.Now()
		ret = aof.loadSingleFile(filename, server)
		if ret == aofOk || (ret == aofTruncated && currFileIdx == fileNum) {
			slog.Info("DB loaded from base aof file",
				"filename", filename, "timecost", time.Since(now)/1e9)
		}

		if ret == aofTruncated && currFileIdx < fileNum {
			ret = aofFailed
			slog.Warn("fatal error: the truncated file is not the last file")
		}

		if ret == aofOpenErr || ret == aofFailed {
			goto cleanup
		}
	}

	for _, incrAofInfo := range am.incrAofInfos {
		currFileIdx++
		filename := incrAofInfo.name
		file, err := os.Create(filename)
		if err != nil {
			slog.Warn("failed create temp aof file", "err", err)
		}
		defer file.Close()

		if err := aof.setFile(file, 'r'); err != nil {
			slog.Warn("failed loading incr aof file", "err", err)
			ret = aofOpenErr
			goto cleanup
		}
		now := time.Now()
		ret = aof.loadSingleFile(filename, server)
		if ret == aofOk || (ret == aofTruncated && currFileIdx == fileNum) {
			slog.Info("DB loaded from incr aof file",
				"filename", filename, "timecost", time.Since(now)/1e9)
		}

		if ret == aofTruncated && currFileIdx < fileNum {
			ret = aofFailed
			slog.Warn("fatal error: the truncated file is not the last file")
		}

		if ret == aofOpenErr || ret == aofFailed {
			goto cleanup
		}
	}
cleanup:
	aof.closeFile()
	return ret == aofOk || ret == aofTruncated
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

func (d Dumper) AofRewriteBackground(server *networking.Server) bool {
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
	go aofRewrite(server)
	slog.Info("background saving started")
	server.AofRewriteTimeStart = now.UnixMilli()
	return true
}

func aofRewrite(server *networking.Server) bool {
	tempFilename := fmt.Sprintf("temp-rewriteaof-%d.aof", os.Getpid())
	file, err := os.Create(tempFilename)
	if err != nil {
		slog.Warn("failed create temp aof file", "err", err)
	}
	defer file.Close()

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	if server.AofUseRdbPreamble {
		rdber, err := newRdbSaver(file, server.DB, newRdberInfo(server))
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
	if err := file.Close(); err != nil {
		slog.Warn("failed close rewritten temp aof file", "err", err)
		return false
	}
	file = nil
	if err := os.Rename(tempFilename, server.AofFilename); err != nil {
		slog.Warn("failed rename rewritten aof file", "err", err)
		return false
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

		tempBaseFilename := fmt.Sprintf("temp-rewriteaof-bg-%d.aof", os.Getpid())
		baseFilename := am.nextBaseAofName(server)
		if err := os.Rename(tempBaseFilename,
			makePath(server.AofDirname, baseFilename)); err != nil {
			slog.Warn("failed trying to rename temporary AOF base file", "err", err)
			return
		}

		tempIncrFilename := tempIncrAofName(server.AofFilename)
		incrFilename := am.nextIncrAofName(server)
		if err := os.Rename(
			makePath(server.AofDirname, tempIncrFilename),
			makePath(server.AofDirname, incrFilename)); err != nil {
			slog.Warn("failed trying to rename tempory AOF incr file", "err", err)
			return
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
