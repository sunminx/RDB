package dump

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sunminx/RDB/internal/networking"
)

const (
	// saved indicates that data persistence has been completed.
	saved = true
	// nosave indicates that an unexpect occurred and not saved.
	nosave = false
)

type Dumper struct{}

func New() Dumper {
	return Dumper{}
}

// RDB active child save type.
const (
	rdbChildTypeNone   = 0
	rdbChildTypeDisk   = 1 // rdb is written to disk.
	rdbChildTypeSocket = 2 // rdb is written to slave socket.
)

func (d Dumper) RdbSaveBackground(filename string, server *networking.Server) bool {
	if !server.RdbChildRunning.CompareAndSwap(false, true) {
		return nosave
	}

	now := time.Now()
	go rdbSave(filename, server)
	slog.Info("background saving started")
	server.RdbSaveTimeStart = now.UnixMilli()
	server.RdbChildType = rdbChildTypeDisk
	return saved
}

type rdberInfo struct {
	version int
}

func rdbSave(filename string, server *networking.Server) bool {
	tempfile := fmt.Sprintf("temp-%d.rdb", os.Getgid())
	file, err := os.Create(tempfile)
	if err != nil {
		slog.Warn("failed opening the RDB file for saving",
			"filename", filename, "err", err)
		return nosave
	}

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
		slog.Warn("error moving temp DB file on the final",
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
	switch server.RdbChildType {
	case rdbChildTypeDisk:
		rdbBgsaveDoneHandlerDisk(server)
	default:
	}
}

func rdbBgsaveDoneHandlerDisk(server *networking.Server) {
	now := time.Now()
	server.RdbChildType = rdbChildTypeNone
	server.LastSave = now.UnixMilli()
	server.RdbSaveTimeUsed = server.LastSave - server.RdbSaveTimeStart
	server.RdbSaveTimeStart = -1
	server.RdbChildRunning.CompareAndSwap(true, false)
}

const (
	rewrited  = true
	noRewrite = false
)

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
		if err := aof.setFile(filename, 'r'); err != nil {
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
		if err := aof.setFile(filename, 'r'); err != nil {
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

// aofLoadNonChunkMode load aof file stored by aof-use-rdb-preamble or only aof.
func aofLoadNonChunkMode(server *networking.Server) bool {
	aof := newAofer(server.DB)
	filename := server.AofFilename
	if err := aof.setFile(filename, 'r'); err != nil {
		slog.Warn("can't open aof file", "err", err)
		return false
	}
	ret := aof.loadSingleFile(filename, server)
	aof.closeFile()
	return ret == aofOk || ret == aofTruncated
}
