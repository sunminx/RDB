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

// RDB active child save type.
const (
	rdbChildTypeNone   = 0
	rdbChildTypeDisk   = 1 // rdb is written to disk.
	rdbChildTypeSocket = 2 // rdb is written to slave socket.
)

func RdbSaveBackground(filename string, server *networking.Server) bool {
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
	return saved
}

func newRdberInfo(server *networking.Server) rdberInfo {
	return rdberInfo{
		version: server.RdbVersion,
	}
}
