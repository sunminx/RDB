package common

import (
	"log/slog"

	"github.com/panjf2000/gnet/v2/pkg/logging"
)

func ToGnetLevel(level string) logging.Level {
	switch level {
	case "debug":
		return logging.DebugLevel
	case "verbose":
		return logging.InfoLevel
	case "notice":
		return logging.WarnLevel
	case "warning":
		return logging.ErrorLevel
	default:
	}
	return logging.WarnLevel
}

func ToSlogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "verbose":
		return slog.LevelInfo
	case "notice":
		return slog.LevelWarn
	case "warning":
		return slog.LevelError
	default:
	}
	return slog.LevelWarn
}
