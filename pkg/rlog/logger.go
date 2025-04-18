package rlog

import (
	"fmt"
	"log/slog"
)

type Logger struct{}

func New() Logger {
	return Logger{}
}

// Debugf logs messages at DEBUG level.
func (l Logger) Debugf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Debug(msg)
}

// Infof logs messages at INFO level.
func (l Logger) Infof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Info(msg)
}

// Warnf logs messages at WARN level.
func (l Logger) Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Warn(msg)
}

// Errorf logs messages at ERROR level.
func (l Logger) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Error(msg)
}

// Fatalf logs messages at FATAL level.
func (l Logger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.Errorf(msg)
}
