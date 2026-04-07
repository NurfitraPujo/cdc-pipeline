package logger

import (
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Init initializes the global logger with the specified level.
// If isDevelopment is true, it uses console-friendly output.
func Init(level string, isDevelopment bool) {
	// 1. Set global time format to UNIX for speed and machine readability
	zerolog.TimeFieldFormat = time.RFC3339

	// 2. Set log level
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	// 3. Configure output
	if isDevelopment {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}

// WatermillLogger is an adapter that implements watermill.LoggerAdapter using zerolog.
type WatermillLogger struct {
	logger zerolog.Logger
}

// NewWatermillLogger creates a new Watermill logger adapter using zerolog.
func NewWatermillLogger() watermill.LoggerAdapter {
	return &WatermillLogger{logger: log.Logger}
}

func (l *WatermillLogger) Error(msg string, err error, fields watermill.LogFields) {
	event := l.logger.Error().Err(err)
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

func (l *WatermillLogger) Info(msg string, fields watermill.LogFields) {
	event := l.logger.Info()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

func (l *WatermillLogger) Debug(msg string, fields watermill.LogFields) {
	event := l.logger.Debug()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

func (l *WatermillLogger) Trace(msg string, fields watermill.LogFields) {
	event := l.logger.Trace()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

func (l *WatermillLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	ctx := l.logger.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return &WatermillLogger{logger: ctx.Logger()}
}
