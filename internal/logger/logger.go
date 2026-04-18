package logger

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Init(level string, isDevelopment bool) {
	zerolog.TimeFieldFormat = time.RFC3339

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	env := os.Getenv("ENV")
	doubleLog := env != "production" && env != "staging"

	if isDevelopment {
		if doubleLog {
			logFile := filepath.Join("logs", "app.log")
			if err := os.MkdirAll("logs", 0755); err != nil {
				log.Warn().Err(err).Msg("Failed to create logs directory, file logging disabled")
				log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})
			} else {
				file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					log.Warn().Err(err).Msg("Failed to open log file, file logging disabled")
					log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})
				} else {
					consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"}
					multi := io.MultiWriter(consoleWriter, file)
					log.Logger = zerolog.New(multi).With().Timestamp().Logger()
				}
			}
		} else {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})
		}
	} else {
		if doubleLog {
			logFile := filepath.Join("logs", "app.log")
			if err := os.MkdirAll("logs", 0755); err != nil {
				log.Warn().Err(err).Msg("Failed to create logs directory, file logging disabled")
				log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
			} else {
				file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					log.Warn().Err(err).Msg("Failed to open log file, file logging disabled")
					log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
				} else {
					multi := io.MultiWriter(os.Stderr, file)
					log.Logger = zerolog.New(multi).With().Timestamp().Logger()
				}
			}
		} else {
			log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
		}
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
