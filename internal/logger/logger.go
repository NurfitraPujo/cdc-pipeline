package logger

import (
	"os"
	"time"

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
