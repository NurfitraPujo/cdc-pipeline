package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests cover the log-truncation guard only. They touch no containers, so
// they are safe to run outside the usual e2e batches (`go test -run TestLog ./internal/test/e2e/`).

func TestTruncateLogFileEmptiesPreviousRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	require.NoError(t, os.WriteFile(path, []byte("stale output from the last run\n"), 0600))

	truncateLogFile(path)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Size(), "previous run's log should be emptied, not appended to")
}

// The file is absent on a clean checkout; truncation must not create it or
// panic -- logger.Init is what creates it, with the right permissions.
func TestTruncateLogFileMissingIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")

	truncateLogFile(path)

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "truncation should not create the log file")
}

func TestHumanBytes(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{2048, "2.0 KB"},
		{12127651152, "11.3 GB"},
	} {
		require.Equal(t, tc.want, humanBytes(tc.in))
	}
}
