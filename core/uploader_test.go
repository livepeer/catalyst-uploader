package core

import (
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestItWritesSlowInputIncrementally(t *testing.T) {
	// Create a temporary output file to write to
	outputFile, err := os.CreateTemp(os.TempDir(), "TestItWritesSlowInputIncrementally-*")
	require.NoError(t, err)
	defer os.Remove(outputFile.Name())

	// Set up a sample manifest to incrementally pipe in
	var lines = []string{
		"#EXTM3U",
		"#EXT-X-VERSION:3",
		"#EXTINF:6.006,",
		"index_1_8779957.ts?m=1566416212",
	}

	// Create an input with an artificial delay between each read operation
	slowReader := &SlowReader{
		lines:    lines,
		interval: 300 * time.Millisecond,
	}

	// Kick off the upload in a goroutine so that we can check the file is incrementally written
	go func() {
		u, err := url.Parse(outputFile.Name())
		require.NoError(t, err)
		err = Upload(slowReader, u, 100*time.Millisecond, time.Second)
		require.NoError(t, err, "")
	}()

	// Check that the manifest is being written incrementally - keep quickly polling the file
	// and check that the lines we're expecting are written one by one
	var expectedLines = lines
	var timeout = 3 * time.Second
	var checkInterval = 10 * time.Millisecond
	for elapsedCheckDuration := 0 * time.Second; elapsedCheckDuration < timeout; elapsedCheckDuration += checkInterval {
		if len(expectedLines) == 0 {
			break
		}
		time.Sleep(checkInterval)

		f, err := os.ReadFile(outputFile.Name())
		require.NoError(t, err)

		if strings.HasSuffix(strings.TrimSpace(string(f)), expectedLines[0]) {
			expectedLines = expectedLines[1:]
		}
	}

	require.Equal(t, 0, len(expectedLines), "Expected to have received each manifest line sequentially")
}

// SlowReader outputs the lines of text with a delay before each one
type SlowReader struct {
	lines    []string
	interval time.Duration
}

func (sr *SlowReader) Read(b []byte) (n int, err error) {
	if len(sr.lines) > 0 {
		time.Sleep(sr.interval) // Introduce the delay
		s := sr.lines[0] + "\n"
		n = copy(b, s[0:])
		sr.lines = sr.lines[1:]
		return n, nil
	}

	return 0, io.EOF
}
