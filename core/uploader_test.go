package core

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
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
		_, err = Upload(slowReader, u, 100*time.Millisecond, time.Second, nil, time.Minute, nil)
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

func TestUploadFileWithBackup(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "TestUploadFileWithBackup-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	testFile := filepath.Join(dir, "input.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("test"), 0644))

	fakeStorage := "s3+https://fake.service.livepeer.com/bucket/"
	backupStorage := filepath.Join(dir, "backup") + "/"
	fakeOutput := fakeStorage + "hls/123/file.txt"
	expectedOutFile := backupStorage + "hls/123/file.txt"

	storageFallbackURLs := map[string]string{
		fakeStorage: "file://" + backupStorage,
	}
	out, written, err := uploadFileWithBackup(mustParseURL(fakeOutput), testFile, nil, 0, false, storageFallbackURLs)
	require.NoError(t, err)
	require.Equal(t, expectedOutFile, out.URL)
	require.Equal(t, int64(4), written)

	b, err := os.ReadFile(expectedOutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), b)
}

func TestBuildBackupURI(t *testing.T) {
	storageFallbackURLs := map[string]string{
		"http://localhost:8888/":                             "http://localhost:8888/os-recordings-backup/",
		"s3+https://user:password@remote.storage.io/bucket/": "s3+https://resu:drowssap@reliable.storage.com/backup-bucket/",
	}
	testCases := []struct {
		name        string
		input       string
		expectedErr string
		expected    string
	}{
		{
			name:     "http",
			input:    "http://localhost:8888/folder/",
			expected: "http://localhost:8888/os-recordings-backup/folder/",
		},
		{
			name:     "s3",
			input:    "s3+https://user:password@remote.storage.io/bucket/",
			expected: "s3+https://resu:drowssap@reliable.storage.com/backup-bucket/",
		},
		{
			name:     "s3 with nested file",
			input:    "s3+https://user:password@remote.storage.io/bucket/folder/file.ts",
			expected: "s3+https://resu:drowssap@reliable.storage.com/backup-bucket/folder/file.ts",
		},
		{
			name:        "no backup URL",
			input:       "s3+https://another.storage.fish/bucket/folder/file.ts",
			expectedErr: "no backup URL",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			uri, err := buildBackupURI(mustParseURL(tc.input), storageFallbackURLs)
			if tc.expectedErr != "" {
				require.ErrorContains(err, tc.expectedErr)
				return
			}
			require.NoError(err)
			require.Equal(tc.expected, uri.String())
		})
	}
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

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}
