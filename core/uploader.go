package core

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/glog"
	"github.com/livepeer/go-tools/drivers"
)

type ByteCounter struct {
	Count int64
}

func (bc *ByteCounter) Write(p []byte) (n int, err error) {
	bc.Count += int64(len(p))
	return n, nil
}

func newExponentialBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 30 * time.Second
	backOff.MaxInterval = 2 * time.Minute
	backOff.MaxElapsedTime = 0 // don't impose a timeout as part of the retries

	return backOff
}

func UploadRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(newExponentialBackOffExecutor(), 4)
}

const segmentWriteTimeout = 5 * time.Minute

var expiryField = map[string]string{
	"Object-Expires": "+168h", // Objects will be deleted after 7 days
}

type StorageBackupURLs []struct {
	Primary string `json:"primary"`
	Backup  string `json:"backup"`
}

func Upload(input io.Reader, outputURI *url.URL, waitBetweenWrites, writeTimeout time.Duration) (*drivers.SaveDataOutput, error) {
	output := outputURI.String()

	// While we wait for storj to implement an easier method for global object deletion we are hacking something
	// here to allow us to have recording objects deleted after 7 days.
	fields := &drivers.FileProperties{}
	if strings.Contains(output, "gateway.storjshare.io/catalyst-recordings-com") {
		fields = &drivers.FileProperties{Metadata: expiryField}
	}

	if strings.HasSuffix(output, ".ts") || strings.HasSuffix(output, ".mp4") {
		// For segments we just write them in one go here and return early.
		// (Otherwise the incremental write logic below caused issues with clipping since it results in partial segments being written.)
		fileContents, err := io.ReadAll(input)
		if err != nil {
			return nil, fmt.Errorf("failed to read file")
		}

		out, bytesWritten, err := uploadFile(outputURI, fileContents, fields, segmentWriteTimeout, true)
		if err != nil {
			return nil, fmt.Errorf("failed to upload video %s: (%d bytes) %w", outputURI.Redacted(), bytesWritten, err)
		}

		if err = extractThumb(outputURI, output, fileContents); err != nil {
			glog.Errorf("extracting thumbnail failed for %s: %v", outputURI.Redacted(), err)
		}
		return out, nil
	}

	// For the manifest files we want a very short cache ttl as the files are updating every few seconds
	fields.CacheControl = "max-age=1"
	var fileContents []byte
	var lastWrite = time.Now()

	scanner := bufio.NewScanner(input)

	// We have to use a custom scanner because the default one is designed for text and will
	// split on and drop newline characters
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// If we have reached the end of the input, return 0 bytes and no error.
		if atEOF {
			return 0, nil, nil
		}

		// Read the entire input as one line by advancing the buffer to its end.
		return len(data), data, nil
	})

	for scanner.Scan() {
		b := scanner.Bytes()
		fileContents = append(fileContents, b...)

		// Only write the latest version of the data that's been piped in if enough time has elapsed since the last write
		if lastWrite.Add(waitBetweenWrites).Before(time.Now()) {
			if _, _, err := uploadFile(outputURI, fileContents, fields, writeTimeout, false); err != nil {
				// Just log this error, since it'll effectively be retried after the next interval
				glog.Errorf("Failed to write: %v", err)
			} else {
				glog.V(5).Infof("Wrote %s to storage: %d bytes", outputURI.Redacted(), len(b))
			}
			lastWrite = time.Now()
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// We have to do this final write, otherwise there might be final data that's arrived since the last periodic write
	if _, _, err := uploadFile(outputURI, fileContents, fields, writeTimeout, false); err != nil {
		// Don't ignore this error, since there won't be any further attempts to write
		return nil, fmt.Errorf("failed to write final save: %w", err)
	}
	glog.Infof("Completed writing %s to storage", outputURI.Redacted())
	return nil, nil
}

func uploadFile(outputURI *url.URL, fileContents []byte, fields *drivers.FileProperties, writeTimeout time.Duration, withRetries bool) (out *drivers.SaveDataOutput, bytesWritten int64, err error) {
	driver, err := drivers.ParseOSURL(outputURI.String(), true)
	if err != nil {
		return nil, 0, err
	}
	session := driver.NewSession("")

	var retryPolicy backoff.BackOff = &backoff.StopBackOff{} // no retries by default
	if withRetries {
		retryPolicy = UploadRetryBackoff()
	}
	err = backoff.Retry(func() error {
		// To count how many bytes we are trying to read then write (upload) to s3 storage
		byteCounter := &ByteCounter{}
		teeReader := io.TeeReader(bytes.NewReader(fileContents), byteCounter)

		out, err = session.SaveData(context.Background(), "", teeReader, fields, writeTimeout)
		bytesWritten = byteCounter.Count

		if err != nil {
			glog.Errorf("failed upload attempt for %s (%d bytes): %v", outputURI.Redacted(), bytesWritten, err)
		}
		return err
	}, retryPolicy)

	return out, bytesWritten, err
}

func extractThumb(outputURI *url.URL, filename string, segment []byte) error {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "thumb-*")
	if err != nil {
		return fmt.Errorf("temp file creation failed: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	outFile := filepath.Join(tmpDir, "out.jpg")
	inFile := filepath.Join(tmpDir, filepath.Base(filename))
	if err = os.WriteFile(inFile, segment, 0644); err != nil {
		return fmt.Errorf("failed to write input file: %w", err)
	}

	args := []string{
		"-i", inFile,
		"-ss", "00:00:00",
		"-vframes", "1",
		"-vf", "scale=854:480:force_original_aspect_ratio=decrease",
		"-y",
		outFile,
	}

	timeout, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", args...)

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("ffmpeg failed[%s] [%s]: %w", outputBuf.String(), stdErr.String(), err)
	}

	f, err := os.Open(outFile)
	if err != nil {
		return fmt.Errorf("opening file failed: %w", err)
	}
	defer f.Close()
	thumbData, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	thumbURL := outputURI.JoinPath("../latest.jpg")
	_, _, err = uploadFile(thumbURL, thumbData, &drivers.FileProperties{
		CacheControl: "max-age=5",
		Metadata:     expiryField,
	}, 10*time.Second, true)
	if err != nil {
		return fmt.Errorf("saving thumbnail failed: %w", err)
	}
	return nil
}
