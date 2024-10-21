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
	"golang.org/x/sync/errgroup"
)

type ByteCounter struct {
	Count int64
}

func (bc *ByteCounter) Write(p []byte) (n int, err error) {
	bc.Count += int64(len(p))
	return n, nil
}

func newExponentialBackOffExecutor(initial, max, totalMax time.Duration) *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = initial
	backOff.MaxInterval = max
	backOff.MaxElapsedTime = totalMax
	backOff.Reset()
	return backOff
}

func NoRetries() backoff.BackOff {
	return &backoff.StopBackOff{}
}

func UploadRetryBackoff() backoff.BackOff {
	return newExponentialBackOffExecutor(30*time.Second, 4*time.Minute, 15*time.Minute)
}

func SingleRequestRetryBackoff() backoff.BackOff {
	return newExponentialBackOffExecutor(5*time.Second, 10*time.Second, 30*time.Second)
}

var expiryField = map[string]string{
	"Object-Expires": "+168h", // Objects will be deleted after 7 days
}

func Upload(input io.Reader, outputURI *url.URL, waitBetweenWrites, writeTimeout time.Duration, storageFallbackURLs map[string]string, segTimeout time.Duration, disableThumbs []string, thumbsURLReplacement map[string]string) (*drivers.SaveDataOutput, error) {
	ext := filepath.Ext(outputURI.Path)
	inputFile, err := os.CreateTemp("", "upload-*"+ext)
	if err != nil {
		return nil, fmt.Errorf("failed to write to temp file: %w", err)
	}
	inputFileName := inputFile.Name()
	defer os.Remove(inputFileName)

	if ext == ".ts" || ext == ".mp4" {
		// For segments we just write them in one go here and return early.
		// (Otherwise the incremental write logic below caused issues with clipping since it results in partial segments being written.)
		_, err = io.Copy(inputFile, input)
		if err != nil {
			return nil, fmt.Errorf("failed to write to temp file: %w", err)
		}
		if err := inputFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close input file: %w", err)
		}

		out, bytesWritten, err := uploadFileWithBackup(outputURI, inputFileName, nil, segTimeout, true, storageFallbackURLs)
		if err != nil {
			return nil, fmt.Errorf("failed to upload video %s: (%d bytes) %w", outputURI.Redacted(), bytesWritten, err)
		}

		if err = extractThumb(outputURI, inputFileName, storageFallbackURLs, disableThumbs, thumbsURLReplacement); err != nil {
			glog.Errorf("extracting thumbnail failed for %s: %v", outputURI.Redacted(), err)
		}
		return out, nil
	}

	// For the manifest files we want a very short cache ttl as the files are updating every few seconds
	fields := &drivers.FileProperties{CacheControl: "max-age=1"}
	var lastWrite = time.Now()
	// Keep the file handle closed while we wait for input data
	if err := inputFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close input file: %w", err)
	}

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

		inputFile, err = os.OpenFile(inputFileName, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		if _, err := inputFile.Write(b); err != nil {
			return nil, fmt.Errorf("failed to append to input file: %w", err)
		}
		if err := inputFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close input file: %w", err)
		}

		// Only write the latest version of the data that's been piped in if enough time has elapsed since the last write
		if lastWrite.Add(waitBetweenWrites).Before(time.Now()) {
			if _, _, err := uploadFileWithBackup(outputURI, inputFileName, fields, writeTimeout, false, storageFallbackURLs); err != nil {
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
	if _, _, err := uploadFileWithBackup(outputURI, inputFileName, fields, writeTimeout, false, storageFallbackURLs); err != nil {
		// Don't ignore this error, since there won't be any further attempts to write
		return nil, fmt.Errorf("failed to write final save: %w", err)
	}
	glog.Infof("Completed writing %s to storage", outputURI.Redacted())
	return nil, nil
}

func uploadFileWithBackup(outputURI *url.URL, fileName string, fields *drivers.FileProperties, writeTimeout time.Duration, withRetries bool, storageFallbackURLs map[string]string) (out *drivers.SaveDataOutput, bytesWritten int64, err error) {
	retryPolicy := NoRetries()
	if withRetries {
		retryPolicy = UploadRetryBackoff()
	}
	err = backoff.Retry(func() error {
		var primaryErr error
		out, bytesWritten, primaryErr = uploadFile(outputURI, fileName, fields, writeTimeout, withRetries)
		if primaryErr == nil {
			return nil
		}

		backupURI, err := buildBackupURI(outputURI, storageFallbackURLs)
		if err != nil {
			glog.Errorf("failed to build backup URL: %v", err)
			return primaryErr
		}
		glog.Warningf("Primary upload failed, uploading to backupURL=%s primaryErr=%q", backupURI.Redacted(), primaryErr)

		out, bytesWritten, err = uploadFile(backupURI, fileName, fields, writeTimeout, withRetries)
		if err == nil {
			return nil
		}
		return fmt.Errorf("upload file errors: primary: %w; backup: %w", primaryErr, err)
	}, retryPolicy)
	return out, bytesWritten, err
}

func buildBackupURI(outputURI *url.URL, storageFallbackURLs map[string]string) (*url.URL, error) {
	outputURIStr := outputURI.String()
	for primary, backup := range storageFallbackURLs {
		if strings.HasPrefix(outputURIStr, primary) {
			backupStr := strings.Replace(outputURIStr, primary, backup, 1)
			return url.Parse(backupStr)
		}
	}
	return nil, fmt.Errorf("no backup URL found for %s", outputURI.Redacted())
}

func uploadFile(outputURI *url.URL, fileName string, fields *drivers.FileProperties, writeTimeout time.Duration, withRetries bool) (out *drivers.SaveDataOutput, bytesWritten int64, err error) {
	outputStr := outputURI.String()
	// While we wait for storj to implement an easier method for global object deletion we are hacking something
	// here to allow us to have recording objects deleted after 7 days.
	if strings.Contains(outputStr, "gateway.storjshare.io/catalyst-recordings-com") {
		var storjFields drivers.FileProperties
		if fields != nil {
			storjFields = *fields
		}
		storjFields.Metadata = expiryField
		fields = &storjFields
	}

	driver, err := drivers.ParseOSURL(outputStr, true)
	if err != nil {
		return nil, 0, err
	}
	session := driver.NewSession("")

	retryPolicy := NoRetries()
	if withRetries {
		retryPolicy = SingleRequestRetryBackoff()
	}
	err = backoff.Retry(func() error {
		file, err := os.Open(fileName)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		// To count how many bytes we are trying to read then write (upload) to s3 storage
		byteCounter := &ByteCounter{}
		teeReader := io.TeeReader(file, byteCounter)

		out, err = session.SaveData(context.Background(), "", teeReader, fields, writeTimeout)
		bytesWritten = byteCounter.Count

		if err != nil {
			glog.Errorf("failed upload attempt for %s (%d bytes): %v", outputURI.Redacted(), bytesWritten, err)
		}
		return err
	}, retryPolicy)

	return out, bytesWritten, err
}

func extractThumb(outputURI *url.URL, segmentFileName string, storageFallbackURLs map[string]string, disableThumbs []string, thumbsURLReplacement map[string]string) error {
	for _, playbackID := range disableThumbs {
		if strings.Contains(outputURI.Path, playbackID) {
			glog.Infof("Thumbnails disabled for %s", outputURI.Redacted())
			return nil
		}
	}
	for playbackIDs, replacement := range thumbsURLReplacement {
		for _, playbackID := range strings.Split(playbackIDs, " ") {
			if strings.Contains(outputURI.Path, playbackID) {
				outputURIStr := outputURI.String()
				split := strings.Split(replacement, " ")
				if len(split) != 2 {
					break
				}
				original, replaceWith := split[0], split[1]

				newURI, err := url.Parse(strings.Replace(outputURIStr, original, replaceWith, 1))
				if err != nil {
					return fmt.Errorf("failed to parse thumbnail URL: %w", err)
				}
				outputURI = newURI
				glog.Infof("Replaced thumbnail location for %s", outputURI.Redacted())
				break
			}
		}
	}

	tmpDir, err := os.MkdirTemp(os.TempDir(), "thumb-*")
	if err != nil {
		return fmt.Errorf("temp file creation failed: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	outFile := filepath.Join(tmpDir, "out.png")

	args := []string{
		"-i", segmentFileName,
		"-ss", "00:00:00",
		"-vframes", "1",
		"-vf", "scale=640:360:force_original_aspect_ratio=decrease",
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

	// two thumbs, one at session level, the other at stream level
	thumbURLs := []*url.URL{outputURI.JoinPath("../latest.png"), outputURI.JoinPath("../../../latest.png")}
	fields := &drivers.FileProperties{CacheControl: "max-age=5"}
	errGroup := &errgroup.Group{}

	for _, thumbURL := range thumbURLs {
		thumbURL := thumbURL
		errGroup.Go(func() error {
			_, _, err = uploadFileWithBackup(thumbURL, outFile, fields, 10*time.Second, true, storageFallbackURLs)
			if err != nil {
				return fmt.Errorf("saving thumbnail failed: %w", err)
			}
			return nil
		})
	}
	return errGroup.Wait()
}
