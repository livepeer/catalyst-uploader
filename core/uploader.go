package core

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/livepeer/go-tools/drivers"
)

func Upload(input io.Reader, outputURI string, waitBetweenWrites, writeTimeout time.Duration) error {
	storageDriver, err := drivers.ParseOSURL(outputURI, true)
	if err != nil {
		return err
	}
	session := storageDriver.NewSession("")
	if err != nil {
		return err
	}

	// While we wait for storj to implement an easier method for global object deletion we are hacking something
	// here to allow us to have recording objects deleted after 7 days.
	var fields *drivers.FileProperties
	if strings.Contains(outputURI, "gateway.storjshare.io/catalyst-recordings-com") {
		fields = &drivers.FileProperties{
			Metadata: map[string]string{
				"Object-Expires": "+168h", // Objects will be deleted after 7 days
			},
		}
	}

	if strings.HasSuffix(outputURI, ".ts") || strings.HasSuffix(outputURI, ".mp4") {
		// For segments we just write them in one go here and return early.
		// (Otherwise the incremental write logic below caused issues with clipping since it results in partial segments being written.)
		_, err := session.SaveData(context.Background(), "", input, fields, writeTimeout)
		if err != nil {
			return err
		}

		extractThumb(session)
		return nil
	}

	var fileContents = []byte{}
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
		if strings.Contains(outputURI, "m3u8") {
			log.Printf("Received new bytes for %s: %s", outputURI, string(b))
		}

		// Only write the latest version of the data that's been piped in if enough time has elapsed since the last write
		if lastWrite.Add(waitBetweenWrites).Before(time.Now()) {
			if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), fields, writeTimeout); err != nil {
				// Just log this error, since it'll effectively be retried after the next interval
				log.Printf("Failed to write: %s", err)
			} else {
				log.Printf("Wrote %s to storage: %d bytes", outputURI, len(b))
			}
			lastWrite = time.Now()
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// We have to do this final write, otherwise there might be final data that's arrived since the last periodic write
	if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), fields, writeTimeout); err != nil {
		// Don't ignore this error, since there won't be any further attempts to write
		return fmt.Errorf("failed to write final save: %w", err)
	}

	return nil
}

func extractThumb(session drivers.OSSession) {
	presigned, err := session.Presign("", 5*time.Minute)
	if err != nil {
		log.Printf("Presigning failed: %s", err)
		return
	}

	outDir, err := os.MkdirTemp(os.TempDir(), "thumb-*")
	if err != nil {
		log.Printf("Temp file creation failed: %s", err)
		return
	}
	defer os.RemoveAll(outDir)
	outFile := filepath.Join(outDir, "out.jpg")

	args := []string{
		"-i", presigned,
		"-ss", "00:00:00",
		"-vframes", "1",
		"-vf", "scale=320:240:force_original_aspect_ratio=decrease",
		"-y",
		outFile,
	}

	timeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", args...)

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr

	err = cmd.Run()
	if err != nil {
		log.Printf("ffmpeg failed[%s] [%s]: %s", outputBuf.String(), stdErr.String(), err)
		return
	}

	f, err := os.Open(outFile)
	if err != nil {
		log.Printf("Opening file failed: %s", err)
		return
	}
	defer f.Close()
	_, err = session.SaveData(context.Background(), "../latest.jpg", f, &drivers.FileProperties{CacheControl: "max-age=5"}, 10*time.Second)
	if err != nil {
		log.Printf("Saving thumbnail failed: %s", err)
		return
	}
}
