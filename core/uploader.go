package core

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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

		// Only write the latest version of the data that's been piped in if enough time has elapsed since the last write
		if lastWrite.Add(waitBetweenWrites).Before(time.Now()) {
			if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), nil, writeTimeout); err != nil {
				// Just log this error, since it'll effectively be retried after the next interval
				log.Printf("Failed to write: %s", err)
			}
			lastWrite = time.Now()
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// We have to do this final write, otherwise there might be final data that's arrived since the last periodic write
	if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), nil, writeTimeout); err != nil {
		// Don't ignore this error, since there won't be any further attempts to write
		return fmt.Errorf("failed to write final save: %w", err)
	}

	return nil
}
