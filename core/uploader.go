package core

import (
	"bufio"
	"bytes"
	"context"
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
			lastWrite = time.Now()
			if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), nil, writeTimeout); err != nil {
				log.Printf("Failed to write: %s", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if _, err := session.SaveData(context.Background(), "", bytes.NewReader(fileContents), nil, writeTimeout); err != nil {
		log.Printf("Failed to write: %s", err)
	}

	return nil
}
