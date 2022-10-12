package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/livepeer/catalyst-uploader/core"
	"github.com/livepeer/go-tools/drivers"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func run() int {
	// cmd line args
	timeout := flag.Duration("t", 10*time.Second, "Upload timeout")
	help := flag.Bool("h", false, "Display usage information")
	describe := flag.Bool("j", false, "Describe supported storage services in JSON format and exit")
	verbosity := flag.Int("v", 4, "Log verbosity, from 0 to 6: Panic, Fatal, Error, Warn, Info, Debug, Trace")
	logPath := flag.String("l", "", "Log file path")
	flag.Parse()

	if *help {
		fmt.Fprintf(flag.CommandLine.Output(), `Livepeer cloud storage upload utility. Uploads data from standard input to the specified URI.
Usage:
 	%s <store_uri_with_credentials> args
Example:
	s3://AWS_KEY:AWS_SECRET@eu-west-1/bucket-name/key_part1/key_part2/key_name.ts
Args:
`, os.Args[0])
		flag.PrintDefaults()
		return 1
	}

	// list enabled handlers and exit
	if *describe {
		_, _ = os.Stdout.Write(drivers.DescribeDriversJson())
		return 0
	}

	if flag.NArg() == 0 {
		log.Fatal("Destination URI is not specified. See -h for usage.")
	}

	uri := flag.Arg(0)

	// replace stdout to prevent any lib from writing debug output there
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)

	// configure logging
	log.SetLevel(log.Level(*verbosity))
	// route only fatal errors causing non-zero exit code to stderr to allow the calling app to log efficiently
	var errHook core.FatalToStderrHook
	log.AddHook(&errHook)
	var logOutputs []io.Writer
	if *logPath != "" {
		lumberjackLogger := &lumberjack.Logger{
			// Log file abbsolute path, os agnostic
			Filename:   filepath.ToSlash(*logPath),
			MaxSize:    100, // MB
			MaxBackups: 5,
			MaxAge:     30, // days
		}
		logOutputs = append(logOutputs, lumberjackLogger)
	}
	log.SetOutput(io.MultiWriter(logOutputs...))

	if uri == "" {
		log.Fatal("Object store URI is not specified. See -h for usage.")
	}

	// Always log out the URI we're trying to write to when we error
	logger := log.WithField("uri", uri).WithField("timeout", *timeout)

	storageDriver, err := drivers.ParseOSURL(uri, true)
	// path is passed along with the path when uploading
	session := storageDriver.NewSession("")
	if err != nil {
		logger.WithField("stage", "NewSession").Fatal(err)
	}
	ctx := context.Background()
	resKey, err := session.SaveData(ctx, "", os.Stdin, nil, *timeout)
	if err != nil {
		logger.WithField("stage", "SaveData").Fatal(err)
	}

	// success, write uploaded file details to stdout
	err = json.NewEncoder(stdout).Encode(map[string]string{"uri": resKey})
	if err != nil {
		logger.WithField("stage", "SuccessResponse").Fatal(err)
	}

	return 0
}

func main() {
	os.Exit(run())
}
