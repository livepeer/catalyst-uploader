package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/livepeer/dms-uploader/core"
	"github.com/livepeer/dms-uploader/drivers"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"time"
)

func run() int {
	// cmd line args
	uri := flag.String("uri", "", "Object storage URI with credentials.")
	path := flag.String("path", "", "Destination path")
	help := flag.Bool("h", false, "Display usage information")
	describe := flag.Bool("j", false, "Describe supported storage services in JSON format and exit")
	verbosity := flag.Int("v", 4, "Log verbosity, from 0 to 6: Panic, Fatal, Error, Warn, Info, Debug, Trace")
	logPath := flag.String("l", "", "Log file path")
	flag.Parse()

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

	// list enabled handlers and exit
	if *describe {
		_, _ = os.Stdout.Write(drivers.DescribeDriversJson())
		return 0
	}

	if *help {
		_, _ = fmt.Fprint(os.Stderr, "Livepeer cloud storage upload utility. Receives data through stdout and uploads it to the specified URI.\nUsage:\n")
		flag.PrintDefaults()
		return 1
	}

	if *uri == "" {
		log.Fatal("Object storage URI is not specified. See -h for usage.")
	}

	if *path == "" {
		log.Fatal("Object destination path is not specified. See -h for usage.")
	}

	storageDriver, err := drivers.ParseOSURL(*uri, true)
	// path is passed along with the path when uploading
	session := storageDriver.NewSession("")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	resKey, err := session.SaveData(ctx, *path, os.Stdin, nil, time.Second*30)
	if err != nil {
		log.Fatal(err)
	}

	// success, write uploaded file details to stdout
	outJson, err := json.Marshal(struct {
		Uri string `json:"uri"`
	}{Uri: resKey})
	_, err = stdout.Write(outJson)
	if err != nil {
		log.Fatal(err)
	}

	return 0
}

func main() {
	os.Exit(run())
}
