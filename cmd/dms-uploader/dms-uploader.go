package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/livepeer/dms-uploader/handlers"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"time"
)

func run() int {
	// cmd line args
	uri := flag.String("path", "", "File upload URI")
	help := flag.Bool("h", false, "Display usage information")
	describe := flag.Bool("j", false, "Describe supported storage services in JSON format and exit")
	verbosity := flag.Int("v", 4, "Log verbosity, from 0 to 6: Panic, Fatal, Error, Warn, Info, Debug, Trace")
	logPath := flag.String("l", "", "Log file path")
	id := flag.String("id", "", "Storage service login or id")
	secret := flag.String("secret", "", "Storage service password or secret")
	param := flag.String("param", "", "Additional storage service argument (e.g. AWS S3 region)")
	flag.Parse()

	// configure logging
	log.SetLevel(log.Level(*verbosity))
	// route only fatal errors causing non-zero exit code to stderr to allow the calling app to log efficiently
	var errHook handlers.FatalToStderrHook
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
		_, _ = os.Stdout.Write(handlers.DescribeHandlersJson())
		return 0
	}

	if *help {
		_, _ = fmt.Fprint(os.Stderr, "Livepeer cloud storage upload utility. Receives data through stdout and uploads it to the specified URI.\nUsage:\n")
		flag.PrintDefaults()
		return 1
	}

	if *uri == "" {
		log.Fatal("Target URI is not specified. See -h for usage.")
	}

	handler, err := handlers.AvailableHandlers.Get(*uri)
	if err != nil {
		log.Fatal(err)
	}

	err = handler.NewSession(*id, *secret, *param)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Hour)
	defer cancelFn()

	resUri, err := handler.Upload(ctx, *uri, os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	// success, write uploaded file details to stdout
	outJson, err := json.Marshal(handlers.ResUri{Uri: resUri})
	_, err = os.Stdout.Write(outJson)
	if err != nil {
		log.Fatal(err)
	}

	return 0
}

func main() {
	os.Exit(run())
}
