package main

import (
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-uploader/core"
	"github.com/livepeer/go-tools/drivers"
)

const WaitBetweenWrites = 5 * time.Second

var Version string

func main() {
	os.Exit(run())
}

func run() int {
	err := flag.Set("logtostderr", "true")
	if err != nil {
		glog.Fatal(err)
	}
	// cmd line args
	describe := flag.Bool("j", false, "Describe supported storage services in JSON format and exit")
	timeout := flag.Duration("t", 30*time.Second, "Upload timeout")
	flag.Parse()

	// list enabled handlers and exit
	if *describe {
		_, _ = os.Stdout.Write(drivers.DescribeDriversJson())
		return 0
	}

	if flag.NArg() == 0 {
		glog.Fatal("Destination URI is not specified. See -h for usage.")
		return 1
	}

	// replace stdout to prevent any lib from writing debug output there
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)

	uri := flag.Arg(0)
	if uri == "" {
		glog.Fatalf("Could not parse object store URI: %s", uri)
		return 1
	}

	err = core.Upload(os.Stdin, uri, WaitBetweenWrites, *timeout)
	if err != nil {
		glog.Fatalf("Uploader failed for %s: %s", uri, err)
		return 1
	}

	// success, write uploaded file details to stdout
	err = json.NewEncoder(stdout).Encode(map[string]string{"uri": uri})
	if err != nil {
		glog.Fatal(err)
		return 1
	}

	return 0
}
