package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/url"
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

	output := flag.Arg(0)
	if output == "" {
		glog.Fatal("Object store URI was empty")
		return 1
	}

	uri, err := url.Parse(output)
	if err != nil {
		glog.Fatalf("Failed to parse URI: %s", err)
		return 1
	}

	out, err := core.Upload(os.Stdin, uri, WaitBetweenWrites, *timeout)
	if err != nil {
		glog.Fatalf("Uploader failed for %s: %s", uri.Redacted(), err)
		return 1
	}

	var respHeaders http.Header
	if out != nil {
		respHeaders = out.UploaderResponseHeaders
	}
	glog.Infof("Uploader succeeded for %s. storageRequestID=%s Etag=%s", uri.Redacted(), respHeaders.Get("X-Amz-Request-Id"), respHeaders.Get("Etag"))
	// success, write uploaded file details to stdout
	if glog.V(5) {
		err = json.NewEncoder(stdout).Encode(map[string]string{"uri": uri.Redacted()})
		if err != nil {
			glog.Fatal(err)
			return 1
		}
	}

	return 0
}
