package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-uploader/core"
	"github.com/livepeer/go-tools/drivers"
	"github.com/peterbourgon/ff"
)

const WaitBetweenWrites = 5 * time.Second

var Version string

func main() {
	os.Exit(run())
}

func run() int {
	err := flag.Set("logtostderr", "true")
	if err != nil {
		glog.Error(err)
		return 1
	}
	fs := flag.NewFlagSet("catalyst-uploader", flag.ExitOnError)

	// cmd line args
	version := fs.Bool("version", false, "print application version")
	describe := fs.Bool("j", false, "Describe supported storage services in JSON format and exit")
	timeout := fs.Duration("t", 30*time.Second, "Upload timeout")
	storageBackupURLs := jsonFlag[core.StorageBackupURLs](fs, "storage-backup-urls", `JSON array of {"primary":X,"backup":Y} objects with base storage URLs. If a file fails uploading to one of the primary storages (detected by prefix), it will fallback to the corresponding backup URL after having the prefix replaced`)

	_ = fs.String("config", "", "config file (optional)")

	err = ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("CATALYST_UPLOADER"),
	)
	if err != nil {
		glog.Fatalf("error parsing cli: %s", err)
	}

	err = flag.CommandLine.Parse(nil)
	if err != nil {
		glog.Fatal(err)
	}

	if *version {
		fmt.Printf("catalyst-uploader version: %s", Version)
		return 0
	}

	// list enabled handlers and exit
	if *describe {
		_, _ = os.Stdout.Write(drivers.DescribeDriversJson())
		return 0
	}

	if fs.NArg() == 0 {
		glog.Error("Destination URI is not specified. See -j for usage.")
		return 1
	}

	// replace stdout to prevent any lib from writing debug output there
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)

	output := fs.Arg(0)
	if output == "" {
		glog.Error("Object store URI was empty")
		return 1
	}

	uri, err := url.Parse(output)
	if err != nil {
		glog.Errorf("Failed to parse URI: %s", err)
		return 1
	}

	out, err := core.Upload(os.Stdin, uri, WaitBetweenWrites, *timeout, storageBackupURLs)
	if err != nil {
		glog.Errorf("Uploader failed for %s: %s", uri.Redacted(), err)
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
			glog.Error(err)
			return 1
		}
	}

	return 0
}

func jsonFlag[T any](fs *flag.FlagSet, name string, usage string) T {
	var value T
	fs.Func(name, usage, func(s string) error {
		return json.Unmarshal([]byte(s), &value)
	})
	return value
}
