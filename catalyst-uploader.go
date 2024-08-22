package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("catalyst-uploader", flag.ExitOnError)

	// cmd line args
	version := fs.Bool("version", false, "print application version")
	describe := fs.Bool("j", false, "Describe supported storage services in JSON format and exit")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	timeout := fs.Duration("t", 30*time.Second, "Upload timeout")
	storageFallbackURLs := CommaMapFlag(fs, "storage-fallback-urls", `Comma-separated map of primary to backup storage URLs. If a file fails uploading to one of the primary storages (detected by prefix), it will fallback to the corresponding backup URL after having the prefix replaced`)
	segTimeout := fs.Duration("segment-timeout", 5*time.Minute, "Segment write timeout")

	defaultConfigFile := "/etc/livepeer/catalyst_uploader.conf"
	if _, err := os.Stat(defaultConfigFile); os.IsNotExist(err) {
		defaultConfigFile = ""
	}
	_ = fs.String("config", defaultConfigFile, "config file (optional)")

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

	if *verbosity != "" {
		err = vFlag.Value.Set(*verbosity)
		if err != nil {
			glog.Fatal(err)
		}
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

	start := time.Now()
	out, err := core.Upload(os.Stdin, uri, WaitBetweenWrites, *timeout, *storageFallbackURLs, *segTimeout)
	if err != nil {
		glog.Errorf("Uploader failed for %s: %s", uri.Redacted(), err)
		return 1
	}

	var respHeaders http.Header
	if out != nil {
		respHeaders = out.UploaderResponseHeaders
	}
	glog.Infof("Uploader succeeded for %s. storageRequestID=%s Etag=%s timeTaken=%vms", uri.Redacted(), respHeaders.Get("X-Amz-Request-Id"), respHeaders.Get("Etag"), time.Since(start).Milliseconds())
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

// handles -foo=key1=value1,key2=value2
func CommaMapFlag(fs *flag.FlagSet, name string, usage string) *map[string]string {
	var dest map[string]string
	fs.Func(name, usage, func(s string) error {
		var err error
		dest, err = parseCommaMap(s)
		return err
	})
	return &dest
}

func parseCommaMap(s string) (map[string]string, error) {
	output := map[string]string{}
	if s == "" {
		return output, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return map[string]string{}, fmt.Errorf("failed to parse keypairs, -option=k1=v1,k2=v2 format required, got %s", s)
		}
		k, v := kv[0], kv[1]
		output[k] = v
	}
	return output, nil
}
