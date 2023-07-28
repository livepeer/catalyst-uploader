package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/livepeer/go-tools/drivers"
	"github.com/stretchr/testify/require"
)

func splitNonEmpty(str string, sep rune) []string {
	splitFn := func(c rune) bool {
		return c == sep
	}
	return strings.FieldsFunc(str, splitFn)
}

func testE2E(t *testing.T, fullUriStr string) {
	// create random data
	rndData := make([]byte, 1024*128+10)
	rand.Read(rndData)
	stdinReader := bytes.NewReader(rndData)
	// run
	uploader := exec.Command("go", "run", "catalyst-uploader.go", "fullUriStr")
	uploader.Stdin = stdinReader
	stdoutRes, err := uploader.Output()
	fmt.Println(string(stdoutRes))
	require.NoError(t, err)

	// check output
	outJson := struct {
		Uri string `json:"uri"`
	}{}
	err = json.Unmarshal(stdoutRes, &outJson)
	require.NoError(t, err)

	// load object and compare contents
	outUrl, _ := url.Parse(outJson.Uri)
	if strings.Contains(fullUriStr, "ipfs://") {
		cid := outUrl.Path
		resp, err := http.Get("https://gateway.pinata.cloud/ipfs/" + cid)
		require.NoError(t, err)
		defer resp.Body.Close()
		ipfsData := new(bytes.Buffer)
		ipfsData.ReadFrom(resp.Body)
		require.Equal(t, rndData, ipfsData.Bytes())
	} else {
		fullUri, _ := outUrl.Parse(fullUriStr)
		bucket := splitNonEmpty(fullUri.Path, '/')[0]
		if !strings.Contains(outUrl.Host, bucket) {
			// if bucket is not included in domain name of output URI, then it's already in the path
			bucket = ""
		}
		// compare key after leading slash
		require.Equal(t, fullUri.Path, path.Clean("/"+bucket+"/"+outUrl.Path))
		os, err := drivers.ParseOSURL(fullUriStr, true)
		require.NoError(t, err)
		session := os.NewSession("")
		// second argument is object key and passed to API unmodified
		data, err := session.ReadData(context.Background(), "")
		require.NoError(t, err)
		require.Equal(t, *data.Size, int64(len(rndData)))
		osBuf := new(bytes.Buffer)
		osBuf.ReadFrom(data.Body)
		osData := osBuf.Bytes()
		require.Equal(t, rndData, osData)
	}
}

func TestFsHandlerE2E(t *testing.T) {
	// create random data
	rndData := make([]byte, 1024*1024+10)
	rand.Read(rndData)
	fileData := make([]byte, len(rndData))
	stdinReader := bytes.NewReader(rndData)
	outFileName := "/tmp/test-fs-upload.dat"
	defer os.Remove(outFileName)

	// run
	uploader := exec.Command("go", "run", "catalyst-uploader.go", outFileName)
	uploader.Stdin = stdinReader
	stdoutRes, err := uploader.Output()
	require.NoError(t, err)

	// check output
	outJson := struct {
		Uri string `json:"uri"`
	}{}
	err = json.Unmarshal(stdoutRes, &outJson)
	require.NoError(t, err)
	require.Equal(t, outFileName, outJson.Uri)

	// check file
	stat, err := os.Stat(outFileName)
	require.NoError(t, err)
	require.Equal(t, int64(len(rndData)), stat.Size())

	// compare contents
	file, _ := os.Open(outFileName)
	_, _ = file.Read(fileData)
	require.Equal(t, rndData, fileData)
}

func TestS3HandlerE2E(t *testing.T) {
	s3key := os.Getenv("AWS_S3_KEY")
	s3secret := os.Getenv("AWS_S3_SECRET")
	s3region := os.Getenv("AWS_S3_REGION")
	s3bucket := os.Getenv("AWS_S3_BUCKET")
	if s3key != "" && s3secret != "" && s3region != "" && s3bucket != "" {
		testKey := "/test/" + uuid.New().String() + ".ts"
		uri := fmt.Sprintf("s3://%s:%s@%s/%s%s", s3key, s3secret, s3region, s3bucket, testKey)
		testE2E(t, uri)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}

func TestIpfsHandlerE2E(t *testing.T) {
	key := os.Getenv("PINATA_KEY")
	secret := os.Getenv("PINATA_SECRET")
	if secret != "" {
		uri := fmt.Sprintf("ipfs://%s:%s@%s/", key, secret, "pinata.cloud")
		testE2E(t, uri)
	} else {
		fmt.Println("No IPFS provider credentials, test skipped")
	}
}

func TestMinioHandlerE2E(t *testing.T) {
	s3key := os.Getenv("MINIO_S3_KEY")
	s3secret := os.Getenv("MINIO_S3_SECRET")
	s3bucket := os.Getenv("MINIO_S3_BUCKET")
	if s3key != "" && s3secret != "" && s3bucket != "" {
		testKey := "/test/" + uuid.New().String() + ".ts"
		uri := fmt.Sprintf("s3+http://%s:%s@localhost:9000/%s%s", s3key, s3secret, s3bucket, testKey)
		testE2E(t, uri)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}

func TestFormatsE2E(t *testing.T) {
	uploader := exec.Command("go", "run", "catalyst-uploader.go", "-j")
	stdoutRes, err := uploader.Output()
	require.NoError(t, err)
	var driverDescr struct {
		Drivers []drivers.OSDriverDescr `json:"storage_drivers"`
	}
	err = json.Unmarshal(stdoutRes, &driverDescr)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, len(drivers.AvailableDrivers), len(driverDescr.Drivers))
	for i, h := range drivers.AvailableDrivers {
		require.Equal(t, h.Description(), driverDescr.Drivers[i].Description)
		require.Equal(t, h.UriSchemes(), driverDescr.Drivers[i].UriSchemes)
	}
}
