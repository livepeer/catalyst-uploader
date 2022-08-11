package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/livepeer/go-tools/drivers"
	"github.com/stretchr/testify/assert"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
)

func splitNonEmpty(str string, sep rune) []string {
	splitFn := func(c rune) bool {
		return c == sep
	}
	return strings.FieldsFunc(str, splitFn)
}

func buildUploader(assert *assert.Assertions) {
	// build app
	build := exec.Command("go", strings.Split("build catalyst-uploader.go", " ")...)
	err := build.Run()
	assert.NoError(err)
}

func testE2E(assert *assert.Assertions, fullUriStr string) {
	buildUploader(assert)
	// create random data
	rndData := make([]byte, 1024*128+10)
	rand.Read(rndData)
	stdinReader := bytes.NewReader(rndData)
	// run
	args := fullUriStr
	uploader := exec.Command("./catalyst-uploader", strings.Split(args, " ")...)
	uploader.Stdin = stdinReader
	stdoutRes, err := uploader.Output()
	fmt.Println(string(stdoutRes))
	assert.NoError(err)

	// check output
	outJson := struct {
		Uri string `json:"uri"`
	}{}
	err = json.Unmarshal(stdoutRes, &outJson)
	assert.NoError(err)

	// load object and compare contents
	outUrl, _ := url.Parse(outJson.Uri)
	fullUri, _ := outUrl.Parse(fullUriStr)
	bucket := splitNonEmpty(fullUri.Path, '/')[0]
	if !strings.Contains(outUrl.Host, bucket) {
		// if bucket is not included in domain name of output URI, then it's already in the path
		bucket = ""
	}
	// compare key after leading slash
	assert.Equal(fullUri.Path, path.Clean("/"+bucket+"/"+outUrl.Path))
	os, err := drivers.ParseOSURL(fullUriStr, true)
	assert.NoError(err)
	session := os.NewSession("")
	// second argument is object key and passed to API unmodified
	data, err := session.ReadData(context.Background(), "")
	assert.NoError(err)
	assert.Equal(*data.Size, int64(len(rndData)))
	osBuf := new(bytes.Buffer)
	osBuf.ReadFrom(data.Body)
	osData := osBuf.Bytes()
	assert.Equal(rndData, osData)
}

func TestFsHandlerE2E(t *testing.T) {
	assert := assert.New(t)
	buildUploader(assert)
	// create random data
	rndData := make([]byte, 1024*1024+10)
	rand.Read(rndData)
	fileData := make([]byte, len(rndData))
	stdinReader := bytes.NewReader(rndData)
	outFileName := "/tmp/test-fs-upload.dat"
	defer os.Remove(outFileName)

	// run
	args := fmt.Sprintf("/tmp/test-fs-upload.dat")
	uploader := exec.Command("./catalyst-uploader", strings.Split(args, " ")...)
	uploader.Stdin = stdinReader
	stdoutRes, err := uploader.Output()
	assert.NoError(err)

	// check output
	outJson := struct {
		Uri string `json:"uri"`
	}{}
	err = json.Unmarshal(stdoutRes, &outJson)
	assert.NoError(err)
	assert.Equal(outFileName, outJson.Uri)

	// check file
	stat, err := os.Stat(outFileName)
	assert.NoError(err)
	assert.Equal(int64(len(rndData)), stat.Size())

	// compare contents
	file, _ := os.Open(outFileName)
	_, _ = file.Read(fileData)
	assert.Equal(rndData, fileData)
}

func TestS3HandlerE2E(t *testing.T) {
	assert := assert.New(t)
	s3key := os.Getenv("AWS_S3_KEY")
	s3secret := os.Getenv("AWS_S3_SECRET")
	s3region := os.Getenv("AWS_S3_REGION")
	s3bucket := os.Getenv("AWS_S3_BUCKET")
	if s3key != "" && s3secret != "" && s3region != "" && s3bucket != "" {
		testKey := "/test/" + uuid.New().String() + ".ts"
		uri := fmt.Sprintf("s3://%s:%s@%s/%s%s", s3key, s3secret, s3region, s3bucket, testKey)
		testE2E(assert, uri)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}

func TestMinioHandlerE2E(t *testing.T) {
	assert := assert.New(t)
	s3key := os.Getenv("MINIO_S3_KEY")
	s3secret := os.Getenv("MINIO_S3_SECRET")
	s3bucket := os.Getenv("MINIO_S3_BUCKET")
	if s3key != "" && s3secret != "" && s3bucket != "" {
		testKey := "/test/" + uuid.New().String() + ".ts"
		uri := fmt.Sprintf("s3+http://%s:%s@localhost:9000/%s%s", s3key, s3secret, s3bucket, testKey)
		testE2E(assert, uri)
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}
