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
	"strings"
	"testing"
)

func buildUploader(assert *assert.Assertions) {
	// build app
	build := exec.Command("go", strings.Split("build catalyst-uploader.go", " ")...)
	err := build.Run()
	assert.NoError(err)
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
	buildUploader(assert)
	// create random data
	rndData := make([]byte, 1024*1024+10)
	rand.Read(rndData)
	stdinReader := bytes.NewReader(rndData)

	s3key := os.Getenv("AWS_TEST_KEY")
	s3secret := os.Getenv("AWS_TEST_SECRET")
	s3region := os.Getenv("AWS_TEST_REGION")
	s3bucket := os.Getenv("AWS_TEST_BUCKET")
	if s3key != "" && s3secret != "" && s3region != "" && s3bucket != "" {
		// run
		testKey := "/test/" + uuid.New().String() + ".ts"
		args := fmt.Sprintf("s3://%s:%s@%s/%s%s", s3key, s3secret, s3region, s3bucket, testKey)
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
		url, _ := url.Parse(outJson.Uri)
		// compare key after leading slash
		assert.Equal(testKey, url.Path)
		os, err := drivers.ParseOSURL(fmt.Sprintf("s3://%s:%s@%s/%s%s", s3key, s3secret, s3region, s3bucket, testKey), true)
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
	} else {
		fmt.Println("No S3 credentials, test skipped")
	}
}
