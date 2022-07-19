package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/livepeer/dms-uploader/core"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestFsHandlerE2E(t *testing.T) {
	assert := assert.New(t)
	// build app
	build := exec.Command("go", strings.Split("build dms-uploader.go", " ")...)
	err := build.Run()
	assert.NoError(err)
	// create random data
	rndData := make([]byte, 1024*1024+10)
	fileData := make([]byte, len(rndData))
	stdinReader := bytes.NewReader(rndData)
	outFileName := "/tmp/test-fs-upload.dat"
	defer os.Remove(outFileName)

	// run
	args := fmt.Sprintf("-path %s", outFileName)
	uploader := exec.Command("./dms-uploader", strings.Split(args, " ")...)
	uploader.Stdin = stdinReader
	stdoutRes, err := uploader.Output()
	assert.NoError(err)

	// check output
	outJson := core.ResUri{}
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
