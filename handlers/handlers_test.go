package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestDescribeHandlersJson(t *testing.T) {
	assert := assert.New(t)
	handlersJson := DescribeHandlersJson()
	var handlersDescr struct {
		Handlers []StorageHandlerDescr `json:"uri_handlers"`
	}
	err := json.Unmarshal(handlersJson, &handlersDescr)
	assert.NoError(err)
	assert.Equal(len(AvailableHandlers), len(handlersDescr.Handlers))
	for i, h := range AvailableHandlers {
		assert.Equal(h.Description(), handlersDescr.Handlers[i].Description)
		assert.Equal(h.UriScheme(), handlersDescr.Handlers[i].UriScheme)
	}
}

func TestGetHandler(t *testing.T) {
	assert := assert.New(t)
	validUris := []string{
		// todo: uncomment after handlers are implemented
		//"s3://region_url/bucket/id",
		//"S3://region_url/bucket/id",
		//"S3://id:key@region_url/bucket/id",
		//"ipfs://region_url/bucket/id",
		"/path/to/file",
	}
	invalidUris := []string{
		"://region_url/bucket/id",
	}
	for _, uriStr := range validUris {
		uri, _ := url.Parse(uriStr)
		handler, err := AvailableHandlers.Get(uriStr)
		assert.NoError(err)
		assert.Equal(strings.ToLower(uri.Scheme), handler.UriScheme())
	}
	for _, uriStr := range invalidUris {
		handler, err := AvailableHandlers.Get(uriStr)
		assert.Error(err)
		assert.Nil(handler)
	}
}

func TestUpload(t *testing.T) {
	assert := assert.New(t)
	testFsUpload := func() {
		// create random memory buf
		rndData := make([]byte, 1024*1024+10)
		fileData := make([]byte, len(rndData))
		rand.Read(rndData)
		outFileName := "/tmp/test-fs-upload.dat"
		defer os.Remove(outFileName)
		h, _ := AvailableHandlers.Get(outFileName)
		// write to tmp file
		resUri, err := h.Upload(context.Background(), outFileName, bytes.NewReader(rndData))
		assert.NoError(err)
		assert.Equal(resUri, outFileName)
		stat, err := os.Stat(outFileName)
		assert.NoError(err)
		assert.Equal(int64(len(rndData)), stat.Size())
		// compare contents
		file, _ := os.Open(outFileName)
		_, _ = file.Read(fileData)
		assert.Equal(rndData, fileData)
	}
	testFuncs := map[string]func(){
		"": testFsUpload,
	}

	for _, h := range AvailableHandlers {
		testFn, ok := testFuncs[h.UriScheme()]
		if !ok {
			assert.Failf("Test incomplete", "No test function for enabled handler %s", reflect.TypeOf(h).String())
			break
		}
		testFn()
	}
}
