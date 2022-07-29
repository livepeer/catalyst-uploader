package drivers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestS3Upload(t *testing.T) {
	s3key := os.Getenv("AWS_TEST_KEY")
	s3secret := os.Getenv("AWS_TEST_SECRET")
	s3region := os.Getenv("AWS_TEST_REGION")
	s3bucket := os.Getenv("AWS_TEST_BUCKET")
	assert := assert.New(t)
	if s3key != "" && s3secret != "" && s3region != "" && s3bucket != "" {
		rndData := make([]byte, 1024*10)
		rand.Read(rndData)
		testKey := uuid.New().String() + ".ts"
		os, err := ParseOSURL(fmt.Sprintf("s3://%s:%s@%s/%s", s3key, s3secret, s3region, s3bucket), true)
		assert.NoError(err)
		session := os.NewSession("test")
		uri, err := session.SaveData(context.Background(), testKey, bytes.NewReader(rndData), nil, 10*time.Second)
		assert.NoError(err)
		url, _ := url.Parse(uri)
		data, err := session.ReadData(context.Background(), url.Path)
		assert.NoError(err)
		assert.Equal(*data.Size, int64(len(rndData)))
		osBuf := new(bytes.Buffer)
		osBuf.ReadFrom(data.Body)
		osData := osBuf.Bytes()
		assert.Equal(rndData, osData)
	}
}
