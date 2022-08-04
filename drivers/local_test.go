package drivers

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/url"
	"strings"
	"testing"
)

func TestLocalOS(t *testing.T) {
	tempData1 := "dataitselftempdata1"
	tempData2 := "dataitselftempdata2"
	tempData3 := "dataitselftempdata3"
	oldDataCacheLen := dataCacheLen
	dataCacheLen = 1
	defer func() {
		dataCacheLen = oldDataCacheLen
	}()
	assert := assert.New(t)
	u, err := url.Parse("fake.com/url")
	assert.NoError((err))
	os := NewMemoryDriver(u)
	sess := os.NewSession(("sesspath")).(*MemorySession)
	path, err := sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData1), nil, 0)
	fmt.Println(path)
	assert.Equal("fake.com/url/stream/sesspath/name1/1.ts", path)
	data := sess.GetData("sesspath/name1/1.ts")
	fmt.Printf("got Data: '%s'\n", data)
	assert.Equal(tempData1, string(data))
	path, err = sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData2), nil, 0)
	data = sess.GetData("sesspath/name1/1.ts")
	assert.Equal(tempData2, string(data))
	path, err = sess.SaveData(context.TODO(), "name1/2.ts", strings.NewReader(tempData3), nil, 0)
	data = sess.GetData("sesspath/name1/2.ts")
	assert.Equal(tempData3, string(data))
	// Test trim prefix when baseURI != nil
	data = sess.GetData(path)
	assert.Equal(tempData3, string(data))
	data = sess.GetData("sesspath/name1/1.ts")
	assert.Nil(data)
	sess.EndSession()
	data = sess.GetData("sesspath/name1/2.ts")
	assert.Nil(data)

	// Test trim prefix when baseURI = nil
	os = NewMemoryDriver(nil)
	sess = os.NewSession("sesspath").(*MemorySession)
	path, err = sess.SaveData(context.TODO(), "name1/1.ts", strings.NewReader(tempData1), nil, 0)
	assert.Nil(err)
	assert.Equal("/stream/sesspath/name1/1.ts", path)

	data = sess.GetData(path)
	assert.Equal(tempData1, string(data))
}
