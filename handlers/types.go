package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/livepeer/dms-uploader/drivers"
	"io"
	"net/url"
	"path"
)

type StorageHandler interface {
	UriScheme() string
	Description() string
	NewSession(id string, secret string, param string) error
	Upload(ctx context.Context, uri string, reader io.Reader) (string, error)
}

type StorageHandlers []StorageHandler
type FileHandler struct{}
type S3Handler struct{}
type IpfsHandler struct{}


var AvailableHandlers = StorageHandlers{
	FileHandler{},
}

type StorageHandlerDescr struct {
	UriScheme   string `json:"scheme"`
	Description string `json:"desc"`
}

type ResUri struct {
	Uri string `json:"uri"`
}

func DescribeHandlersJson() []byte {
	var descrs []StorageHandlerDescr
	for _, h := range AvailableHandlers {
		descrs = append(descrs, StorageHandlerDescr{h.UriScheme(), h.Description()})
	}
	bytes, _ := json.Marshal(struct {
		Handlers []StorageHandlerDescr `json:"uri_handlers"`
	}{descrs})
	return bytes
}

// ParseOSURL returns the correct OS for a given OS url
func GetDriverByUrl(input string) (drivers.OSDriver, error) {
	u, err := url.Parse(input)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "s3" {
		pw, ok := u.User.Password()
		if !ok {
			return nil, fmt.Errorf("password is required with s3:// OS")
		}
		base := path.Base(u.Path)
		return drivers.NewS3Driver(u.Host, base, u.User.Username(), pw, false)
	}
	// custom s3-compatible store
	if u.Scheme == "s3+http" || u.Scheme == "s3+https" {
		scheme := "http"
		if u.Scheme == "s3+https" {
			scheme = "https"
		}
		region := "ignored"
		base, bucket := path.Split(u.Path)
		if len(base) > 1 && base[len(base)-1] == '/' {
			base = base[:len(base)-1]
			_, region = path.Split(base)
		}
		hosturl, err := url.Parse(input)
		if err != nil {
			return nil, err
		}
		hosturl.User = nil
		hosturl.Scheme = scheme
		hosturl.Path = ""
		pw, ok := u.User.Password()
		if !ok {
			return nil, fmt.Errorf("password is required with s3:// OS")
		}
		return drivers.NewCustomS3Driver(hosturl.String(), bucket, region, u.User.Username(), pw, false)
	}
	if u.Scheme == "gs" {
		file := u.User.Username()
		return drivers.NewGoogleDriver(u.Host, file, false)
	}
	return nil, fmt.Errorf("unrecognized OS scheme: %s", u.Scheme)
}