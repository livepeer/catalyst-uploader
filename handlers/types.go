package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
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

func (handlers StorageHandlers) Get(uriString string) (StorageHandler, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, err
	}
	scheme := strings.ToLower(uri.Scheme)
	for _, h := range AvailableHandlers {
		if scheme == h.UriScheme() {
			return h, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Handler not found for scheme: %q", scheme))
}
