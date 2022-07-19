package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

type StorageHandler interface {
	UriScheme() string
	Description() string
	NewSession(id string, secret string, param string) error
	UploadWithContext(ctx context.Context, uri string, reader io.Reader) (string, error)
}

type StorageHandlers []StorageHandler
type FileHandler struct{}
type S3Handler struct{}
type IpfsHandler struct{}

var AvailableHandlers = StorageHandlers{
	FileHandler{},
	//S3Handler{},
	//IpfsHandler{},
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

//FileHandler
func (FileHandler) NewSession(id string, secret string, param string) error {
	return nil
}

func (FileHandler) UploadWithContext(ctx context.Context, uri string, reader io.Reader) (string, error) {
	file, err := os.Create(uri)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 128*1024)
	defer file.Close()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			read, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				return "", err
			}
			if read > 0 {
				_, err = file.Write(buf[:read])
				if err != nil {
					return "", err
				}
			} else {
				return uri, nil
			}
		}
	}
}

func (FileHandler) UriScheme() string {
	return ""
}

func (FileHandler) Description() string {
	return "File system interface. All extra arguments are ignored."
}

//S3Handler
func (S3Handler) NewSession(id string, secret string, param string) error {
	if secret == "" {
		return errors.New("S3 secret not specified")
	}
	panic("implement me")
}

func (S3Handler) UploadWithContext(ctx context.Context, uri string, reader io.Reader) (string, error) {
	panic("implement me")
}

func (S3Handler) UriScheme() string {
	return "s3"
}

func (S3Handler) Description() string {
	return "Storage service with AWS S3 interface. AWS requires the secret, and the region as 'param' argument."
}

//IpfsHandler
func (IpfsHandler) NewSession(id string, secret string, param string) error {
	panic("implement me")
}

func (IpfsHandler) UploadWithContext(ctx context.Context, uri string, reader io.Reader) (string, error) {
	panic("implement me")
}

func (IpfsHandler) UriScheme() string {
	return "ipfs"
}

func (IpfsHandler) Description() string {
	return "Storage service with IPFS interface."
}
