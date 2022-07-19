package handlers

import (
	"context"
	"io"
)

//IpfsHandler
func (IpfsHandler) NewSession(id string, secret string, param string) error {
	panic("implement me")
}

func (IpfsHandler) Upload(ctx context.Context, uri string, reader io.Reader) (string, error) {
	panic("implement me")
}

func (IpfsHandler) UriScheme() string {
	return "ipfs"
}

func (IpfsHandler) Description() string {
	return "Storage service with IPFS interface."
}
