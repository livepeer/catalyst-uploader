package handlers

import (
	"context"
	"errors"
	"io"
)

//S3Handler
func (S3Handler) NewSession(id string, secret string, param string) error {
	if secret == "" {
		return errors.New("S3 secret not specified")
	}
	panic("implement me")
}

func (S3Handler) Upload(ctx context.Context, uri string, reader io.Reader) (string, error) {
	panic("implement me")
}

func (S3Handler) UriScheme() string {
	return "s3"
}

func (S3Handler) Description() string {
	return "Storage service with AWS S3 interface. AWS requires the secret, and the region as 'param' argument."
}
