package handlers

import (
	"context"
	"io"
	"os"
)

//FileHandler
func (FileHandler) NewSession(id string, secret string, param string) error {
	return nil
}

func (FileHandler) Upload(ctx context.Context, uri string, reader io.Reader) (string, error) {
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
