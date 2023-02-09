package drivers

import (
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type FSOS struct {
	baseURI  *url.URL
	sessions map[string]*FSSession
	lock     sync.RWMutex
}

type FSSession struct {
	os     *FSOS
	path   string
	ended  bool
	dCache map[string]*dataCache
	dLock  sync.RWMutex
}

func NewFSDriver(baseURI *url.URL) *FSOS {
	return &FSOS{
		baseURI:  baseURI,
		sessions: make(map[string]*FSSession),
		lock:     sync.RWMutex{},
	}
}

func (ostore *FSOS) NewSession(path string) OSSession {
	ostore.lock.Lock()
	defer ostore.lock.Unlock()
	if session, ok := ostore.sessions[path]; ok {
		return session
	}
	session := &FSSession{
		os:     ostore,
		path:   path,
		dCache: make(map[string]*dataCache),
		dLock:  sync.RWMutex{},
	}
	ostore.sessions[path] = session
	return session
}

func (ostore *FSOS) GetSession(path string) *FSSession {
	ostore.lock.Lock()
	defer ostore.lock.Unlock()
	if session, ok := ostore.sessions[path]; ok {
		return session
	}
	return nil
}

func (ostore *FSOS) UriSchemes() []string {
	return []string{""}
}

func (ostore *FSOS) Description() string {
	return "File system driver."
}

func (ostore *FSSession) OS() OSDriver {
	return ostore.os
}

// EndSession clears memory cache
func (ostore *FSSession) EndSession() {
	ostore.dLock.Lock()
	ostore.ended = true
	for k := range ostore.dCache {
		delete(ostore.dCache, k)
	}
	ostore.dLock.Unlock()

	ostore.os.lock.Lock()
	delete(ostore.os.sessions, ostore.path)
	ostore.os.lock.Unlock()
}

func (ostore *FSSession) ListFiles(ctx context.Context, dir, delim string) (PageInfo, error) {
	pi := &singlePageInfo{
		files:       []FileInfo{},
		directories: []string{},
	}
	ostore.dLock.RLock()
	defer ostore.dLock.RUnlock()

	fullPath := ostore.getAbsoluteURI(dir)

	if fullPath == "" {
		return pi, nil
	}

	// list files in specified dir
	files, err := ioutil.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	// create metadata
	for _, f := range files {
		if f.IsDir() {
			pi.directories = append(pi.directories, f.Name())
		} else {
			size := f.Size()
			pi.files = append(pi.files, FileInfo{
				Name:         f.Name(),
				ETag:         "",
				LastModified: f.ModTime(),
				Size:         &size,
			})
		}
	}
	return pi, nil
}

func (ostore *FSSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	prefix := ""
	if ostore.os.baseURI != nil {
		prefix += ostore.os.baseURI.String()
	}
	fullPath := path.Join(prefix, name)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	size := stat.Size()
	res := &FileInfoReader{
		FileInfo: FileInfo{
			Name: name,
			Size: &size,
		},
		Body: file,
	}
	return res, nil
}

func (ostore *FSSession) IsExternal() bool {
	return false
}

func (ostore *FSSession) IsOwn(url string) bool {
	return strings.HasPrefix(url, ostore.path)
}

func (ostore *FSSession) GetInfo() *OSInfo {
	return nil
}

func (ostore *FSSession) SaveData(ctx context.Context, name string, data io.Reader, meta map[string]string, timeout time.Duration) (string, error) {
	fullPath := ostore.getAbsoluteURI(name)
	dir, name := path.Split(fullPath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return "", err
	}
	file, err := os.Create(fullPath)
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
			read, err := data.Read(buf)
			if err != nil && err != io.EOF {
				return "", err
			}
			if read > 0 {
				_, err = file.Write(buf[:read])
				if err != nil {
					return "", err
				}
			} else {
				return fullPath, nil
			}
		}
	}
}

func (ostore *FSSession) getCacheForStream(streamID string) *dataCache {
	sc, ok := ostore.dCache[streamID]
	if !ok {
		sc = newDataCache(dataCacheLen)
		ostore.dCache[streamID] = sc
	}
	return sc
}

func (ostore *FSSession) getAbsolutePath(name string) string {
	return path.Clean(ostore.path + "/" + name)
}

func (ostore *FSSession) getAbsoluteURI(name string) string {
	if ostore.os.baseURI != nil {
		return path.Join(ostore.os.baseURI.String(), ostore.getAbsolutePath(name))
	} else {
		return ostore.getAbsolutePath(name)
	}
}
