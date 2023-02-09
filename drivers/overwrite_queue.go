package drivers

import (
	"bytes"
	"context"
	"time"
)

const (
	timeoutMultiplier    = 1.5
	overwriteQueueLength = 32
)

type (
	OverwriteQueue struct {
		desc           string
		name           string
		session        OSSession
		maxRetries     int
		initialTimeout time.Duration
		maxTimeout     time.Duration
		queue          chan []byte
		quit           chan struct{}
	}
)

func NewOverwriteQueue(session OSSession, name, desc string, maxRetries int, initialTimeout, maxTimeout time.Duration) *OverwriteQueue {
	oq := &OverwriteQueue{
		desc:           desc,
		name:           name,
		maxRetries:     maxRetries,
		session:        session,
		initialTimeout: initialTimeout,
		maxTimeout:     maxTimeout,
		queue:          make(chan []byte, overwriteQueueLength),
		quit:           make(chan struct{}),
	}
	if maxRetries < 1 {
		panic("maxRetries should be greater than zero")
	}
	go oq.workerLoop()
	return oq
}

// Save queues data to be saved
func (oq *OverwriteQueue) Save(data []byte) {
	oq.queue <- data
}

// StopAfter stops reading loop after some time
func (oq *OverwriteQueue) StopAfter(pause time.Duration) {
	go func(p time.Duration) {
		time.Sleep(p)
		close(oq.quit)
	}(pause)
}

func (oq *OverwriteQueue) workerLoop() {
	var err error
	for {
		select {
		case data := <-oq.queue:
			timeout := oq.initialTimeout
			for try := 0; try < oq.maxRetries; try++ {
				// we only care about last data
				data = oq.getLastMessage(data)
				_, err = oq.session.SaveData(context.Background(), oq.name, bytes.NewReader(data), nil, timeout)
				if err == nil {
					break
				}
				timeout = time.Duration(float64(timeout) * timeoutMultiplier)
				if timeout > oq.maxTimeout {
					timeout = oq.maxTimeout
				}
			}

		case <-oq.quit:
			return
		}
	}
}

func (oq *OverwriteQueue) getLastMessage(current []byte) []byte {
	res := current
	for {
		select {
		case data := <-oq.queue:
			res = data
		default:
			return res
		}
	}
}
