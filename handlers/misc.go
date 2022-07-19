package handlers

import (
	"github.com/sirupsen/logrus"
	"os"
)

type FatalToStderrHook struct {
}

func (h *FatalToStderrHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.FatalLevel}
}

func (h *FatalToStderrHook) Fire(e *logrus.Entry) error {
	errMsg, err := logrus.StandardLogger().Formatter.Format(e)
	if err != nil {
		return err
	}
	_, err = os.Stderr.Write(errMsg)
	return err
}
