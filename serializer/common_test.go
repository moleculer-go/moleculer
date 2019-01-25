package serializer_test

import (
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", "Transit Pkg")

func CreateLogger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}
