package service

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestInternals(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Service Internal Test Suite")
}
