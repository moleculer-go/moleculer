package nats_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestNats(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Nats Suite")
}
