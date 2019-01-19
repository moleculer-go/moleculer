package endpoint_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEndpointList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndpointList Suite")
}
