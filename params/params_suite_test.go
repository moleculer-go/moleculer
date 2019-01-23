package params_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestParams(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Params Suite")
}
