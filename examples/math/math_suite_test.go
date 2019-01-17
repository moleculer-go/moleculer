package math_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMath(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Math Suite")
}
