package serializer_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSerializer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Serializer Suite")
}
