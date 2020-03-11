package pubsub

import (
	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/test"
	"github.com/moleculer-go/moleculer/transit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "ERROR"

var _ = Describe("PubSub Internals", func() {

	It("Should return the number of neighbours", func() {

		pubsub := PubSub{knownNeighbours: map[string]int64{
			"x": int64(10),
			"y": int64(10),
			"z": int64(10),
		}}

		Expect(pubsub.neighbours()).Should(BeEquivalentTo(3))
	})

	It("onServiceAdded should call broadcastNodeInfo for local service", func() {
		localNode := test.NodeMock{ID: "test", ExportAsMapResult: map[string]interface{}{}}
		mockT := &mockTransporter{}
		svc := service.Service{}
		svc.SetNodeID(localNode.GetID())
		pubsub := PubSub{
			isConnected: true,
			serializer:  &serializer.JSONSerializer{},
			broker: &moleculer.BrokerDelegates{
				LocalNode: func() moleculer.Node {
					return &localNode
				},
			},
			transport:     mockT,
			brokerStarted: true,
		}
		pubsub.onServiceAdded(svc.Summary())
		Expect(mockT.PublishCalled).Should(BeTrue())
	})

	It("onServiceAdded shouldn't call broadcastNodeInfo for remote service", func() {
		localNode := test.NodeMock{ID: "test", ExportAsMapResult: map[string]interface{}{}}
		mockT := &mockTransporter{}
		svc := service.Service{}
		svc.SetNodeID("test-remote")
		pubsub := PubSub{
			isConnected: true,
			serializer:  &serializer.JSONSerializer{},
			broker: &moleculer.BrokerDelegates{
				LocalNode: func() moleculer.Node {
					return &localNode
				},
			},
			transport:     mockT,
			brokerStarted: true,
		}
		pubsub.onServiceAdded(svc.Summary())
		Expect(mockT.PublishCalled).Should(BeFalse())
	})

	It("onBrokerStarted should call broadcastNodeInfo", func() {
		localNode := test.NodeMock{ID: "test", ExportAsMapResult: map[string]interface{}{}}
		mockT := &mockTransporter{}
		pubsub := PubSub{
			isConnected: true,
			serializer:  &serializer.JSONSerializer{},
			broker: &moleculer.BrokerDelegates{
				LocalNode: func() moleculer.Node {
					return &localNode
				},
			},
			transport: mockT,
		}
		pubsub.onBrokerStarted()
		Expect(mockT.PublishCalled).Should(BeTrue())
	})

	It("should create a new pubsub Create()", func() {
		localNode := test.NodeMock{ID: "test", ExportAsMapResult: map[string]interface{}{}}
		pubsub := Create(&moleculer.BrokerDelegates{
			Logger: func(name string, value string) *log.Entry { return log.WithField(name, value) },
			LocalNode: func() moleculer.Node {
				return &localNode
			},
			Bus: func() *bus.Emitter {
				return bus.Construct()
			},
		})
		Expect(pubsub).ShouldNot(BeNil())
	})

	It("should find a pending request by nodeID)", func() {
		//TODO
	})
})

type mockTransporter struct {
	SubscribeCalled bool
	PublishCalled   bool
}

func (t *mockTransporter) Connect() chan error {
	return nil
}

func (t *mockTransporter) Disconnect() chan error {
	return nil
}

func (t *mockTransporter) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	t.SubscribeCalled = true
}
func (t *mockTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	t.PublishCalled = true
}

func (t *mockTransporter) SetPrefix(string) {
}

func (t *mockTransporter) SetNodeID(string) {
}

func (t *mockTransporter) SetSerializer(serializer.Serializer) {
}
