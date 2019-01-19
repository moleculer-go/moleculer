package endpoint_test

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/endpointList"
)

var logger = log.WithField("Unit Test", true)

var _ = Describe("EndpointList", func() {

	Describe("EndpointList.Add", func() {

		broker := &BrokerInfo{"node-server-2", logger}
		node := &NodeInfo{"node-server-1"}
		service := &ServiceInfo{"earth"}
		actionEvent := &ActionEventInfo{"earth.rotate"}
		endpointList := EndpointList{}

		It("Should add a new Endpoint", func() {

			Expect(endpointList.Size()).Should(Equal(0))

			endpoint := endpointList.Add(broker, node, service, actionEvent)

			Expect(endpoint).Should(Not(BeNil()))

			Expect(endpoint.Name).Should(Equal(fmt.Sprintf("%s:%s", node.ID, actionEvent.Name)))
			Expect(endpoint.Broker.NodeID).Should(Equal(broker.NodeID))
			Expect(endpoint.Node.ID).Should(Equal(node.ID))
			Expect(endpoint.Service.Name).Should(Equal(service.Name))
			Expect(endpoint.ActionEvent.Name).Should(Equal(actionEvent.Name))

			Expect(endpointList.Size()).Should(Equal(1))
			Expect(endpointList.SizeLocal()).Should(Equal(0))

		})

		It("Should add a new local Endpoint", func() {

			Expect(endpointList.Size()).Should(Equal(1))

			node := &NodeInfo{"node-server-2"}
			endpoint := endpointList.Add(broker, node, service, actionEvent)

			Expect(endpoint).Should(Not(BeNil()))

			Expect(endpoint.Name).Should(Equal(fmt.Sprintf("%s:%s", node.ID, actionEvent.Name)))
			Expect(endpoint.Broker.NodeID).Should(Equal(broker.NodeID))
			Expect(endpoint.Node.ID).Should(Equal(node.ID))
			Expect(endpoint.Service.Name).Should(Equal(service.Name))
			Expect(endpoint.ActionEvent.Name).Should(Equal(actionEvent.Name))

			Expect(endpointList.Size()).Should(Equal(2))
			Expect(endpointList.SizeLocal()).Should(Equal(1))

		})

		It("Should update action on existing Endpoint", func() {

			Expect(endpointList.Size()).Should(Equal(2))

			//update broker to it becames local and we can verify at the end
			broker := &BrokerInfo{"node-server-1", logger}
			endpoint := endpointList.Add(broker, node, service, actionEvent)

			Expect(endpoint).Should(Not(BeNil()))

			Expect(endpoint.Name).Should(Equal(fmt.Sprintf("%s:%s", node.ID, actionEvent.Name)))
			Expect(endpoint.Broker.NodeID).Should(Equal(broker.NodeID))
			Expect(endpoint.Node.ID).Should(Equal(node.ID))
			Expect(endpoint.Service.Name).Should(Equal(service.Name))
			Expect(endpoint.ActionEvent.Name).Should(Equal(actionEvent.Name))

			Expect(endpointList.Size()).Should(Equal(2))
			Expect(endpointList.SizeLocal()).Should(Equal(2))

		})
	})

})
