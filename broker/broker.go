package broker

import (
	"context"
	"fmt"

	"github.com/moleculer-go/moleculer/service"
)

type ServiceBroker struct {
	Context *context.Context
}

func (broker *ServiceBroker) Start(service *service.Service) {
	fmt.Println("Broker - start !")
}

func (broker *ServiceBroker) Call(action string, params interface{}) interface{} {
	fmt.Println("Broker - call !")
	return 0
}

func (broker *ServiceBroker) Emit(event string, params interface{}) {
	fmt.Println("Broker - emit !")
}

// returns a valid broker based on a passed context
// this is called from any action / event
func BrokerFromContext(ctx *context.Context) *ServiceBroker {
	brokerInstance := ServiceBroker{ctx}
	return &brokerInstance
}

// returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func BrokerFromConfig() *ServiceBroker {
	fmt.Println("Broker - brokerFromConfig() ")
	brokerInstance := ServiceBroker{}
	return &brokerInstance
}
