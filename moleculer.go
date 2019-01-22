package moleculer

import (
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/common"
	"github.com/moleculer-go/moleculer/params"
	"github.com/moleculer-go/moleculer/service"
)

type Service = service.ServiceSchema
type Action = service.ServiceActionSchema
type Event = service.ServiceEventSchema
type Params = params.Params

type Context = common.Context

// returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func BrokerFromConfig() *broker.ServiceBroker {
	return broker.FromConfig()
}
