package moleculer

import (
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/common"
	"github.com/moleculer-go/moleculer/service"
)

type Service = service.ServiceSchema
type Action = service.ServiceActionSchema
type Event = service.ServiceEventSchema
type Mixin = service.MixinSchema
type Params = common.Params
type Context = common.Context

type BrokerConfig = broker.BrokerConfig

type ServiceBroker = broker.ServiceBroker

// returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func BrokerFromConfig(userConfig ...*BrokerConfig) *broker.ServiceBroker {
	return broker.FromConfig(userConfig)
}
