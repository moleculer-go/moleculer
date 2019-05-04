package nats_test

import (
	. "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"

	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/registry"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("unit test pkg", "transit_test")

func CreateLogger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}

var localNode = registry.CreateNode("unit-test-node", true, logger)

func BrokerDelegates() *moleculer.BrokerDelegates {
	localBus := Construct()
	broker := &moleculer.BrokerDelegates{
		LocalNode: func() moleculer.Node {
			return localNode
		},
		Logger: CreateLogger,
		Bus: func() *Emitter {
			return localBus
		}}
	return broker
}

func userService() moleculer.ServiceSchema {
	return moleculer.ServiceSchema{
		Name: "user",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					list := params.StringArray()
					list = append(list, "user update")
					return list
				},
			},
		},
	}
}

func contactService() moleculer.ServiceSchema {
	return moleculer.ServiceSchema{
		Name: "contact",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					list := params.StringArray()
					list = append(list, "contact update")
					return list
				},
			},
		},
	}
}

func profileService() moleculer.ServiceSchema {
	return moleculer.ServiceSchema{
		Name:         "profile",
		Dependencies: []string{"user", "contact"},
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					paramsList := params.StringArray()

					contactList := <-context.Call("contact.update", []string{"profile update"})
					userUpdate := <-context.Call("user.update", contactList)

					userList := userUpdate.StringArray()
					for _, item := range paramsList {
						userList = append(userList, item)
					}

					return userList
				},
			},
		},
	}
}

func stopBrokers(brokers ...*broker.ServiceBroker) {
	for _, bkr := range brokers {
		bkr.Stop()
	}
}
