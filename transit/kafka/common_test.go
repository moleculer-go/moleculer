package kafka_test

import (
	. "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/segmentio/kafka-go"

	"github.com/moleculer-go/moleculer/registry"
	log "github.com/sirupsen/logrus"
)

var commonTopics = []string{
	"MOL.DISCONNECT",
	"MOL.DISCOVER",
	"MOL.HEARTBEAT",
	"MOL.INFO",
	"MOL.PING",

	"MOL.DISCONNECT.user_node",
	"MOL.DISCONNECT.profile_node",
	"MOL.DISCONNECT.node1",
	"MOL.DISCOVER.user_node",
	"MOL.DISCOVER.profile_node",
	"MOL.DISCOVER.node1",
	"MOL.EVENT.user_node",
	"MOL.EVENT.profile_node",
	"MOL.EVENT.node1",
	"MOL.HEARTBEAT.user_node",
	"MOL.HEARTBEAT.profile_node",
	"MOL.HEARTBEAT.node1",
	"MOL.INFO.user_node",
	"MOL.INFO.profile_node",
	"MOL.INFO.node1",
	"MOL.PING.user_node",
	"MOL.PING.profile_node",
	"MOL.PING.node1",
	"MOL.PONG.user_node",
	"MOL.PONG.profile_node",
	"MOL.PONG.node1",
	"MOL.REQ.user_node",
	"MOL.REQ.profile_node",
	"MOL.REQ.node1",
	"MOL.RES.user_node",
	"MOL.RES.profile_node",
	"MOL.RES.node1",
}

func CreateCommonTopics(kafkaHost string) {
	conn, err := kafka.Dial("tcp", kafkaHost)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	var topicConfigs []kafka.TopicConfig
	for _, topic := range commonTopics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}

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
			{
				Name: "read",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					list := params.StringArray()
					return list
				},
			},
		},
	}
}

func profileService() moleculer.ServiceSchema {
	return moleculer.ServiceSchema{
		Name:         "profile",
		Dependencies: []string{"user"},
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					paramsList := params.StringArray()

					var payload []interface{}
					userUpdate := <-context.Call("user.update", payload)

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
