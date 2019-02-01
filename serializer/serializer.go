package serializer

import "github.com/moleculer-go/moleculer"
import "github.com/moleculer-go/moleculer/transit"

type Serializer interface {
	BytesToMessage(bytes *[]byte) transit.Message
	MessageToContextMap(transit.Message) map[string]interface{}
	MapToMessage(mapValue *map[string]interface{}) (transit.Message, error)
}

func FromConfig(broker moleculer.BrokerDelegates) Serializer {
	return CreateJSONSerializer(broker.Logger("serializer", "json"))
}
