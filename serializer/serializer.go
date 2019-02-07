package serializer

import "github.com/moleculer-go/moleculer"

type Serializer interface {
	BytesToMessage(bytes *[]byte) moleculer.Payload
	MessageToContextMap(moleculer.Payload) map[string]interface{}
	MapToMessage(mapValue *map[string]interface{}) (moleculer.Payload, error)
}

func FromConfig(broker moleculer.BrokerDelegates) Serializer {
	return CreateJSONSerializer(broker.Logger("serializer", "json"))
}
