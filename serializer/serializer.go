package serializer

import "github.com/moleculer-go/moleculer"

type Serializer interface {
	BytesToPayload(bytes *[]byte) moleculer.Payload
	PayloadToContextMap(moleculer.Payload) map[string]interface{}
	MapToPayload(mapValue *map[string]interface{}) (moleculer.Payload, error)
}

func FromConfig(broker moleculer.BrokerDelegates) Serializer {
	return CreateJSONSerializer(broker.Logger("serializer", "json"))
}
