package serializer

import "github.com/moleculer-go/moleculer"

type Serializer interface {
	BytesToPayload(*[]byte) moleculer.Payload
	PayloadToBytes(moleculer.Payload) []byte
	PayloadToContextMap(moleculer.Payload) map[string]interface{}
	MapToPayload(*map[string]interface{}) (moleculer.Payload, error)
}

func New(broker *moleculer.BrokerDelegates) Serializer {
	return CreateJSONSerializer(broker.Logger("serializer", "json"))
}
