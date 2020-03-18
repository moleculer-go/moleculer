package serializer

import (
	"github.com/moleculer-go/moleculer"
	"io"
)

type Serializer interface {
	ReaderToPayload(io.Reader) moleculer.Payload
	BytesToPayload(*[]byte) moleculer.Payload
	PayloadToBytes(moleculer.Payload) []byte
	PayloadToString(moleculer.Payload) string
	PayloadToContextMap(moleculer.Payload) map[string]interface{}
	MapToPayload(*map[string]interface{}) (moleculer.Payload, error)
}

func New(broker *moleculer.BrokerDelegates) Serializer {
	return CreateJSONSerializer(broker.Logger("serializer", "json"))
}
