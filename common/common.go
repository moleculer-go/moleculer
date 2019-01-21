// common : contain common types
package common

import (
	"context"

	. "github.com/moleculer-go/goemitter"
	log "github.com/sirupsen/logrus"
)

type Endpoint interface {
	InvokeAction(context *context.Context) chan interface{}
	IsLocal() bool
}

type Strategy interface {
	SelectEndpoint([]*Endpoint) *Endpoint
}

type OptionsFunc func(key string) interface{}

func GetStringOption(key string, opts []OptionsFunc) string {
	result := GetOption(key, opts)
	if result != nil {
		return result.(string)
	}
	return ""
}

func GetOption(key string, opts []OptionsFunc) interface{} {
	for _, opt := range opts {
		result := opt(key)
		if result != nil {
			return result
		}
	}
	return nil
}

func WrapOptions(opts []OptionsFunc) OptionsFunc {
	return func(key string) interface{} {
		return GetOption(key, opts)
	}
}

type getLoggerFunction func(name string) *log.Entry
type getLocalBusFunction func() *Emitter
type isStartedFunction func() bool

type BrokerInfo struct {
	NodeID      string
	Logger      *log.Entry
	GetLogger   getLoggerFunction
	GetLocalBus getLocalBusFunction
	IsStarted   isStartedFunction
}

type NodeInfo struct {
	ID string
}

type ServiceInfo struct {
	Name string
}

type ActionEventInfo struct {
	Name string
}
