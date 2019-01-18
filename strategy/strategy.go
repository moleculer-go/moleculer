package strategy

import (
	. "github.com/moleculer-go/moleculer/endpoint"
)

type Strategy interface {
	SelectEndpoint([]*Endpoint) *Endpoint
}
