package endpoint

import (
	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/strategy"
)

type EndpointList struct {
	endpoints []*Endpoint

	localEndpoints []*Endpoint

	internal bool
}

// Next : Get next endpoint
func (endpointList *EndpointList) Next(strategy Strategy) *Endpoint {
	if len(endpointList.endpoints) == 0 {
		return nil
	}

	// If internal (service), return the local always
	if endpointList.internal && endpointList.hasLocal() {
		return endpointList.nextLocal(strategy)
	}
	return nil // todo
}

func (endpointList *EndpointList) hasLocal() bool {
	return len(endpointList.localEndpoints) > 0
}

type filterPredicate func(endpoint *Endpoint) bool

func filter(localEndpoints []*Endpoint, predicate filterPredicate) []*Endpoint {
	var result []*Endpoint
	for _, endpoint := range localEndpoints {
		if predicate(endpoint) {
			result = append(result, endpoint)
		}
	}
	return result
}

func (endpointList *EndpointList) nextLocal(strategy Strategy) *Endpoint {
	if len(endpointList.localEndpoints) == 0 {
		return nil
	}

	// Only 1 item
	if len(endpointList.localEndpoints) == 1 {
		// No need to select a node, return the only one
		item := endpointList.localEndpoints[0]
		if item.IsAvailable {
			return item
		}

		return nil
	}

	endpoints := filter(endpointList.localEndpoints, func(endpoint *Endpoint) bool {
		return endpoint.IsAvailable
	})
	if len(endpoints) == 0 {
		return nil
	}

	return strategy.SelectEndpoint(endpoints)
}
