package endpoint

import (
	"context"
)

type ActionEndpoint struct {
	Name string
}

type Endpoint struct {
	Action ActionEndpoint

	IsAvailable bool
}

// ActionHandler : Invoke the action handler.
func (endpoint *Endpoint) ActionHandler(context *context.Context) {

}
