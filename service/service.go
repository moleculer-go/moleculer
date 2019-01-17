package service

import (
	"context"

	"github.com/moleculer-go/moleculer/params"
)

type ActionSchema struct {
}

type ActionHandler func(ctx context.Context, params params.Params) interface{}

type EventHandler func(ctx context.Context, params params.Params)

type ServiceAction struct {
	Name    string
	Handler ActionHandler
	Schema  ActionSchema
}

type ServiceEvent struct {
	Name    string
	Handler EventHandler
}

type Service struct {
	Name    string
	Actions []ServiceAction
	Events  []ServiceEvent
}
