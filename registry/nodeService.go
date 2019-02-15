package registry

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

// createNodeService create the local node service -> $node.
func createNodeService(registry *ServiceRegistry) *service.Service {
	return service.FromSchema(moleculer.Service{
		Name: "$node",
		Actions: []moleculer.Action{
			moleculer.Action{
				Name: "events",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					return nil
				},
			},
			moleculer.Action{
				Name: "actions",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					return nil
				},
			},
			moleculer.Action{
				Name: "services",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					withActions := false
					if params.Get("withActions").Exists() && params.Get("withActions").Bool() {
						withActions = true
					}

					skipInternal := false
					if params.Get("skipInternal").Exists() && params.Get("skipInternal").Bool() {
						skipInternal = true
					}

					context.Logger().Debug("$node.services withActions: ", withActions, " skipInternal:", skipInternal)

					result := make([]map[string]interface{}, 0)
					return result
				},
			},
			moleculer.Action{
				Name: "list",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					removeServices := true
					if params.Get("withServices").Exists() && params.Get("withServices").Bool() {
						removeServices = false
					}

					onlyAvailable := false
					if params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool() {
						onlyAvailable = true
					}

					nodes := registry.nodes.list()
					result := make([]map[string]interface{}, 0)
					for _, node := range nodes {
						if onlyAvailable && !node.IsAvailable() {
							continue
						}
						maps := node.ExportAsMap()
						if removeServices {
							delete(maps, "services")
						}
						result = append(result, maps)
					}

					return result
				},
			},
		},
	}, registry.logger)
}
