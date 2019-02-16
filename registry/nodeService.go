package registry

import (
	"strings"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

// createNodeService create the local node service -> $node.
func createNodeService(registry *ServiceRegistry) *service.Service {
	isAvailable := func(nodeID string) bool {
		node, exists := registry.nodes.findNode(nodeID)
		return exists && node.IsAvailable()
	}
	isLocal := func(nodeID string) bool {
		return registry.localNode.GetID() == nodeID
	}
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
				Name:        "services",
				Description: "Find and return a list of services in the registry of this service broker.",
				Schema: moleculer.ObjectSchema{
					struct {
						withActions   bool
						withEvents    bool `required:"true"`
						skipInternal  bool
						onlyAvailable bool
						onlyLocal     bool
						result        []map[string]interface{}
					}{withActions: true},
				},
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					//TODO remove the .Exists() check once we have action schema validation and default values assignment.
					withActions := params.Get("withActions").Exists() && params.Get("withActions").Bool()
					withEvents := params.Get("withEvents").Exists() && params.Get("withEvents").Bool()
					skipInternal := params.Get("skipInternal").Exists() && params.Get("skipInternal").Bool()
					onlyAvailable := params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool()
					onlyLocal := params.Get("onlyLocal").Exists() && params.Get("onlyLocal").Bool()

					context.Logger().Debug("$node.services params: ", params.Value())
					services := registry.services.list()
					result := make([]map[string]interface{}, 0)
					for _, service := range services {
						if skipInternal && strings.Index(service.Name(), "$") == 0 {
							continue
						}
						if onlyLocal && !isLocal(service.NodeID()) {
							continue
						}
						if onlyAvailable && !isAvailable(service.NodeID()) {
							continue
						}
						maps := service.AsMap()
						if !withActions {
							delete(maps, "actions")
						}
						if !withEvents {
							delete(maps, "events")
						}
						result = append(result, maps)
					}
					return result
				},
			},
			moleculer.Action{
				Name:        "list",
				Description: "Find and return a list of nodes in the registry of this service broker.",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					withServices := params.Get("withServices").Exists() && params.Get("withServices").Bool()
					onlyAvailable := params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool()

					nodes := registry.nodes.list()
					result := make([]map[string]interface{}, 0)
					for _, node := range nodes {
						if onlyAvailable && !node.IsAvailable() {
							continue
						}
						maps := node.ExportAsMap()
						if !withServices {
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
