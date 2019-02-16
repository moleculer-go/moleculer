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
	without := func(in map[string]interface{}, key string) map[string]interface{} {
		delete(in, key)
		return in
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
				Name:        "actions",
				Description: "Find and return a list of actions in the registry of this service broker.",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					onlyLocal := params.Get("onlyLocal").Exists() && params.Get("onlyLocal").Bool()
					onlyAvailable := params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool()
					skipInternal := params.Get("skipInternal").Exists() && params.Get("skipInternal").Bool()
					withEndpoints := params.Get("withEndpoints").Exists() && params.Get("withEndpoints").Bool()

					result := make([]map[string]interface{}, 0)
					for name, entries := range registry.actions.listByName() {
						has := func(check func(nodeID string) bool) bool {
							for _, item := range entries {
								if check(item.service.NodeID()) {
									return true
								}
							}
							return false
						}
						endpoints := func() []map[string]interface{} {
							edps := make([]map[string]interface{}, 0)
							for _, item := range entries {
								edps = append(edps, map[string]interface{}{
									"nodeID":    item.service.NodeID(),
									"available": isAvailable(item.service.NodeID()),
								})
							}
							return edps
						}
						if onlyLocal && !has(isLocal) {
							continue
						}
						if onlyAvailable && !has(isAvailable) {
							continue
						}
						if skipInternal && strings.Index(name, "$") == 0 {
							continue
						}
						item := map[string]interface{}{
							"name":      name,
							"count":     len(entries),
							"hasLocal":  has(isLocal),
							"available": has(isAvailable),
						}
						if withEndpoints {
							item["endpoints"] = endpoints()
						}
						result = append(result, item)
					}

					return result
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
					context.Logger().Debug("len(services): ", len(services))

					result := make([]map[string]interface{}, 0)
					for _, service := range services {
						context.Logger().Debug("checking service -> : ", (*service))
						if isLocal(service.NodeID()) && skipInternal && strings.Index(service.Name(), "$") == 0 {
							context.Logger().Debug("skiping service -> : ", (*service), " flag skipInternal: ", skipInternal)
							continue
						}
						if !isLocal(service.NodeID()) && strings.Index(service.Name(), "$") == 0 {
							context.Logger().Debug("skiping service -> : ", (*service), " flag internal service of a remote node.")
							continue
						}
						if onlyLocal && !isLocal(service.NodeID()) {
							context.Logger().Debug("skiping service -> : ", (*service), " flag onlyLocal: ", onlyLocal)
							continue
						}
						if onlyAvailable && !isAvailable(service.NodeID()) {
							context.Logger().Debug("skiping service -> : ", (*service), " flag onlyAvailable: ", onlyAvailable)
							continue
						}
						maps := service.AsMap()
						maps["available"] = isAvailable(service.NodeID())
						if !withActions {
							maps = without(maps, "actions")
						}
						if !withEvents {
							maps = without(maps, "events")
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
						if withServices {
							if !isLocal(node.GetID()) {
								maps["services"] = filterLocal(maps["services"].([]map[string]interface{}))
							}
						} else {
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

func filterLocal(in []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0)
	for _, item := range in {
		if strings.Index(item["name"].(string), "$") == 0 {
			continue
		}
		out = append(out, item)
	}
	return out
}
