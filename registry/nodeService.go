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
	return service.FromSchema(moleculer.ServiceSchema{
		Name: "$node",
		Actions: []moleculer.Action{
			moleculer.Action{
				Name: "events",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					onlyLocal := params.Get("onlyLocal").Exists() && params.Get("onlyLocal").Bool()
					onlyAvailable := params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool()
					skipInternal := params.Get("skipInternal").Exists() && params.Get("skipInternal").Bool()
					withEndpoints := params.Get("withEndpoints").Exists() && params.Get("withEndpoints").Bool()

					result := make([]map[string]interface{}, 0)
					for name, entries := range registry.events.listByName() {
						has := func(check func(nodeID string) bool) bool {
							for _, item := range entries {
								if check(item.service.NodeID()) {
									return true
								}
							}
							return false
						}
						endpoints := func() []map[string]interface{} {
							list := make([]map[string]interface{}, 0)
							for _, item := range entries {
								nodeID := item.service.NodeID()
								list = append(list, map[string]interface{}{
									"nodeID":    nodeID,
									"available": isAvailable(nodeID),
								})
							}
							return list
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
							"group":     entries[0].event.Group(),
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
							list := make([]map[string]interface{}, 0)
							for _, item := range entries {
								list = append(list, map[string]interface{}{
									"nodeID":    item.service.NodeID(),
									"available": isAvailable(item.service.NodeID()),
								})
							}
							return list
						}
						context.Logger().Debug("$node.actions name: ", name)
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
						context.Logger().Debug("$node.actions name: ", name, " contents: ", item)
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
						withEndpoints bool
						withEvents    bool `required:"true"`
						skipInternal  bool
						onlyAvailable bool
						onlyLocal     bool
						result        []map[string]interface{}
					}{withActions: true},
				},
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Debug("$node.services params: ", params.Value())

					//TODO simplify this by removing the .Exists() check once we have action schema validation and default values assignment.
					skipInternal := params.Get("skipInternal").Exists() && params.Get("skipInternal").Bool()
					onlyAvailable := params.Get("onlyAvailable").Exists() && params.Get("onlyAvailable").Bool()
					onlyLocal := params.Get("onlyLocal").Exists() && params.Get("onlyLocal").Bool()
					withActions := params.Get("withActions").Exists() && params.Get("withActions").Bool()
					withEvents := params.Get("withEvents").Exists() && params.Get("withEvents").Bool()
					withEndpoints := params.Get("withEndpoints").Exists() && params.Get("withEndpoints").Bool()

					//ISSUE: is returning duplicate services. -> printer which exists in 2 brokers.. local and remote.
					result := make([]map[string]interface{}, 0)
					for name, entries := range registry.services.listByName() {
						has := func(check func(nodeID string) bool) bool {
							for _, item := range entries {
								if check(item.nodeID) {
									return true
								}
							}
							return false
						}
						endpoints := func() []map[string]interface{} {
							list := make([]map[string]interface{}, 0)
							for _, item := range entries {
								list = append(list, map[string]interface{}{
									"nodeID":    item.nodeID,
									"available": isAvailable(item.nodeID),
								})
							}
							return list
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
						smap := entries[0].service.AsMap()
						smap["available"] = has(isAvailable)
						smap["hasLocal"] = has(isLocal)
						smap = without(smap, "nodeID")
						if withEndpoints {
							smap["endpoints"] = endpoints()
						}
						if !withActions {
							smap = without(smap, "actions")
						}
						if !withEvents {
							smap = without(smap, "events")
						}
						result = append(result, smap)
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
	}, registry.broker)
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
