package registry

import (
	"strings"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

var startedTime = time.Now()

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
			{
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
			{
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
			{
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
			{
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
			/* FIXME: currently returns incomplete payload */
			{
				Name:	"health",
				Description: "Return health status of local node including transit, os, cpu, memory, process, network, client information.",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					/* TODO: map as JSON which follows standard structure
					{ cpu:
					   { load1: 1.802734375,
						 load5: 1.8603515625,
						 load15: 1.82666015625,
						 cores: 16,
						 utilization: 11 },
					  mem:
					   { free: 886513664,
						 total: 68719476736,
						 percent: 1.2900471687316895 },
					  os:
					   { uptime: 1507874,
						 type: 'Darwin',
						 release: '18.5.0',
						 hostname: 'imac.local',
						 arch: 'x64',
						 platform: 'darwin',
						 user:
						  { uid: 501,
							gid: 20,
							username: 'dehypnosis',
							homedir: '/Users/dehypnosis',
							shell: '/bin/zsh' } },
					  process:
					   { pid: 67218,
						 memory:
						  { rss: 60497920,
							heapTotal: 32743424,
							heapUsed: 27003144,
							external: 104085 },
						 uptime: 80.434,
						 argv:
						  [ '/usr/local/bin/node',
							'/usr/local/bin/moleculer',
							'connect',
							'nats://dev.nats.svc.cluster.local:4222' ] },
					  client: { type: 'nodejs', version: '0.13.8', langVersion: 'v8.16.0' },
					  net: { ip: [ '222.107.184.34', '172.30.1.7' ] },
					  transit:
					   { stat:
						  { packets:
							 { sent: { count: 28, bytes: 13409 },
							   received: { count: 158, bytes: 199426 } } } },
					  time:
					   { now: 1556737026387,
						 iso: '2019-05-01T18:57:06.387Z',
						 utc: 'Wed, 01 May 2019 18:57:06 GMT' } }
					 */
					nodeInfo := registry.localNode.ExportAsMap()
					return map[string]interface{}{
						"cpu": map[string]interface{}{
						},
						"mem": map[string]interface{}{
						},
						"os": map[string]interface{}{
						},
						"process": map[string]interface{}{
							"uptime": time.Since(startedTime),
						},
						"client": nodeInfo["client"],
						"net": map[string]interface{}{
							"ip": nodeInfo["ipList"],
						},
						"transit": map[string]interface{}{
							// TODO
						},
						"time": map[string]interface{}{
							// TODO
						},
					}
				},
			},
			/* TODO: support $node.options */
			{
				Name: "options",
				Description: "Return broker configuration of local node.",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					/* TODO: map as JSON which follows standard structure
					{ logger: true,
					  transporter: 'nats://dev.nats.svc.cluster.local:4222',
					  nodeID: 'cli-imac.local-67218',
					  namespace: '',
					  logLevel: null,
					  logFormatter: 'default',
					  logObjectPrinter: null,
					  requestTimeout: 0,
					  retryPolicy:
					   { enabled: false,
						 retries: 5,
						 delay: 100,
						 maxDelay: 1000,
						 factor: 2,
						 check: [Function: check] },
					  maxCallLevel: 0,
					  heartbeatInterval: 5,
					  heartbeatTimeout: 15,
					  tracking: { enabled: false, shutdownTimeout: 5000 },
					  disableBalancer: false,
					  registry: { strategy: 'RoundRobin', preferLocal: true },
					  circuitBreaker:
					   { enabled: false,
						 threshold: 0.5,
						 windowTime: 60,
						 minRequestCount: 20,
						 halfOpenTime: 10000,
						 check: [Function: check] },
					  bulkhead: { enabled: false, concurrency: 10, maxQueueSize: 100 },
					  transit:
					   { maxQueueSize: 50000,
						 packetLogFilter: [],
						 disableReconnect: false,
						 disableVersionCheck: false },
					  cacher: null,
					  serializer: null,
					  validation: true,
					  validator: null,
					  metrics: false,
					  metricsRate: 1,
					  internalServices: true,
					  internalMiddlewares: true,
					  hotReload: false,
					  middlewares: null,
					  replCommands: null }
					 */
					// from registry.broker.Config ?
					return map[string]interface{}{}
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
