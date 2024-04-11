package tcp

import (
	"math/rand"
	"time"

	"github.com/moleculer-go/moleculer"
	payloadPkg "github.com/moleculer-go/moleculer/payload"
)

func (transporter *TCPTransporter) startGossipTimer() {
	transporter.gossipTimer = time.NewTicker(time.Second * time.Duration(transporter.options.GossipPeriod))
	go func() {
		for range transporter.gossipTimer.C {
			transporter.sendGossipRequest()
		}
	}()
}

func (transporter *TCPTransporter) sendGossipRequest() {

	transporter.logger.Debug("Sending gossip request")

	node := transporter.registry.GetLocalNode()
	node.UpdateMetrics()

	onlineResponse := map[string]interface{}{}
	offlineResponse := map[string]interface{}{}
	onlineNodes := []moleculer.Node{}
	offlineNodes := []moleculer.Node{}

	transporter.registry.ForEachNode(func(node moleculer.Node) bool {
		if node.IsAvailable() {
			onlineResponse[node.GetID()] = []interface{}{node.GetSequence(), node.GetCpuSequence(), node.GetCpu()}
			onlineNodes = append(onlineNodes, node)
		} else {
			offlineResponse[node.GetID()] = node.GetSequence()
			offlineNodes = append(offlineNodes, node)
		}
		return true
	})

	payload := payloadPkg.Empty()
	payload.Add("sender", node.GetID())
	if len(onlineResponse) > 0 {
		payload.Add("online", onlineResponse)
	}
	if len(offlineResponse) > 0 {
		payload.Add("offline", offlineResponse)
	}

	if len(onlineResponse) > 0 {
		transporter.sendGossipToRandomEndpoint(payload, onlineNodes)
	}

	if len(offlineNodes) > 0 {
		ratio := float64(len(offlineNodes)) / float64(len(onlineNodes)+1)
		if ratio >= 1 || rand.Float64() < ratio {
			transporter.sendGossipToRandomEndpoint(payload, offlineNodes)
		}
	}
}

func (transporter *TCPTransporter) sendGossipToRandomEndpoint(payload moleculer.Payload, nodes []moleculer.Node) {
	if len(nodes) == 0 {
		return
	}
	node := nodes[rand.Intn(len(nodes))]
	if !node.IsLocal() {
		transporter.logger.Debug("Sending gossip request to " + node.GetID())
		transporter.Publish(msgTypeToCommand(PACKET_GOSSIP_REQ), node.GetID(), payload)
	}
}

func (transporter *TCPTransporter) onGossipHello(fromAddrss string, msgBytes *[]byte) {
	payload := transporter.serializer.BytesToPayload(msgBytes)
	sender := payload.Get("sender").String()
	port := payload.Get("port").Int()
	hostname := payload.Get("host").String()

	transporter.logger.Debug("Received gossip hello from sender: ", sender, "ipAddress: ", fromAddrss, " hostname: ", hostname)

	node := transporter.registry.GetNodeByID(sender)
	if node == nil {
		transporter.logger.Debug("Unknown node. Register as offline node - sender: ", sender)
		node = transporter.registry.AddOfflineNode(sender, hostname, fromAddrss, port)
	}
	if node.GetUdpAddress() == "" {
		node.UpdateInfo(map[string]interface{}{
			"udpAddress": fromAddrss,
		})
	}
}

func (transporter *TCPTransporter) onGossipRequest(msgBytes *[]byte) {
	payload := transporter.serializer.BytesToPayload(msgBytes)
	sender := payload.Get("sender").String()

	transporter.logger.Debug("Received gossip request from " + sender)

	onlineResponse := map[string]interface{}{}
	offlineResponse := map[string]interface{}{}

	transporter.registry.ForEachNode(func(node moleculer.Node) bool {

		onlineMap := payload.Get("online")
		offlineMap := payload.Get("offline")
		var seq int64 = 0
		var cpuSeq int64 = 0
		var cpu int64 = 0
		var offline moleculer.Payload
		var online moleculer.Payload

		if offlineMap.Exists() {
			offline = offlineMap.Get(node.GetID())
			if offline.Exists() {
				transporter.logger.Debug("received seq for " + node.GetID())
				seq = offline.Int64()
			}
		}
		if onlineMap.Exists() {
			online = onlineMap.Get(node.GetID())
			if online.Exists() {
				transporter.logger.Debug("received seq, cpuSeq, cpu for " + node.GetID())
				seq = online.Get("seq").Int64()
				cpuSeq = online.Get("cpuSeq").Int64()
				cpu = online.Get("cpu").Int64()
			}
		}

		if seq != 0 && seq < node.GetSequence() {
			transporter.logger.Debug("We have newer info or requester doesn't know it")
			if node.IsAvailable() {
				info := node.ExportAsMap()
				onlineResponse[node.GetID()] = []interface{}{info, node.GetCpuSequence(), node.GetCpu()}
				transporter.logger.Debug("Node is available - send back the node info and cpu, cpuSed to " + node.GetID())
			} else {
				offlineResponse[node.GetID()] = node.GetSequence()
				transporter.logger.Debug("Node is offline - send back the seq to " + node.GetID())
			}
			return true
		}

		if offline != nil && offline.Exists() {
			transporter.logger.Debug("Requester said it is OFFLINE")
			if !node.IsAvailable() {
				transporter.logger.Debug("We also know it as offline - update the seq")
				if seq > node.GetSequence() {
					node.UpdateInfo(map[string]interface{}{
						"seq": seq,
					})
				}
				return true
			}

			if !node.IsLocal() {
				transporter.logger.Debug("our current state for it is online - change it to offline and update seq - nodeID:", node.GetID(), "seq:", seq)
				// We know it is online, so we change it to offline
				transporter.registry.DisconnectNode(node.GetID())

				// Update the 'seq' to the received value
				node.UpdateInfo(map[string]interface{}{
					"seq": seq,
				})
				return true
			}

			if node.IsLocal() {
				transporter.logger.Debug("msg is about the Local node - update the seq and send back info, cpu and cpuSeq")
				// Update the 'seq' to the received value
				node.UpdateInfo(map[string]interface{}{
					"seq": seq + 1,
				})
				onlineResponse[node.GetID()] = []interface{}{node.ExportAsMap(), node.GetCpuSequence(), node.GetCpu()}
				return true
			}
		}

		if online != nil && online.Exists() {
			// Requester said it is ONLINE
			if node.IsAvailable() {
				if cpuSeq > node.GetCpuSequence() {
					// We update CPU info
					node.UpdateInfo(map[string]interface{}{
						"cpu":    cpu,
						"cpuSeq": cpuSeq,
					})
					transporter.logger.Debug("CPU info updated for " + node.GetID())
				} else if cpuSeq < node.GetCpuSequence() {
					// We have newer info, send back
					onlineResponse[node.GetID()] = []interface{}{node.GetCpuSequence(), node.GetCpu()}
					transporter.logger.Debug("CPU info sent back to " + node.GetID())
				}
			} else {
				// We know it as offline. We do nothing, because we'll request it and we'll receive its INFO.
				return true
			}
		}
		return true
	})

	if len(onlineResponse) > 0 || len(offlineResponse) > 0 {
		sender := payload.Get("sender").String()
		transporter.Publish(msgTypeToCommand(PACKET_GOSSIP_RES), sender, payloadPkg.Empty().Add("online", onlineResponse).Add("offline", offlineResponse))
		transporter.logger.Debug("Gossip response sent to " + sender)
	} else {
		transporter.logger.Debug("No response sent to " + payload.Get("sender").String())
	}

}

func (transporter *TCPTransporter) onGossipResponse(msgBytes *[]byte) {
	payload := transporter.serializer.BytesToPayload(msgBytes)
	sender := payload.Get("sender").String()

	transporter.logger.Debug("Received gossip response from " + sender)

	online := payload.Get("online")
	offline := payload.Get("offline")

	if online.Exists() {
		transporter.logger.Debug("Received online info from nodeID: " + sender)
		online.ForEach(func(key interface{}, value moleculer.Payload) bool {
			nodeID, ok := key.(string)
			if !ok {
				transporter.logger.Error("Error parsing online nodeID")
				return true
			}
			node := transporter.registry.GetNodeByID(nodeID)
			if node != nil && node.IsLocal() {
				transporter.logger.Debug("Received info about the local node - ignore it")
				return true
			}
			row := online.Get(nodeID).Array()
			info, cpu, cpuSeq := parseGossipResponse(row)

			if info != nil && (node != nil || node.GetSequence() < info["seq"].(int64)) {
				transporter.logger.Debug("If we don't know it, or know, but has smaller seq, update 'info'")
				info["sender"] = sender
				transporter.registry.RemoteNodeInfoReceived(payloadPkg.New(info))
			}
			if node != nil && cpuSeq > node.GetCpuSequence() {
				transporter.logger.Debug("If we know it and has smaller cpuSeq, update 'cpu'")
				node.HeartBeat(map[string]interface{}{
					"cpu":    cpu,
					"cpuSeq": cpuSeq,
				})
			}
			return true
		})
	}

	if offline.Exists() {
		transporter.logger.Debug("Received offline info from nodeID: " + sender)
		offline.ForEach(func(key interface{}, value moleculer.Payload) bool {
			nodeID, ok := key.(string)
			if !ok {
				transporter.logger.Error("Error parsing offline nodeID")
				return true
			}
			node := transporter.registry.GetNodeByID(nodeID)
			if node != nil && node.IsLocal() {
				transporter.logger.Debug("Received info about the local node - ignore it")
				return true
			}
			if node == nil {
				return true
			}

			seq := offline.Get(nodeID).Int64()

			if node.GetSequence() < seq {
				if node.IsAvailable() {
					transporter.logger.Debug("Node is online, will change it to offline")
					transporter.registry.DisconnectNode(nodeID)
				}
				node.UpdateInfo(map[string]interface{}{
					"seq": seq,
				})
			}
			return true
		})
	}
}

func parseGossipResponse(row []moleculer.Payload) (info map[string]interface{}, cpu int64, cpuSeq int64) {
	cpuSeq = -1
	cpu = -1
	if len(row) == 1 {
		info = row[0].RawMap()
	}
	if len(row) == 2 {
		cpuSeq = row[0].Int64()
		cpu = row[1].Int64()
	}
	if len(row) == 3 {
		info = row[0].RawMap()
		cpuSeq = row[1].Int64()
		cpu = row[2].Int64()
	}
	return info, cpu, cpuSeq
}

func isGossipMessage(msgType byte) bool {
	return msgType == PACKET_GOSSIP_REQ || msgType == PACKET_GOSSIP_RES || msgType == PACKET_GOSSIP_HELLO
}
