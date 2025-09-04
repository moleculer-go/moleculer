package tcp

import (
	"math/rand"
	"net"
	"time"

	"github.com/moleculer-go/moleculer"
	payloadPkg "github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"
)

func (transporter *TCPTransporter) startGossipTimer() {
	// Ensure GossipPeriod is at least 1 second to avoid panic
	gossipPeriod := transporter.options.GossipPeriod
	if gossipPeriod <= 0 {
		gossipPeriod = 2 // Default to 2 seconds
		transporter.logger.Warn("Invalid GossipPeriod, using default value of 2 seconds")
	}

	transporter.gossipTimer = time.NewTicker(time.Second * time.Duration(gossipPeriod))
	go func() {
		for range transporter.gossipTimer.C {
			transporter.sendGossipRequest("")
		}
	}()
}

func (transporter *TCPTransporter) getRandomNode(nodes []moleculer.Node) moleculer.Node {
	if len(nodes) == 0 {
		return nil
	}
	nonLocalNodes := []moleculer.Node{}
	for _, node := range nodes {
		if !node.IsLocal() {
			nonLocalNodes = append(nonLocalNodes, node)
		}
	}
	if len(nonLocalNodes) == 0 {
		return nil
	}
	return nonLocalNodes[rand.Intn(len(nonLocalNodes))]
}

func (transporter *TCPTransporter) sendGossip(nodeID string, payload moleculer.Payload) {
	node := transporter.registry.GetNodeByID(nodeID)
	if !node.IsLocal() {
		transporter.logger.Trace("GOSSIP SEND - About to send gossip to nodeID:", nodeID, "payload size:", len(util.PrettyPrintMap(payload.RawMap())))
		transporter.Publish(msgTypeToCommand(PACKET_GOSSIP_REQ), node.GetID(), payload)
		transporter.logger.Trace("GOSSIP SEND - Gossip sent successfully to nodeID:", nodeID)
	}
}

func (transporter *TCPTransporter) sendGossipHello(nodeID string) {
	localNode := transporter.registry.GetLocalNode()
	payload := payloadPkg.Empty()
	payload.Add("sender", localNode.GetID())
	payload.Add("host", localNode.GetHost())
	payload.Add("port", localNode.GetPort())

	transporter.Publish(msgTypeToCommand(PACKET_GOSSIP_HELLO), nodeID, payload)
}

func (transporter *TCPTransporter) onGossipHello(fromAddrss string, payload moleculer.Payload) {
	sender := payload.Get("sender").String()
	port := payload.Get("port").Int()
	hostname := payload.Get("host").String()

	transporter.logger.Debug("Received gossip hello from sender: ", sender, "ipAddress: ", fromAddrss, " hostname: ", hostname)

	node := transporter.registry.GetNodeByID(sender)
	if node == nil {
		transporter.logger.Debug("Unknown node. Register as offline node - sender: ", sender)
		// Extract just the IP address from the full address
		ipAddress, _, err := net.SplitHostPort(fromAddrss)
		if err != nil {
			// If splitting fails, use the full address as fallback
			ipAddress = fromAddrss
		}
		node = transporter.registry.AddOfflineNode(sender, hostname, ipAddress, port)
	}
	if node.GetUdpAddress() == "" {
		// Extract just the IP address from the full address
		host, _, err := net.SplitHostPort(fromAddrss)
		if err != nil {
			// If splitting fails, use the full address as fallback
			host = fromAddrss
		}
		node.UpdateInfo(map[string]interface{}{
			"udpAddress": host,
		})
	}
	node.Available()
	transporter.logger.Trace("will send a gossip response to node: " + sender)
	transporter.onGossipRequest(payloadPkg.Empty().Add("sender", sender))
}

func (transporter *TCPTransporter) sendGossipRequest(target string) {
	transporter.logger.Trace("Sending gossip request")
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
		var targetNode moleculer.Node
		if target != "" {
			transporter.logger.Debug("target node is specified target:", target)
			targetNode = transporter.registry.GetNodeByID(target)
		}
		if targetNode == nil {
			targetNode = transporter.getRandomNode(onlineNodes)
			if targetNode != nil {
				transporter.logger.Trace("selected a random node to send gossip request - nodeId:", targetNode.GetID())
			} else {
				transporter.logger.Trace("could not select a random node  - online nodes size:", len(onlineNodes))
			}
		}
		if targetNode != nil {
			transporter.sendGossip(targetNode.GetID(), payload)
		} else {
			transporter.logger.Debug("No target node found for gossip request - target param:", target, " online nodes size:", len(onlineNodes))
		}
	}

	if len(offlineNodes) > 0 {
		ratio := float64(len(offlineNodes)) / float64(len(onlineNodes)+1)
		if ratio >= 1 || rand.Float64() < ratio {
			randomNode := transporter.getRandomNode(offlineNodes)
			transporter.sendGossip(randomNode.GetID(), payload)
		}
	}
}

func (transporter *TCPTransporter) onGossipRequest(payload moleculer.Payload) {
	sender := payload.Get("sender").String()

	transporter.logger.Debug("onGossipRequest() - sender:" + sender)

	onlineResponse := map[string]interface{}{}
	offlineResponse := map[string]interface{}{}

	onlineMap := payload.Get("online")
	offlineMap := payload.Get("offline")

	transporter.registry.ForEachNode(func(node moleculer.Node) bool {
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
				transporter.logger.Trace("received for: "+node.GetID(), " seq: ", seq, " cpuSeq: ", cpuSeq, " cpu: ", cpu)
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
				transporter.logger.Debug("Node is available - send back the node info and cpu, cpuSed to "+node.GetID(), " seq: ", seq, " cpuSeq: ", cpuSeq, " cpu: ", cpu, " info: ", util.PrettyPrintMap(info))
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
					onlineResponse[node.GetID()] = []interface{}{node.ExportAsMap(), node.GetCpuSequence(), node.GetCpu()}
					transporter.logger.Debug("CPU info sent back to " + node.GetID())
				}
			} else {
				// We know it as offline. We do nothing, because we'll request it and we'll receive its INFO.

				return true
			}
		}
		return true
	})

	localNode := transporter.registry.GetLocalNode()
	localNode.UpdateMetrics()
	info := localNode.ExportAsMap()
	onlineResponse[localNode.GetID()] = []interface{}{info, localNode.GetCpuSequence(), localNode.GetCpu()}

	if len(onlineResponse) > 0 || len(offlineResponse) > 0 {
		sender := payload.Get("sender").String()
		responsePayload := payloadPkg.
			Empty().
			Add("sender", transporter.registry.GetLocalNode().GetID())
		if len(onlineResponse) > 0 {
			responsePayload.Add("online", onlineResponse)
		}
		if len(offlineResponse) > 0 {
			responsePayload.Add("offline", offlineResponse)
		}
		transporter.logger.Trace("Gossip response sent to "+sender, " payload:", util.PrettyPrintMap(responsePayload.RawMap()))
		transporter.Publish(msgTypeToCommand(PACKET_GOSSIP_RES), sender, responsePayload)
	} else {
		transporter.logger.Trace("No response sent to " + sender)
	}

}

func (transporter *TCPTransporter) onGossipResponse(payload moleculer.Payload) {
	sender := payload.Get("sender").String()

	transporter.logger.Trace("Received gossip response from "+sender, " payload:", util.PrettyPrintMap(payload.RawMap()))

	online := payload.Get("online")
	offline := payload.Get("offline")

	if online.Exists() {
		transporter.logger.Trace("Received online info from nodeID: " + sender)
		online.ForEach(func(key interface{}, value moleculer.Payload) bool {
			nodeID, ok := key.(string)
			transporter.logger.Debug("Received online info from nodeID: " + nodeID)
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
			transporter.logger.Trace("Parsing gossip response row for nodeID:", nodeID, "row:", util.PrettyPrintMap(row))
			info, cpu, cpuSeq := parseGossipResponse(row)
			transporter.logger.Trace("Parsed gossip response - info:", util.PrettyPrintMap(info.RawMap()), "cpu:", cpu, "cpuSeq:", cpuSeq)

			if info != nil && (node == nil || node.GetSequence() < info.Get("seq").Int64()) {
				transporter.logger.Debug("If we don't know it, or know, but has smaller seq, update 'info'")
				currentSeq := int64(0)
				if node != nil {
					currentSeq = node.GetSequence()
				}
				transporter.logger.Trace("Current node sequence:", currentSeq, "incoming sequence:", info.Get("seq").Int64())
				info = info.Add("sender", sender)
				transporter.logger.Trace("Calling RemoteNodeInfoReceived with info:", util.PrettyPrintMap(info.RawMap()))
				transporter.registry.RemoteNodeInfoReceived(info)
				transporter.logger.Trace("RemoteNodeInfoReceived completed for nodeID:", nodeID)
			} else {
				currentSeq := int64(0)
				if node != nil {
					currentSeq = node.GetSequence()
				}
				incomingSeq := int64(0)
				if info != nil {
					incomingSeq = info.Get("seq").Int64()
				}
				transporter.logger.Trace("Skipping node update - info is nil or sequence not newer. node:", node != nil, "info:", info != nil, "currentSeq:", currentSeq, "incomingSeq:", incomingSeq)
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

func parseGossipResponse(row []moleculer.Payload) (info moleculer.Payload, cpu int64, cpuSeq int64) {
	cpuSeq = -1
	cpu = -1
	if len(row) == 1 {
		info = row[0]
	}
	if len(row) == 2 {
		cpuSeq = row[0].Int64()
		cpu = row[1].Int64()
	}
	if len(row) == 3 {
		info = row[0]
		cpuSeq = row[1].Int64()
		cpu = row[2].Int64()
	}
	return info, cpu, cpuSeq
}

func isGossipMessage(msgType byte) bool {
	return msgType == PACKET_GOSSIP_REQ || msgType == PACKET_GOSSIP_RES || msgType == PACKET_GOSSIP_HELLO
}
