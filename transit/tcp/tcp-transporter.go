package tcp

import (
	"time"

	"github.com/moleculer-go/moleculer"
	payloadPkg "github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"

	log "github.com/sirupsen/logrus"
)

type TCPTransporter struct {
	options   TCPOptions
	tcpReader *TcpReader
	tcpWriter *TcpWriter
	udpServer *UdpServer
	registry  moleculer.Registry

	logger *log.Entry

	validateMsg transit.ValidateMsgFunc
	serializer  serializer.Serializer
	handlers    map[string][]transit.TransportHandler
}

type TCPOptions struct {

	// Enable UDP discovery
	UdpDiscovery bool
	// Reusing UDP server socket
	UdpReuseAddr bool

	// UDP port
	UdpPort int
	// UDP bind address (if null, bind on all interfaces)
	UdpBindAddress string
	// UDP sending period (seconds)
	UdpPeriod time.Duration

	UdpMaxDiscovery int

	// Multicast address.
	UdpMulticast string
	// Multicast TTL setting
	UdpMulticastTTL int

	// Send broadcast (Boolean, String, Array<String>)
	UdpBroadcast      []string
	UdpBroadcastAddrs []string
	// TCP server port. Null or 0 means random port
	Port int
	// Static remote nodes address list (when UDP discovery is not available)
	Urls []string
	// Use hostname as preffered connection address
	UseHostname bool

	// Gossip sending period in seconds
	GossipPeriod int
	// Maximum enabled outgoing connections. If reach, close the old connections
	MaxConnections int
	// Maximum TCP packet size
	MaxPacketSize int

	Prefix      string
	NodeId      string
	Logger      *log.Entry
	Serializer  serializer.Serializer
	ValidateMsg transit.ValidateMsgFunc
}

func CreateTCPTransporter(options TCPOptions) TCPTransporter {
	transport := TCPTransporter{options: options, logger: options.Logger}
	return transport
}

func (transporter *TCPTransporter) Connect(registry moleculer.Registry) chan error {
	transporter.registry = registry
	transporter.logger.Info("TCP Transported Connect()")
	endChan := make(chan error)
	go func() {
		transporter.startTcpServer()
		transporter.startUDPServer()
		transporter.startGossip()
		endChan <- nil
	}()
	return endChan
}

type MessageType int

const (
	PACKET_EVENT        = 1
	PACKET_REQUEST      = 2
	PACKET_RESPONSE     = 3
	PACKET_PING         = 4
	PACKET_PONG         = 5
	PACKET_GOSSIP_REQ   = 6
	PACKET_GOSSIP_RES   = 7
	PACKET_GOSSIP_HELLO = 8
)

func (transporter *TCPTransporter) onTcpMessage(fromAddrss string, msgType int, msgBytes *[]byte) {
	switch msgType {
	case PACKET_GOSSIP_HELLO:
		transporter.onGossipHello(fromAddrss, msgBytes)
	case PACKET_GOSSIP_REQ:
		transporter.onGossipRequest(msgBytes)
	case PACKET_GOSSIP_RES:
		transporter.onGossipResponse(msgBytes)
	default:
		transporter.incomingMessage(msgType, msgBytes)
	}
}

func (transporter *TCPTransporter) onGossipHello(fromAddrss string, msgBytes *[]byte) {
	packet := transporter.serializer.BytesToPayload(msgBytes)
	payload := packet.Get("payload")
	nodeID := payload.Get("sender").String()

	node := transporter.registry.GetNodeByID(nodeID)
	if node == nil {
		// Unknown node. Register as offline node
		node = transporter.registry.AddOfflineNode(nodeID, payload.Get("host").String(), payload.Get("port").Int())
	}
	if node.GetUdpAddress() == "" {
		node.UpdateInfo(nodeID, map[string]interface{}{
			"udpAddress": fromAddrss,
		})
	}
}

func (transporter *TCPTransporter) onGossipRequest(msgBytes *[]byte) {
	packet := transporter.serializer.BytesToPayload(msgBytes)
	payload := packet.Get("payload")

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
					node.UpdateInfo(node.GetID(), map[string]interface{}{
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
				node.UpdateInfo(node.GetID(), map[string]interface{}{
					"seq": seq,
				})
				return true
			}

			if node.IsLocal() {
				transporter.logger.Debug("msg is about the Local node - update the seq and send back info, cpu and cpuSeq")
				// Update the 'seq' to the received value
				node.UpdateInfo(node.GetID(), map[string]interface{}{
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
					node.UpdateInfo(node.GetID(), map[string]interface{}{
						"cpu":    cpu,
						"cpuSeq": cpuSeq,
					})
					transporter.logger.Debug("CPU info updated for " + node.GetID())
				} else if cpuSeq < node.GetCpuSequence() {
					// We have newer info, send back
					//TODO check where we process this to see if we handle this array correctly
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
	packet := transporter.serializer.BytesToPayload(msgBytes)
	payload := packet.Get("payload")
	sender := payload.Get("sender").String()

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
				node.UpdateInfo(nodeID, map[string]interface{}{
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

func msgTypeToCommand(msgType int) string {
	switch msgType {
	case PACKET_EVENT:
		return "EVENT"
	case PACKET_REQUEST:
		return "REQ"
	case PACKET_RESPONSE:
		return "RES"
	// case PACKET_DISCOVER:
	// 	return "DISCOVER"
	// case PACKET_INFO:
	// 	return "INFO"
	// case PACKET_DISCONNECT:
	// 	return "DISCONNECT"
	// case PACKET_HEARTBEAT:
	// 	return "HEARTBEAT"
	case PACKET_PING:
		return "PING"
	case PACKET_PONG:
		return "PONG"
	case PACKET_GOSSIP_REQ:
		return "GOSSIP_REQ"
	case PACKET_GOSSIP_RES:
		return "GOSSIP_RES"
	case PACKET_GOSSIP_HELLO:
		return "GOSSIP_HELLO"
	default:
		return "???"
	}
}

func (transporter *TCPTransporter) incomingMessage(msgType int, msgBytes *[]byte) {
	command := msgTypeToCommand(msgType)
	message := transporter.serializer.BytesToPayload(msgBytes)
	if transporter.validateMsg(message) {
		if handlers, ok := transporter.handlers[command]; ok {
			for _, handler := range handlers {
				handler(message)
			}
		}
	}
}

func (transporter *TCPTransporter) startTcpServer() {
	transporter.tcpReader = NewTcpReader(transporter.options.Port, transporter.onTcpMessage, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "TCPReader",
	}))
	transporter.tcpWriter = NewTcpWriter(transporter.options.MaxConnections, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "TCPWriter",
	}))
}

func (transporter *TCPTransporter) startUDPServer() {
	transporter.udpServer = NewUdpServer(UdpServerOptions{
		Port:           transporter.options.UdpPort,
		BindAddress:    transporter.options.UdpBindAddress,
		Multicast:      transporter.options.UdpMulticast,
		MulticastTTL:   transporter.options.UdpMulticastTTL,
		BroadcastAddrs: transporter.options.UdpBroadcast,
		DiscoverPeriod: transporter.options.UdpPeriod,
		MaxDiscovery:   transporter.options.UdpMaxDiscovery,
		Discovery:      transporter.options.UdpDiscovery,
		// Namespace:      transporter.options.Namespace, TODO
	}, transporter.onUdpMessage, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "UdpServer",
	}))

	err := transporter.udpServer.Start()
	if err != nil {
		transporter.logger.Error("Error starting UDP server:", err)
	}

}

func addIpToList(ipList []string, address string) []string {
	found := false
	for i, ip := range ipList {
		if ip == address {
			// Move the address to the front of the list
			ipList = append([]string{address}, append(ipList[:i], ipList[i+1:]...)...)
			found = true
			break
		}
	}
	if !found {
		// If the address is not in the list, add it to the front
		ipList = append([]string{address}, ipList...)
	}
	return ipList
}

// TODO - check full lifecycle - this message creates or updates a node with ip address and port to connect to directly
// need to find where the TCP connection step happens.. is not happening here - where is this node info used ?
func (transporter *TCPTransporter) onUdpMessage(nodeID, address string, port int) {
	if nodeID != "" && nodeID != transporter.options.NodeId {
		transporter.logger.Debug("UDP discovery received from " + address + " nodeId: " + nodeID + " port: " + string(port))
		node := transporter.registry.GetNodeByID(nodeID)
		if node == nil {
			// Unknown node. Register as offline node
			node = transporter.registry.AddOfflineNode(nodeID, address, port)
		} else if !node.IsAvailable() {
			ipList := addIpToList(node.GetIpList(), address)
			node.UpdateInfo(nodeID, map[string]interface{}{
				"hostname": address,
				"port":     port,
				"ipList":   ipList,
			})
		}
		node.UpdateInfo(nodeID, map[string]interface{}{
			"udpAddress": address,
		})
	}
}

func (transporter *TCPTransporter) startGossip() {
	transporter.gossip = &Gossip{}
}

func (transporter *TCPTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		// Additional disconnection logic goes here
		endChan <- nil
	}()
	return endChan
}

func (transporter *TCPTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if _, ok := transporter.handlers[command]; !ok {
		transporter.handlers[command] = make([]transit.TransportHandler, 0)
	}
	transporter.handlers[command] = append(transporter.handlers[command], handler)
}

func (transporter *TCPTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	// Additional publish logic goes here
}

func (transporter *TCPTransporter) SetPrefix(prefix string) {
	transporter.options.Prefix = prefix
}

func (transporter *TCPTransporter) SetNodeID(nodeID string) {
	transporter.options.NodeId = nodeID
}

func (transporter *TCPTransporter) SetSerializer(serializer serializer.Serializer) {
	transporter.options.Serializer = serializer
}
