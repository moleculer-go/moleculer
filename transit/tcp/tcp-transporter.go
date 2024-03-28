package tcp

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"

	log "github.com/sirupsen/logrus"
)

type TCPTransporter struct {
	options   TCPOptions
	tcpReader *TcpReader
	tcpWriter *TcpWriter
	udpServer *UdpServer
	gossip    *Gossip
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

func (transporter *TCPTransporter) onTcpMessage(msgType int, msgBytes *[]byte) {
	switch msgType {
	case PACKET_GOSSIP_HELLO:
		transporter.gossip.processHello(msgBytes)
	case PACKET_GOSSIP_REQ:
		transporter.gossip.processRequest(msgBytes)
	case PACKET_GOSSIP_RES:
		transporter.gossip.processResponse(msgBytes)
	default:
		transporter.incomingMessage(msgType, msgBytes)
	}
}

func (transporter *TCPTransporter) msgTypeToCommand(msgType int) string {
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
	command := transporter.msgTypeToCommand(msgType)
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
	}, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "UdpServer",
	}))

	err := transporter.udpServer.Start()
	if err != nil {
		transporter.logger.Error("Error starting UDP server:", err)
	}

}

func (transporter *TCPTransporter) onUdpMessage(nodeID, address string, port int) {
	if nodeID != "" && nodeID != transporter.options.NodeId {
		transporter.logger.Debug(`UDP discovery received from ${address} on ${nodeID}.`)

		node := transporter.registry.GetNodeByID(nodeID)
		if node == nil {
			// Unknown node. Register as offline node
			node = transporter.registry.AddOfflineNode(nodeID, address, port)
		} else if !node.IsAvailable() {
			ipList := node.GetIpList()
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
			node.Update(nodeID, map[string]interface{}{
				"hostname": address,
				"port":     port,
				"ipList":   ipList,
			})
		}
		node.Update(nodeID, map[string]interface{}{
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
