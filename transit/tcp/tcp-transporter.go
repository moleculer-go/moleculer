package tcp

import (
	"errors"
	"strconv"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"

	log "github.com/sirupsen/logrus"
)

type TCPTransporter struct {
	options     TCPOptions
	tcpReader   *TcpReader
	tcpWriter   *TcpWriter
	udpServer   *UdpServer
	registry    moleculer.Registry
	gossipTimer *time.Ticker

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
	Namespace   string
	Logger      *log.Entry
	Serializer  serializer.Serializer
	ValidateMsg transit.ValidateMsgFunc
}

func CreateTCPTransporter(options TCPOptions) TCPTransporter {
	transport := TCPTransporter{options: options, logger: options.Logger}
	transport.handlers = make(map[string][]transit.TransportHandler)
	transport.serializer = options.Serializer
	transport.validateMsg = options.ValidateMsg
	return transport
}

func (transporter *TCPTransporter) Connect(registry moleculer.Registry) chan error {
	transporter.registry = registry
	transporter.logger.Info("TCP Transported Connect()")
	endChan := make(chan error)
	go func() {
		transporter.startTcpServer()
		transporter.startUDPServer()
		transporter.startGossipTimer()
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
func commandToMsgType(command string) int {
	switch command {
	case "EVENT":
		return PACKET_EVENT
	case "REQ":
		return PACKET_REQUEST
	case "RES":
		return PACKET_RESPONSE
	case "PING":
		return PACKET_PING
	case "PONG":
		return PACKET_PONG
	case "GOSSIP_REQ":
		return PACKET_GOSSIP_REQ
	case "GOSSIP_RES":
		return PACKET_GOSSIP_RES
	case "GOSSIP_HELLO":
		return PACKET_GOSSIP_HELLO
	default:
		return -1
	}
}

func (transporter *TCPTransporter) incomingMessage(msgType int, msgBytes *[]byte) {
	command := msgTypeToCommand(msgType)
	if command == "???" {
		transporter.logger.Error("Unknown command received - msgType: " + string(msgType))
		return
	}
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
	transporter.tcpReader.Listen()
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
		NodeID:         transporter.options.NodeId,
		Namespace:      transporter.options.Namespace,
	}, transporter.onUdpMessage, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "UdpServer",
	}))

	err := transporter.udpServer.Start()
	if err != nil {
		transporter.logger.Error("TCPTransporter.startUDPServer() Error starting UDP server:", err)
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

func (transporter *TCPTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		transporter.tcpReader.Close()
		transporter.tcpWriter.Close()
		transporter.udpServer.Close()
		if transporter.gossipTimer != nil {
			transporter.gossipTimer.Stop()
		}
		endChan <- nil
	}()
	return endChan
}

func (transporter *TCPTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	// if commandToMsgType(command) == -1 {
	// 	transporter.logger.Error("TCPTransporter.Subscribe() Invalid command: " + command)
	// 	return
	// }
	if _, ok := transporter.handlers[command]; !ok {
		transporter.handlers[command] = make([]transit.TransportHandler, 0)
	}
	transporter.handlers[command] = append(transporter.handlers[command], handler)
}

func (transporter *TCPTransporter) getNodeAddress(node moleculer.Node) string {
	if node.GetUdpAddress() != "" {
		return node.GetUdpAddress()
	}
	if transporter.options.UseHostname && node.GetHostname() != "" {
		return node.GetHostname()
	}
	if len(node.GetIpList()) > 0 {
		return node.GetIpList()[0]
	}
	return ""
}

func (transporter *TCPTransporter) tryToConnect(nodeID string) error {
	node := transporter.registry.GetNodeByID(nodeID)
	if node == nil {
		transporter.logger.Error("TCPTransporter.tryToConnect() Unknown nodeID: " + nodeID)
		return errors.New("Unknown nodeID: " + nodeID)
	}
	nodeAddress := transporter.getNodeAddress(node)
	if nodeAddress == "" {
		transporter.logger.Error("TCPTransporter.tryToConnect() No address found for nodeID: " + nodeID)
		return errors.New("No address found for nodeID: " + nodeID)
	}
	_, err := transporter.tcpWriter.Connect(nodeID, nodeAddress, node.GetPort())
	if err != nil {
		transporter.logger.Error("TCPTransporter.tryToConnect() Error connecting to nodeID: "+nodeID+" node address:"+nodeAddress+" port: "+strconv.Itoa(node.GetPort())+" error: ", err)
		return err
	}
	transporter.logger.Info("TCPTransporter.tryToConnect() Connected to nodeID: " + nodeID + " node address:" + nodeAddress + " port: " + strconv.Itoa(node.GetPort()))
	return nil
}

func (transporter *TCPTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	msgType := commandToMsgType(command)
	if msgType == -1 {
		transporter.logger.Error("TCPTransporter.Publish() Invalid command: " + command + " nodeID: " + nodeID)
		return
	}

	msgBts := transporter.serializer.PayloadToBytes(message)

	if nodeID == "" {
		err := transporter.tcpWriter.Broadcast(byte(msgType), msgBts)
		if err != nil {
			transporter.logger.Error("TCPTransporter.Publish() Error broadcasting message command:"+command+" error: ", err)
		}
		return
	}

	if !transporter.tcpWriter.IsConnected(nodeID) {
		err := transporter.tryToConnect(nodeID)
		if err != nil {
			transporter.logger.Error("TCPTransporter.Publish() Error connecting to nodeID: "+nodeID+" error: ", err)
			return
		}
	}
	err := transporter.tcpWriter.Send(nodeID, byte(msgType), msgBts)
	if err != nil {
		transporter.logger.Error("TCPTransporter.Publish() Error sending message command:"+command+" error: ", err)
	}
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
