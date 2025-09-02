package tcp

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"

	log "github.com/sirupsen/logrus"
)

type TransportState int

const (
	TransportStopped TransportState = iota
	TransportStarting
	TransportRunning
	TransportStopping
)

type TCPTransporter struct {
	options     TCPOptions
	tcpReader   *TcpReader
	tcpWriter   *TcpWriter
	udpServer   *UdpServer
	registry    moleculer.Registry
	gossipTimer *time.Ticker
	workerPool  *WorkerPool

	state      TransportState
	stateMutex sync.RWMutex

	logger *log.Entry

	validateMsg   transit.ValidateMsgFunc
	serializer    serializer.Serializer
	handlersMutex sync.RWMutex
	handlers      map[string][]transit.TransportHandler
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
	// TCP server port. 0 means random port
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
	// Apply default values from JavaScript implementation for compatibility
	if options.Port == 0 {
		options.Port = 0 // Random port (matches JS default)
	}
	if options.UdpPort == 0 {
		options.UdpPort = 4445 // Matches JS default
	}
	if options.UdpMulticast == "" {
		options.UdpMulticast = "239.0.0.0" // Matches JS default
	}
	if options.UdpMulticastTTL == 0 {
		options.UdpMulticastTTL = 1 // Matches JS default
	}
	if options.UdpPeriod == 0 {
		options.UdpPeriod = time.Second // Matches JS default (1 second)
	}
	if options.UdpMaxDiscovery == 0 {
		options.UdpMaxDiscovery = 0 // Unlimited (matches JS default)
	}
	if options.GossipPeriod == 0 {
		options.GossipPeriod = 2 // 2 seconds (matches JS default)
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = 32 // Matches JS default
	}
	if options.MaxPacketSize == 0 {
		options.MaxPacketSize = 1 * 1024 * 1024 // 1MB (matches JS default)
	}
	if options.UdpBindAddress == "" {
		options.UdpBindAddress = "0.0.0.0" // Bind to all interfaces (JS default)
	}

	transport := TCPTransporter{options: options, logger: options.Logger}
	transport.handlers = make(map[string][]transit.TransportHandler)
	transport.serializer = options.Serializer
	transport.validateMsg = options.ValidateMsg
	return transport
}

// State management methods for thread-safe access
func (transporter *TCPTransporter) getState() TransportState {
	transporter.stateMutex.RLock()
	defer transporter.stateMutex.RUnlock()
	return transporter.state
}

func (transporter *TCPTransporter) setState(state TransportState) {
	transporter.stateMutex.Lock()
	defer transporter.stateMutex.Unlock()
	transporter.state = state
}

func (transporter *TCPTransporter) Connect(registry moleculer.Registry) chan error {
	transporter.logger.Info("TCP Transport Connect() called")
	// Set state to starting
	transporter.setState(TransportStarting)

	transporter.registry = registry

	// Allow starting without URLs - the service can still receive incoming connections
	// from other services that have URLs pointing to this service
	if !transporter.options.UdpDiscovery && len(transporter.options.Urls) == 0 {
		transporter.logger.Info("TCP Transport: No peer URLs configured, waiting for incoming connections")
	}

	transporter.logger.Info("TCP Transport validation passed")

	// Initialize worker pool
	transporter.workerPool = NewWorkerPool(5, transporter.logger.WithField("component", "transport"))
	transporter.logger.Info("Worker pool created")

	endChan := make(chan error, 1)

	// Use worker pool for initialization tasks
	transporter.logger.Info("Submitting transport initialization task to worker pool")
	transporter.workerPool.Submit(func() {
		transporter.logger.Info("Transport initialization task STARTED")
		defer func() {
			if r := recover(); r != nil {
				transporter.logger.Errorf("Transport initialization panic: %v", r)
				transporter.setState(TransportStopped)
				endChan <- fmt.Errorf("transport initialization failed: %v", r)
			}
		}()

		transporter.startTcpServer()
		transporter.logger.Info("TCP server started")

		// Only start UDP server if UDP discovery is enabled
		if transporter.options.UdpDiscovery {
			transporter.startUDPServer()
		} else {
			// Parse URLs and connect to static peers when UDP discovery is disabled
			transporter.connectToStaticPeers()
			transporter.logger.Info("Static peer connection completed")
		}

		transporter.startGossipTimer()
		transporter.logger.Info("Gossip timer started")

		transporter.setState(TransportRunning)
		transporter.logger.Info("TCP Transport connected successfully")
		endChan <- nil
	})

	return endChan
}

type MessageType int

const (
	// JavaScript-compatible packet IDs - must match exactly
	PACKET_EVENT        = 1 // JS: PACKET_EVENT_ID = 1
	PACKET_REQUEST      = 2 // JS: PACKET_REQUEST_ID = 2
	PACKET_RESPONSE     = 3 // JS: PACKET_RESPONSE_ID = 3
	PACKET_PING         = 4 // JS: PACKET_PING_ID = 4
	PACKET_PONG         = 5 // JS: PACKET_PONG_ID = 5
	PACKET_GOSSIP_REQ   = 6 // JS: PACKET_GOSSIP_REQ_ID = 6
	PACKET_GOSSIP_RES   = 7 // JS: PACKET_GOSSIP_RES_ID = 7
	PACKET_GOSSIP_HELLO = 8 // JS: PACKET_GOSSIP_HELLO_ID = 8

	// Note: JavaScript does NOT support PACKET_DISCOVER, PACKET_INFO, PACKET_DISCONNECT, PACKET_HEARTBEAT
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
		transporter.logger.Errorf("Unknown command received - msgType: %d", msgType)
		return
	}
	transporter.logger.Debug("Incoming message - command: " + command)
	message := transporter.serializer.BytesToPayload(msgBytes)

	// Thread-safe access to handlers map
	transporter.handlersMutex.RLock()
	handlers, ok := transporter.handlers[command]
	transporter.handlersMutex.RUnlock()

	// if transporter.validateMsg(message) {
	if ok {
		for _, handler := range handlers {
			handler(message)
		}
	}
	// }
}

func (transporter *TCPTransporter) disconnectNodeByAddress(address string) {
	node := transporter.registry.GetNodeByAddress(address)
	if node != nil && !node.IsLocal() {
		transporter.registry.DisconnectNode(node.GetID())
	}
}

func (transporter *TCPTransporter) startTcpServer() {
	transporter.logger.Info("startTcpServer() called")
	transporter.tcpReader = NewTcpReader(transporter.options.Port, transporter.onTcpMessage, transporter.disconnectNodeByAddress, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "TCPReader",
	}))
	transporter.tcpWriter = NewTcpWriter(transporter.options.MaxConnections, transporter.logger.WithFields(log.Fields{
		"TCPTransporter": "TCPWriter",
	}))

	transporter.logger.Info("About to call tcpReader.Listen()")
	port, err := transporter.tcpReader.Listen()
	if err != nil {
		transporter.logger.Error("Error trying to listen on tcp reader - error: ", err)
		return
	}
	transporter.logger.Infof("tcpReader.Listen() returned port: %d", port)
	node := transporter.registry.GetLocalNode()
	node.UpdateInfo(map[string]interface{}{
		"port": port,
	})
	transporter.logger.Infof("TCP server is listening on port %d", port)
}

func (transporter *TCPTransporter) connectToStaticPeers() {
	transporter.logger.Info("Starting static peer connection process")
	if len(transporter.options.Urls) == 0 {

		transporter.logger.Info("No static peers configured - skipping direct connections")
		return
	}

	transporter.logger.Trace("Found %d peer URLs to process", len(transporter.options.Urls))
	transporter.logger.Infof("Connecting to %d static peer(s)...", len(transporter.options.Urls))

	for i, url := range transporter.options.Urls {
		transporter.logger.Trace("Processing peer URL %d: %s", i, url)
		nodeID, address, port, err := transporter.parsePeerURL(url)
		transporter.logger.Trace("Parsed peer URL %d successfully", i)
		if err != nil {
			transporter.logger.Errorf("Failed to parse peer URL '%s': %v", url, err)
			continue
		}

		transporter.logger.Trace("Adding offline node: %s", nodeID)
		peer := transporter.registry.AddOfflineNode(nodeID, address, address, port)
		transporter.logger.Trace("AddOfflineNode returned for %s (peer=%v)", nodeID, peer != nil)
		if peer != nil {
			transporter.logger.Infof("Registered static peer: %s at %s:%d", nodeID, address, port)

			transporter.logger.Trace("Starting connection goroutine for peer: %s", nodeID)
			go func(peerID, peerAddr string, peerPort int) {
				transporter.logger.Trace("Connection goroutine started for peer: %s", peerID)
				transporter.tryConnectToPeer(peerID, peerAddr, peerPort)
				transporter.logger.Trace("Connection goroutine finished for peer: %s", peerID)
			}(nodeID, address, port)
		}
	}
	transporter.logger.Trace("Static peer connection process completed")
}

func (transporter *TCPTransporter) parsePeerURL(url string) (nodeID, address string, port int, err error) {
	// Support formats:
	// 1. "nodeID@host:port" (Go format)
	// 2. "host:port/nodeID" (JavaScript format)
	// 3. "host:port" (with default nodeID)

	// Check for JavaScript format first (contains "/")
	if strings.Contains(url, "/") {
		parts := strings.Split(url, "/")
		if len(parts) == 2 {
			hostPort := parts[0]
			nodeID = parts[1]

			host, portStr, err := net.SplitHostPort(hostPort)
			if err != nil {
				return "", "", 0, fmt.Errorf("invalid host:port format '%s': %v", hostPort, err)
			}

			port, err = strconv.Atoi(portStr)
			if err != nil {
				return "", "", 0, fmt.Errorf("invalid port '%s': %v", portStr, err)
			}

			address = host
			return nodeID, address, port, nil
		}
	}

	// Check for Go format (contains "@")
	if strings.Contains(url, "@") {
		parts := strings.Split(url, "@")
		if len(parts) == 2 {
			nodeID = parts[0]
			hostPort := parts[1]

			host, portStr, err := net.SplitHostPort(hostPort)
			if err != nil {
				return "", "", 0, fmt.Errorf("invalid host:port format '%s': %v", hostPort, err)
			}

			port, err = strconv.Atoi(portStr)
			if err != nil {
				return "", "", 0, fmt.Errorf("invalid port '%s': %v", portStr, err)
			}

			address = host
			return nodeID, address, port, nil
		}
	}

	// Default format: "host:port" (with default nodeID)
	host, portStr, err := net.SplitHostPort(url)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid host:port format '%s': %v", url, err)
	}

	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid port '%s': %v", portStr, err)
	}

	// Default nodeID format: host_port
	nodeID = strings.ReplaceAll(url, ":", "_")
	address = host
	return nodeID, address, port, nil
}

func (transporter *TCPTransporter) tryConnectToPeer(nodeID, address string, port int) {
	err := transporter.tryToConnect(nodeID)
	if err != nil {
		transporter.logger.Warnf("Failed to connect to peer %s at %s:%d: %v", nodeID, address, port, err)
	} else {
		transporter.logger.Infof("Successfully connected to peer %s at %s:%d", nodeID, address, port)
	}
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
	}, transporter.registry, transporter.onUdpMessage, transporter.logger.WithFields(log.Fields{
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
		transporter.logger.Debugf("UDP discovery received from %s nodeId: %s port: %d", address, nodeID, port)
		node := transporter.registry.GetNodeByID(nodeID)
		if node == nil {
			transporter.logger.Debug("Unknown node. Register as offline node")
			node = transporter.registry.AddOfflineNode(nodeID, address, address, port)
		} else if !node.IsAvailable() {
			ipList := addIpToList(node.GetIpList(), address)
			node.UpdateInfo(map[string]interface{}{
				// "hostname": address,
				"port":   port,
				"ipList": ipList,
			})
		}
		node.UpdateInfo(map[string]interface{}{
			"udpAddress": address,
		})
	}
}

func (transporter *TCPTransporter) Disconnect() chan error {
	// Set state to stopping
	transporter.setState(TransportStopping)

	endChan := make(chan error, 1)

	// Use worker pool for shutdown if available, otherwise direct goroutine
	if transporter.workerPool != nil && !transporter.workerPool.IsStopped() {
		transporter.workerPool.Submit(func() {
			transporter.performShutdown(endChan)
		})
	} else {
		go func() {
			transporter.performShutdown(endChan)
		}()
	}

	return endChan
}

func (transporter *TCPTransporter) performShutdown(endChan chan error) {
	defer func() {
		if r := recover(); r != nil {
			transporter.logger.Errorf("Transport shutdown panic: %v", r)
			transporter.setState(TransportStopped)
			endChan <- fmt.Errorf("transport shutdown failed: %v", r)
		}
	}()

	transporter.logger.Info("TCP Transport disconnecting...")

	// Stop worker pool first
	if transporter.workerPool != nil {
		transporter.workerPool.Stop()
	}

	// Close components
	if transporter.tcpReader != nil {
		transporter.tcpReader.Close()
	}

	if transporter.tcpWriter != nil {
		transporter.tcpWriter.Close()
	}

	if transporter.udpServer != nil && transporter.options.UdpDiscovery {
		transporter.udpServer.Close()
	}

	// Stop gossip timer
	if transporter.gossipTimer != nil {
		transporter.gossipTimer.Stop()
		transporter.gossipTimer = nil
	}

	transporter.setState(TransportStopped)
	transporter.logger.Info("TCP Transport disconnected successfully")
	endChan <- nil
}

func (transporter *TCPTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	transporter.handlersMutex.Lock()
	defer transporter.handlersMutex.Unlock()

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
		return NewTransportErrorWithNode("connect", nodeID, fmt.Errorf("unknown node"))
	}

	nodeAddress := transporter.getNodeAddress(node)
	if nodeAddress == "" {
		return NewTransportErrorWithNode("connect", nodeID, fmt.Errorf("no address found"))
	}

	_, err := transporter.tcpWriter.Connect(nodeID, nodeAddress, node.GetPort())
	if err != nil {
		return NewTransportErrorWithAddress("connect", nodeID,
			fmt.Sprintf("%s:%d", nodeAddress, node.GetPort()), err)
	}

	// Update node status to online after successful connection
	node.Available()
	transporter.logger.Infof("Connected to node %s at %s:%d", nodeID, nodeAddress, node.GetPort())
	return nil
}

func (transporter *TCPTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	// Check if transport is running
	if transporter.getState() != TransportRunning {
		transporter.logger.Warn("Cannot publish message: transport not running")
		return
	}

	transporter.logger.Debugf("TCP Transport Publish() command: %s to nodeID: %s", command, nodeID)

	// JavaScript TCP transporter only supports: EVENT, REQ, RES, PING, PONG, GOSSIP_*
	// Handle unsupported commands gracefully
	switch command {
	case "HEARTBEAT":
		// JavaScript doesn't support HEARTBEAT - silently ignore
		transporter.logger.Debugf("Ignoring HEARTBEAT command (not supported by JavaScript TCP transporter)")
		return
	case "DISCOVER", "INFO", "DISCONNECT":
		// JavaScript doesn't support these - silently ignore
		transporter.logger.Debugf("Ignoring unsupported command: %s", command)
		return
	}

	msgType := commandToMsgType(command)
	if msgType == -1 {
		transporter.logger.Errorf("Invalid command: %s", command)
		return
	}

	msgBytes := transporter.serializer.PayloadToBytes(message)

	// Use worker pool for async operations
	transporter.workerPool.Submit(func() {
		transporter.publishMessage(command, nodeID, byte(msgType), msgBytes)
	})
}

func (transporter *TCPTransporter) publishMessage(command, nodeID string, msgType byte, msgBytes []byte) {
	defer func() {
		if r := recover(); r != nil {
			transporter.logger.Errorf("Publish panic recovered: %v", r)
		}
	}()

	if nodeID == "" {
		// Broadcast - not yet implemented with connection pool
		transporter.logger.Warn("Broadcast not implemented with new connection pool")
		return
	}

	// Check connection and send
	if !transporter.tcpWriter.IsConnected(nodeID) {
		err := transporter.tryToConnect(nodeID)
		if err != nil {
			transporter.logger.Errorf("Failed to connect to node %s: %v", nodeID, err)
			return
		}
	}

	err := transporter.tcpWriter.Send(nodeID, msgType, msgBytes)
	if err != nil {
		if transportErr, ok := err.(*TransportError); ok {
			transporter.logger.Errorf("Transport error sending to %s: %v", nodeID, transportErr)
		} else {
			transporter.logger.Errorf("Error sending message to %s: %v", nodeID, err)
		}
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
