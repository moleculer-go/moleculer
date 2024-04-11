package tcp

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/ipv4"

	log "github.com/sirupsen/logrus"
)

type UdpServerEntry struct {
	conn             *net.UDPConn
	discoveryTargets []string
}

type OnUdpMessage func(nodeID, ip string, port int)

type UdpServer struct {
	state            State
	opts             UdpServerOptions
	discoveryCounter int
	logger           *log.Entry
	discoverTimer    *time.Ticker
	servers          []*UdpServerEntry
	onUdpMessage     OnUdpMessage
}

type UdpServerOptions struct {
	Port           int
	Multicast      string
	MulticastTTL   int
	BindAddress    string
	BroadcastAddrs []string
	DiscoverPeriod time.Duration
	MaxDiscovery   int
	Discovery      bool
	Namespace      string
	NodeID         string
}

func NewUdpServer(opts UdpServerOptions, onUdpMessage OnUdpMessage, logger *log.Entry) *UdpServer {
	return &UdpServer{
		opts:         opts,
		onUdpMessage: onUdpMessage,
		logger:       logger,
	}
}

func (u *UdpServer) startServer(ip string, port int, multicast string, multicastTTL int, discoveryTargets []string) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		u.logger.Warnf("Unable to resolve UDP address: %s\n", err)
		return err
	}

	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		u.logger.Warnf("Unable to listen on UDP address: %s\n", err)
		return err
	}

	if multicast != "" {
		u.logger.Infof("UDP Multicast Server is listening on %s:%d. Membership:  %s \n", ip, port, multicast)
		err := u.joinMulticastGroup(multicast, udpConn, multicastTTL, ip, port)
		if err != nil {
			u.logger.Error("Error joining multicast group:", err)
			return err
		}

	} else {
		u.logger.Infof("UDP Broadcast Server is listening on %s:%d\n", ip, port)
	}

	// // Start a goroutine to handle incoming messages
	// go func() {
	// 	buf := make([]byte, 1024)
	// 	for {
	// 		n, addr, err := udpConn.ReadFromUDP(buf)
	// 		if err != nil {
	// 			u.logger.Warnf("Error reading from UDP: %s\n", err)
	// 			break
	// 		}

	// 		// Handle the message
	// 		u.onMessage(buf[:n], addr)
	// 	}
	// }()
	// Add the connection to your list of servers
	u.servers = append(u.servers, &UdpServerEntry{udpConn, discoveryTargets})
	return nil
}

// func (u *UdpServer) onMessage(buf []byte, addr *net.UDPAddr) {
// 	msg := string(buf)
// 	u.logger.Debugf("UDP message received from %s: %s\n", addr.String(), msg)

// 	parts := strings.Split(msg, "|")
// 	if len(parts) != 3 {
// 		u.logger.Debugf("Malformed UDP packet received: %s\n", msg)
// 		return
// 	}

// 	if parts[0] == u.namespace {
// 		port, err := strconv.Atoi(parts[2])
// 		if err != nil {
// 			u.logger.Debugf("UDP packet process error: %s\n", err)
// 			return
// 		}

// 		u.emit("message", parts[1], addr.IP.String(), port)
// 	}
// }

func (u *UdpServer) joinMulticastGroup(multicast string, udpConn *net.UDPConn, multicastTTL int, ip string, port int) error {
	groupAddr, err := net.ResolveUDPAddr("udp4", multicast)
	if err != nil {
		u.logger.Warnf("Unable to resolve multicast address: %s\n", err)
		return err
	}

	p := ipv4.NewPacketConn(udpConn)

	interfaces, err := net.Interfaces()
	if err != nil {
		u.logger.Warnf("Unable to get network interfaces: %s\n", err)
		return err
	}

	for _, iface := range interfaces {
		if err := p.JoinGroup(&iface, groupAddr); err != nil {
			u.logger.Warnf("Unable to join multicast group on interface %s: %s\n", iface.Name, err)
		}
	}

	if err := p.SetMulticastTTL(multicastTTL); err != nil {
		u.logger.Warnf("Unable to set multicast TTL: %s\n", err)
		return err
	}

	u.logger.Infof("UDP Multicast Server is listening on %s:%d. Membership: %s\n", ip, port, multicast)
	return nil
}

func (u *UdpServer) getAllIPs() []string {
	ips := []string{}
	interfaces, err := net.Interfaces()
	if err != nil {
		u.logger.Error("Error getting interfaces:", err)
		return ips
	}

	for _, i := range interfaces {
		addrs, err := i.Addrs()
		if err != nil {
			u.logger.Error("Error getting addresses for interface:", err)
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil {
				continue
			}
			ips = append(ips, ip.String())
			u.logger.Debug("Interface: %v, IP Address: %v", i.Name, ip.String())
		}
	}
	return ips
}

func (u *UdpServer) Start() error {
	if u.opts.Multicast != "" {
		if u.opts.BindAddress != "" {
			u.logger.Debug("Multicast + BindAddress options specified - Binding to a specific interface:", u.opts.BindAddress)
			// Bind only one interface
			return u.startServer(u.opts.BindAddress, u.opts.Port, u.opts.Multicast, u.opts.MulticastTTL, []string{u.opts.Multicast})
		}
		//list all interfaces and the ip addresses of each interface
		u.logger.Debug("Multicast option specified - listing all interfaces and the ip addresses of each interface")
		ips := u.getAllIPs()
		for _, ip := range ips {
			u.logger.Debug("Starting UDP server on IP:", ip, "Port:", u.opts.Port, "Multicast:", u.opts.Multicast, "MulticastTTL:", u.opts.MulticastTTL)
			err := u.startServer(ip, u.opts.Port, u.opts.Multicast, u.opts.MulticastTTL, []string{u.opts.Multicast})
			if err != nil {
				u.logger.Error("Error starting server on IP:", ip, err)
			}
		}
	} else if len(u.opts.BroadcastAddrs) > 0 {
		u.logger.Debug("Starting UDP server on IP (BindAddress):", u.opts.BindAddress, "Port:", u.opts.Port, " BroadcastAddrs option specified - Broadcasting to the specified addresses - BroadcastAddrs:", u.opts.BroadcastAddrs)
		return u.startServer(u.opts.BindAddress, u.opts.Port, "", 0, u.opts.BroadcastAddrs)
	} else {
		broadcastAddrss := u.getBroadcastAddresses()
		u.logger.Debug("Starting UDP server on IP (BindAddress):", u.opts.BindAddress, "Port:", u.opts.Port, "No Multicast or BroadcastAddrs options specified - Broadcasting to all interfaces - broadcastAddrss: ", broadcastAddrss)
		return u.startServer(u.opts.BindAddress, u.opts.Port, "", 0, broadcastAddrss)
	}

	u.state = STARTED

	go u.firstDiscoveryMessage()

	for _, server := range u.servers {
		go u.handleIncomingMessagesForServer(server)
	}

	u.startDiscovering()

	return nil
}

func (u *UdpServer) getBroadcastAddresses() []string {
	list := []string{}
	interfaces, err := net.Interfaces()
	if err != nil {
		u.logger.Error("Error getting network interfaces:", err)
		return list
	}
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			u.logger.Error("Error getting addresses for interface:", iface.Name, err)
			continue
		}
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					// Calculate the broadcast address by inverting the netmask and OR'ing it with the IP address
					ip := ipnet.IP.To4()
					mask := ipnet.Mask
					broadcast := net.IPv4(0, 0, 0, 0)
					for i := range ip {
						broadcast[i] = ip[i] | ^mask[i]
					}
					list = append(list, broadcast.String())
				}
			}
		}
	}
	return list
}

func (u *UdpServer) firstDiscoveryMessage() {
	//wait for 1 second before sending the first discovery message
	time.Sleep(time.Second)
	u.broadcastDiscoveryMessage()
}

func (u *UdpServer) handleIncomingMessagesForServer(server *UdpServerEntry) {
	buffer := make([]byte, 2048)
	for u.state == STARTED {
		n, addr, err := server.conn.ReadFromUDP(buffer)
		if err != nil {
			u.logger.Errorln("Error reading from UDP:", err)
			continue
		}
		message := string(buffer[:n])
		u.logger.Debug("Received message from %s: %s", addr.String(), message)

		// Parse the message
		parts := strings.Split(message, "|")
		if len(parts) != 3 {
			u.logger.Debug("Malformed UDP packet received: %s", message)
			continue
		}

		namespace := parts[0]

		if namespace != u.opts.Namespace {
			u.logger.Debug("Message received for a different namespace: %s", namespace)
			continue
		}

		nodeID := parts[1]
		port, err := strconv.Atoi(parts[2])
		if err != nil {
			u.logger.Debug("Error parsing port number: %s", err)
			continue
		}

		u.onUdpMessage(nodeID, addr.IP.String(), port)
	}
	u.logger.Debug("handleIncomingMessagesForServer() stopped")
}

func (u *UdpServer) startDiscovering() {
	if u.opts.Discovery == false {
		u.logger.Info("UDP Discovery is disabled.")
		return
	}
	if u.discoverTimer != nil {
		u.logger.Warn("Discovery already started.")
		return
	}
	u.discoverTimer = time.NewTicker(u.opts.DiscoverPeriod)
	go func() {
		for range u.discoverTimer.C {
			u.broadcastDiscoveryMessage()
			if u.opts.MaxDiscovery > 0 && u.discoveryCounter >= u.opts.MaxDiscovery {
				u.logger.Info("Discovery limit reached, stopping UDP discovery")
				u.StopDiscovering()
			}
		}
	}()
}

func (u *UdpServer) broadcastDiscoveryMessage() {
	message := fmt.Sprintf("%s|%s|%d", u.opts.Namespace, u.opts.NodeID, u.opts.Port)
	u.logger.Debug("Broadcasting discovery message:", message)
	u.discoveryCounter++
	for _, server := range u.servers {
		for _, target := range server.discoveryTargets {
			destAddr, err := net.ResolveUDPAddr("udp4", target+":"+strconv.Itoa(u.opts.Port))
			if err != nil {
				u.logger.Error("Error resolving UDP address:", err)
				continue
			}
			if _, err := server.conn.WriteToUDP([]byte(message), destAddr); err != nil {
				u.logger.Error("Error broadcasting discovery message to:", destAddr, " error:", err)
			} else {
				u.logger.Debug("Discovery message sent to:", destAddr)
			}
		}
	}
}

func (u *UdpServer) StopDiscovering() {
	u.logger.Info("Stopping UDP Discovery.")
	if u.discoverTimer != nil {
		u.discoverTimer.Stop()
		u.discoverTimer = nil
	}
}

func (u *UdpServer) Close() {
	u.logger.Info("Closing UDP Server.")
	u.state = STOPPED
	u.StopDiscovering()
	for _, server := range u.servers {
		if server.conn != nil {
			server.conn.Close()
		}
	}
}
