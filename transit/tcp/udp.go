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

type UdpServer struct {
	conn             *net.UDPConn
	opts             UdpServerOptions
	discoveryCounter int
	logger           *log.Entry
	discoverTimer    *time.Ticker
	servers          []*net.UDPConn
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
}

func NewUdpServer(opts UdpServerOptions, logger *log.Entry) *UdpServer {
	return &UdpServer{
		opts:   opts,
		logger: logger,
	}
}

func (u *UdpServer) startServer(ip string, port int, multicast string, multicastTTL int) error {
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

	// Start a goroutine to handle incoming messages
	go func() {
		buf := make([]byte, 1024)
		for {
			n, addr, err := udpConn.ReadFromUDP(buf)
			if err != nil {
				u.logger.Warnf("Error reading from UDP: %s\n", err)
				break
			}

			// Handle the message
			u.onMessage(buf[:n], addr)
		}
	}()

	// Add the connection to your list of servers
	u.servers = append(u.servers, udpConn)

	return nil
}

func (u *UdpServer) onMessage(buf []byte, addr *net.UDPAddr) {
	msg := string(buf)
	u.logger.Debugf("UDP message received from %s: %s\n", addr.String(), msg)

	parts := strings.Split(msg, "|")
	if len(parts) != 3 {
		u.logger.Debugf("Malformed UDP packet received: %s\n", msg)
		return
	}

	if parts[0] == u.namespace {
		port, err := strconv.Atoi(parts[2])
		if err != nil {
			u.logger.Debugf("UDP packet process error: %s\n", err)
			return
		}

		u.emit("message", parts[1], addr.IP.String(), port)
	}
}

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
			// Bind only one interface
			return u.startServer(u.opts.BindAddress, u.opts.Port, u.opts.Multicast, u.opts.MulticastTTL)
		}
		//list all interfaces and the ip addresses of each interface
		ips := u.getAllIPs()
		for _, ip := range ips {
			err := u.startServer(ip, u.opts.Port, u.opts.Multicast, u.opts.MulticastTTL)
			if err != nil {
				u.logger.Error("Error starting server on IP:", ip, err)
			}
		}
	} else if len(u.opts.BroadcastAddrs) > 0 {
		return u.startServer(u.opts.BindAddress, u.opts.Port, "", 0)
	}

	go u.firstDiscoveryMessage()

	go u.handleIncomingMessages()

	u.startDiscovering()

	return nil
}

func (u *UdpServer) firstDiscoveryMessage() {
	//wait for 1 second before sending the first discovery message
	time.Sleep(time.Second)
	u.broadcastDiscoveryMessage()
}

func (u *UdpServer) handleIncomingMessages() {
	buffer := make([]byte, 2048)
	for {
		n, addr, err := u.conn.ReadFromUDP(buffer)
		if err != nil {
			u.logger.Println("Error reading from UDP:", err)
			continue
		}
		message := string(buffer[:n])
		u.logger.Debug("Received message from %s: %s", addr.String(), message)
		// Process message here

	}
}

func (u *UdpServer) startDiscovering() {
	if u.opts.Discovery == false {
		u.logger.Info("UDP Discovery is disabled.")
		return
	}
	u.discoverTimer = time.NewTicker(u.opts.DiscoverPeriod)
	go func() {
		for range u.discoverTimer.C {
			u.broadcastDiscoveryMessage()
			if u.opts.MaxDiscovery > 0 && u.discoveryCounter >= u.opts.MaxDiscovery {
				u.StopDiscovering()
			}
		}
	}()
}

func (u *UdpServer) broadcastDiscoveryMessage() {
	message := []byte("discovery message") // Customize your message
	destAddr := &net.UDPAddr{IP: net.IPv4bcast, Port: u.opts.Port}
	if _, err := u.conn.WriteToUDP(message, destAddr); err != nil {
		u.logger.Println("Error broadcasting discovery message:", err)
	}
}

func (u *UdpServer) StopDiscovering() {
	if u.discoverTimer != nil {
		u.discoverTimer.Stop()
		u.discoverTimer = nil
		u.logger.Info("Discovery limit reached, stopping UDP discovery")
	}
}

func (u *UdpServer) Close() {
	u.StopDiscovering()
	if u.conn != nil {
		u.conn.Close()
	}
}
