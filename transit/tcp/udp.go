package tcp

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/net/ipv4"

	log "github.com/sirupsen/logrus"
)

type UdpServer struct {
	conn          *net.UDPConn
	opts          UdpServerOptions
	logger        *log.Entry
	discoverTimer *time.Ticker
}

type UdpServerOptions struct {
	Port           int
	Multicast      string
	MulticastTTL   int
	Broadcast      bool
	BroadcastAddrs []string // Optional, specify if not broadcasting to default subnet broadcast addresses
	DiscoverPeriod time.Duration
}

func NewUdpServer(opts UdpServerOptions, logger *log.Entry) *UdpServer {
	return &UdpServer{
		opts:   opts,
		logger: logger,
	}
}

func (u *UdpServer) Bind() error {
	addr := net.UDPAddr{
		Port: u.opts.Port,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return err
	}
	u.conn = conn

	if u.opts.Multicast != "" {
		err := u.joinMulticastGroup()
		if err != nil {
			u.logger.Println("Error joining multicast group:", err)
			// Handle non-fatal error, as we can continue in broadcast mode
		}
	}

	go u.handleIncomingMessages()

	u.startDiscovering()

	return nil
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
		u.logger.Printf("Received message from %s: %s", addr.String(), message)
		// Process message here
	}
}

func (u *UdpServer) startDiscovering() {
	u.discoverTimer = time.NewTicker(u.opts.DiscoverPeriod)
	go func() {
		for range u.discoverTimer.C {
			u.broadcastDiscoveryMessage()
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
	}
}

func (u *UdpServer) Close() {
	u.StopDiscovering()
	if u.conn != nil {
		u.conn.Close()
	}
}

func (u *UdpServer) joinMulticastGroup() error {
	multicastAddr := net.ParseIP(u.opts.Multicast)
	if multicastAddr == nil {
		return fmt.Errorf("invalid multicast address: %s", u.opts.Multicast)
	}
	iface, err := net.InterfaceByName("eth0") // Specify the appropriate interface, or iterate over all if needed
	if err != nil {
		return fmt.Errorf("failed to get interface: %v", err)
	}
	p := ipv4.NewPacketConn(u.conn)
	if err := p.JoinGroup(iface, &net.UDPAddr{IP: multicastAddr}); err != nil {
		return fmt.Errorf("failed to join multicast group: %v", err)
	}
	if err := p.SetMulticastLoopback(true); err != nil {
		return fmt.Errorf("failed to set multicast loopback: %v", err)
	}
	return nil
}
