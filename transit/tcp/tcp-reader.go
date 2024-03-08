package tcp

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
)

type TcpReader struct {
	port     int
	listener net.Listener
	sockets  map[net.Conn]bool
	logger   *log.Entry
	lock     sync.Mutex
}

func NewTcpReader(port int, logger *log.Entry) *TcpReader {
	return &TcpReader{
		port:    port,
		sockets: make(map[net.Conn]bool),
		logger:  logger,
	}
}

func (r *TcpReader) Listen() {
	var err error
	r.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", r.port))
	if err != nil {
		r.logger.Fatal("Server error: ", err)
	}

	r.logger.Info("TCP server is listening on port %d\n", r.port)

	go func() {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				r.logger.Error("Error accepting connection: ", err)
				continue
			}
			r.lock.Lock()
			r.sockets[conn] = true
			r.lock.Unlock()

			go r.handleConnection(conn)
		}
	}()
}

func (r *TcpReader) handleConnection(conn net.Conn) {
	address := conn.RemoteAddr().String()
	r.logger.Debug("New TCP client connected from '%s'\n", address)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		// TODO: Process incoming message here
		// For example, emit to a channel or call a callback function
		fmt.Printf("Received message from '%s': %s\n", address, msg)
		//
	}

	if err := scanner.Err(); err != nil {
		r.logger.Error("Error reading from '%s': %s\n", address, err)
	}

	r.closeSocket(conn)
}

func (r *TcpReader) closeSocket(conn net.Conn) {
	conn.Close()
	r.lock.Lock()
	delete(r.sockets, conn)
	r.lock.Unlock()
}

func (r *TcpReader) Close() {
	r.listener.Close()
	for conn := range r.sockets {
		r.closeSocket(conn)
	}
}
