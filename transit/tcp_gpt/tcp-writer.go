package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type TcpWriter struct {
	sockets map[string]*net.TCPConn
	opts    WriterOptions
	logger  *log.Entry
	lock    sync.Mutex
}

type WriterOptions struct {
	MaxConnections int
}

func NewTcpWriter(opts WriterOptions, logger *log.Entry) *TcpWriter {
	return &TcpWriter{
		sockets: make(map[string]*net.TCPConn),
		opts:    opts,
		logger:  logger,
	}
}

func (w *TcpWriter) Connect(nodeID, host string, port int) (*net.TCPConn, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if socket, exists := w.sockets[nodeID]; exists && socket != nil {
		return socket, nil
	}

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}

	conn.SetNoDelay(true)
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(3 * time.Minute)

	w.sockets[nodeID] = conn
	return conn, nil
}

func (w *TcpWriter) Send(nodeID string, packetType byte, data []byte) error {
	socket, exists := w.sockets[nodeID]
	if !exists || socket == nil {
		return errors.New("connection does not exist")
	}

	header := make([]byte, 6)
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)+len(header)))
	header[5] = packetType
	crc := header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]
	header[0] = crc

	payload := append(header, data...)
	_, err := socket.Write(payload)
	return err
}

func (w *TcpWriter) manageConnections() {
	// Simplified version: Close excess connections
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.sockets) <= w.opts.MaxConnections {
		return
	}

	for nodeID, socket := range w.sockets {
		socket.Close()
		delete(w.sockets, nodeID)
		w.logger.Debug("Closed connection to node %s", nodeID)

		if len(w.sockets) <= w.opts.MaxConnections {
			break
		}
	}
}

func (w *TcpWriter) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	for nodeID, socket := range w.sockets {
		socket.Close()
		delete(w.sockets, nodeID)
	}
}
