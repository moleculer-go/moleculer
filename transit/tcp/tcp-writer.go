package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"sort"

	log "github.com/sirupsen/logrus"
)

const HEADER_SIZE = 6

type TCPConnEntry struct {
	conn     *net.TCPConn
	lastUsed time.Time
}

type TcpWriter struct {
	sockets        map[string]*TCPConnEntry
	maxConnections int
	logger         *log.Entry
	lock           sync.Mutex
}

func NewTcpWriter(maxConnections int, logger *log.Entry) *TcpWriter {
	return &TcpWriter{
		sockets:        make(map[string]*TCPConnEntry),
		maxConnections: maxConnections,
		logger:         logger,
	}
}

func (w *TcpWriter) Connect(nodeID, host string, port int) (*net.TCPConn, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if socket, exists := w.sockets[nodeID]; exists && socket != nil {
		return socket.conn, nil
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

	w.sockets[nodeID] = &TCPConnEntry{conn: conn, lastUsed: time.Now()}

	if len(w.sockets) > w.maxConnections {
		w.manageConnections()
	}
	return conn, nil
}

func (w *TcpWriter) Send(nodeID string, msgType byte, msgBytes []byte) error {
	socket, exists := w.sockets[nodeID]
	if !exists || socket == nil {
		return errors.New("connection does not exist")
	}
	header := make([]byte, HEADER_SIZE)
	binary.BigEndian.PutUint32(header[1:], uint32(len(msgBytes)+len(header)))
	header[5] = msgType
	crc := header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]
	header[0] = crc

	payload := append(header, msgBytes...)
	_, err := socket.conn.Write(payload)

	if !isGossipMessage(msgType) {
		socket.lastUsed = time.Now()
		w.sockets[nodeID] = socket
	}
	return err
}

type kv struct {
	Key   string
	Value *TCPConnEntry
}

func (w *TcpWriter) manageConnections() {
	// Simplified version: Close excess connections
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.sockets) <= w.maxConnections {
		return
	}

	orderedList := make([]kv, 0, len(w.sockets))
	for k, v := range w.sockets {
		orderedList = append(orderedList, kv{k, v})
	}
	sort.Slice(orderedList, func(i, j int) bool {
		return orderedList[i].Value.lastUsed.Before(orderedList[j].Value.lastUsed)
	})

	for _, kv := range orderedList {
		kv.Value.conn.Close()
		nodeID := kv.Key
		delete(w.sockets, nodeID)
		w.logger.Debugf("Closed connection to node %s", nodeID)
		if len(w.sockets) <= w.maxConnections {
			break
		}
	}
}

func (w *TcpWriter) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()

	for nodeID, socket := range w.sockets {
		socket.conn.Close()
		delete(w.sockets, nodeID)
	}
}
