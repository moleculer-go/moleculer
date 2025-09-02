package tcp

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type State int

const (
	STARTED State = iota
	STOPPED
)

type OnMessageFunc func(fromAddrss string, msgType int, msgBytes *[]byte)

type TcpReader struct {
	port                    int
	listener                net.Listener
	sockets                 map[net.Conn]bool
	logger                  *log.Entry
	lock                    sync.Mutex
	state                   State
	maxPacketSize           int
	onMessage               OnMessageFunc
	disconnectNodeByAddress func(address string)

	// New components for improved resource management
	bufferPool *BufferPool
	workerPool *WorkerPool
}

func NewTcpReader(port int, onMessage OnMessageFunc, disconnectNodeByAddress func(address string), logger *log.Entry) *TcpReader {
	reader := &TcpReader{
		port:                    port,
		sockets:                 make(map[net.Conn]bool),
		logger:                  logger,
		onMessage:               onMessage,
		disconnectNodeByAddress: disconnectNodeByAddress,
	}

	// Initialize new components
	reader.bufferPool = NewBufferPool()
	reader.workerPool = NewWorkerPool(10, logger.WithField("component", "tcp-reader"))

	return reader
}

func (r *TcpReader) Listen() (int, error) {
	var err error
	r.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", r.port))
	if err != nil {
		r.logger.Error("Server error: ", err)
		return 0, err
	}

	if r.port == 0 {
		_, portString, err := net.SplitHostPort(r.listener.Addr().String())
		if err != nil {
			r.logger.Error("Could not net.SplitHostPort() error: ", err)
			return 0, err
		}
		port, err := strconv.Atoi(portString)
		if err != nil {
			r.logger.Error("Could not convert port to integer error: ", err)
			return 0, err
		}
		r.port = port
	}

	r.logger.Infof("TCP server is listening on port %d", r.port)

	r.state = STARTED
	go func() {
		for r.state == STARTED {
			conn, err := r.listener.Accept()
			if err != nil {
				r.logger.Error("Error accepting connection: ", err)
				continue
			}

			// Set connection read deadline
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))

			r.lock.Lock()
			r.sockets[conn] = true
			r.lock.Unlock()

			// Use worker pool instead of direct goroutine
			r.workerPool.Submit(func() {
				r.handleConnection(conn)
			})
		}
	}()
	return r.port, nil
}

func (r *TcpReader) handleConnection(conn net.Conn) {
	address := conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		r.logger.Error("Failed to split host and port - address:", address)
		host = address // fallback
	}

	r.logger.Debugf("New TCP client connected from '%s'", address)
	defer r.closeSocket(conn)

	for {
		// Reset read deadline on each iteration
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			r.logger.Error("Failed to set read deadline:", err)
			return
		}

		msgType, msgBytes, err := r.readMessage(conn)
		if err != nil {
			// Use structured error handling
			if transportErr, ok := err.(*TransportError); ok {
				r.logger.Errorf("Transport error from '%s': %v", address, transportErr)
			} else if err.Error() == "EOF" {
				r.logger.Debugf("EOF received from '%s'", address)
				r.disconnectNodeByAddress(address)
			} else {
				r.logger.Errorf("Error reading message from '%s': %v", address, err)
			}
			return
		}

		r.logger.Tracef("Message read from socket - msgType: %d, size: %d", msgType, len(msgBytes))
		r.onMessage(host, msgType, &msgBytes)
	}
}

func (r *TcpReader) readMessage(conn net.Conn) (msgType int, msg []byte, err error) {
	var buf []byte
	defer func() {
		// Return buffer to pool when done
		if buf != nil && cap(buf) == 4096 {
			r.bufferPool.Put(buf[:4096])
		}
	}()

	for {
		// Get buffer from pool instead of allocating new one
		chunk := r.bufferPool.Get(1024)
		n, err := conn.Read(chunk)
		if err != nil {
			return 0, nil, NewTransportError("read", err)
		}
		chunk = chunk[:n]

		// If there's a previous chunk, concatenate them
		if buf != nil {
			buf = append(buf, chunk...)
		} else {
			buf = chunk
		}

		// If the buffer is too short, wait for the next chunk
		if len(buf) < 6 {
			continue
		}

		// If the buffer is larger than the max packet size, return an error
		if r.maxPacketSize > 0 && len(buf) > r.maxPacketSize {
			return 0, nil, NewTransportErrorWithNode("read",
				fmt.Sprintf("packet too large: %d > %d", len(buf), r.maxPacketSize), nil)
		}

		length := int(binary.BigEndian.Uint32(buf[1:]))

		// Check the CRC
		crc := buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5]
		if crc != buf[0] {
			r.logger.Errorf("Invalid packet CRC: expected %d, got %d", crc, buf[0])
			return 0, nil, NewTransportError("crc", fmt.Errorf("CRC mismatch: expected %d, got %d", crc, buf[0]))
		}

		// If the buffer contains a complete message, return it
		if len(buf) >= length {
			msg = make([]byte, length-6) // Allocate exact size for message
			copy(msg, buf[6:length])
			msgType = int(buf[5])
			return msgType, msg, nil
		}

		// If the buffer doesn't contain a complete message, wait for the next chunk
	}
}

func (r *TcpReader) closeSocket(conn net.Conn) {
	conn.Close()
	r.lock.Lock()
	delete(r.sockets, conn)
	r.lock.Unlock()
}

func (r *TcpReader) Close() {
	r.state = STOPPED
	if r.listener != nil {
		r.listener.Close()
	}

	// Close all active connections
	r.lock.Lock()
	for conn := range r.sockets {
		conn.Close()
		delete(r.sockets, conn)
	}
	r.lock.Unlock()

	// Stop worker pool
	if r.workerPool != nil {
		r.workerPool.Stop()
	}

	r.logger.Info("TCP reader closed")
}
