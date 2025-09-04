package tcp

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

type State int

const (
	STARTED State = iota
	STOPPED
)

type OnMessageFunc func(fromAddrss string, msgType int, msgBytes *[]byte)

type OnConnectionFunc func(address, host string, port int)

type TcpReader struct {
	port                    int
	listener                net.Listener
	sockets                 map[net.Conn]bool
	logger                  *log.Entry
	lock                    sync.Mutex
	state                   State
	maxPacketSize           int
	onConnection            OnConnectionFunc
	onMessage               OnMessageFunc
	disconnectNodeByAddress func(address string)
	bufferPool              *BufferPool
	workerPool              *WorkerPool
	connectionBuffers       map[net.Conn][]byte // Per-connection buffer state
}

func NewTcpReader(port int, onMessage OnMessageFunc, onConnection OnConnectionFunc, disconnectNodeByAddress func(address string), logger *log.Entry, bufferPool *BufferPool, workerPool *WorkerPool, maxPacketSize int) *TcpReader {
	return &TcpReader{
		port:                    port,
		sockets:                 make(map[net.Conn]bool),
		connectionBuffers:       make(map[net.Conn][]byte),
		logger:                  logger,
		onMessage:               onMessage,
		onConnection:            onConnection,
		disconnectNodeByAddress: disconnectNodeByAddress,
		bufferPool:              bufferPool,
		workerPool:              workerPool,
		maxPacketSize:           maxPacketSize,
	}
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
			r.lock.Lock()
			r.sockets[conn] = true
			r.lock.Unlock()

			// Use worker pool for connection handling
			r.workerPool.Submit(func() {
				r.handleConnection(conn)
			})
		}
	}()
	return r.port, nil
}

func (r *TcpReader) handleConnection(conn net.Conn) {
	address := conn.RemoteAddr().String()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		r.logger.Error("Failed to split host and port - address:", address)
	}
	r.logger.Debugf("New TCP client connected from '%s'\n", address)
	postInt, err := strconv.Atoi(port)
	if err != nil {
		r.logger.Error("Failed to convert port to integer - port:", port)
	}
	r.onConnection(address, host, postInt)
	for err == nil {
		msgType, msgBytes, e := r.readMessage(conn)
		err = e
		if err != nil {

			if err.Error() == "EOF" {
				r.logger.Debugf("EOF received from '%s' ", address)
				r.disconnectNodeByAddress(address)
			} else {
				r.logger.Errorf("Error reading message from '%s': %s", address, err)
			}
			break
		}

		r.onMessage(address, msgType, &msgBytes)
	}
	r.closeSocket(conn)
}

func (r *TcpReader) readMessage(conn net.Conn) (msgType int, msg []byte, err error) {
	// Get or create buffer for this connection
	r.lock.Lock()
	buf, exists := r.connectionBuffers[conn]
	if !exists {
		buf = nil
	}
	r.lock.Unlock()

	for {
		// Read data from the connection using buffer pool
		chunk := r.bufferPool.GetBuffer(1024)
		n, err := conn.Read(chunk)
		if err != nil {
			r.bufferPool.PutBuffer(chunk)
			// Clean up connection buffer
			r.lock.Lock()
			delete(r.connectionBuffers, conn)
			r.lock.Unlock()
			return 0, nil, err
		}
		chunk = chunk[:n]

		// If there's a previous buffer, concatenate them
		if buf != nil {
			// Create new buffer with combined data
			newBuf := make([]byte, len(buf)+len(chunk))
			copy(newBuf, buf)
			copy(newBuf[len(buf):], chunk)
			r.logger.Tracef("TCP READER - Concatenating buffers: old_len=%d, chunk_len=%d, new_len=%d", len(buf), len(chunk), len(newBuf))
			first20 := 20
			if len(newBuf) < 20 {
				first20 = len(newBuf)
			}
			r.logger.Tracef("TCP READER - First 20 bytes of concatenated buffer: %v", newBuf[:first20])
			buf = newBuf
			// Return the chunk to pool
			r.bufferPool.PutBuffer(chunk)
		} else {
			buf = chunk
			first20 := 20
			if len(chunk) < 20 {
				first20 = len(chunk)
			}
			r.logger.Tracef("TCP READER - First chunk: len=%d, first 20 bytes: %v", len(chunk), chunk[:first20])
		}

		// If the buffer is too short, wait for the next chunk
		if len(buf) < 6 {
			// Store the current buffer state for this connection
			r.lock.Lock()
			r.connectionBuffers[conn] = buf
			r.lock.Unlock()
			continue
		}
		// If the buffer is larger than the max packet size, return an error
		if r.maxPacketSize > 0 && len(buf) > r.maxPacketSize {
			// Clean up connection buffer
			r.lock.Lock()
			delete(r.connectionBuffers, conn)
			r.lock.Unlock()
			return 0, nil, fmt.Errorf("incoming packet is larger than the 'maxPacketSize' limit (%d > %d)", len(buf), r.maxPacketSize)
		}

		length := int(binary.BigEndian.Uint32(buf[1:]))

		// Check the CRC
		crc := buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5]
		if crc != buf[0] {
			r.logger.Errorf("invalid packet CRC: %d buf[0]: %d buf: %s", crc, buf[0], string(buf))
			r.logger.Errorf("CRC DEBUG - buf[0]: %d, buf[1]: %d, buf[2]: %d, buf[3]: %d, buf[4]: %d, buf[5]: %d",
				buf[0], buf[1], buf[2], buf[3], buf[4], buf[5])
			r.logger.Errorf("CRC DEBUG - calculated crc: %d, expected: %d", crc, buf[0])
			first20 := 20
			if len(buf) < 20 {
				first20 = len(buf)
			}
			first50 := 50
			if len(buf) < 50 {
				first50 = len(buf)
			}
			r.logger.Errorf("CRC DEBUG - buffer length: %d, first 20 bytes: %v", len(buf), buf[:first20])
			r.logger.Errorf("CRC DEBUG - message starts with: %s", string(buf[:first50]))
			// Clean up connection buffer
			r.lock.Lock()
			delete(r.connectionBuffers, conn)
			r.lock.Unlock()
			return 0, nil, fmt.Errorf("invalid packet CRC: %d buf[0]: %d  ", crc, buf[0])
		}

		// If the buffer contains a complete message, return it
		if len(buf) >= length {
			msg = make([]byte, length-6) // Create new slice for return value
			copy(msg, buf[6:length])
			msgType = int(buf[5])

			// Handle remaining buffer data
			if len(buf) > length {
				// There's more data in the buffer, keep it for the next message
				remaining := make([]byte, len(buf)-length)
				copy(remaining, buf[length:])
				r.lock.Lock()
				r.connectionBuffers[conn] = remaining
				r.lock.Unlock()
			} else {
				// Buffer is empty, remove it from the map
				r.lock.Lock()
				delete(r.connectionBuffers, conn)
				r.lock.Unlock()
			}

			return msgType, msg, nil
		}

		// If the buffer doesn't contain a complete message, wait for the next chunk
	}
}

func (r *TcpReader) closeSocket(conn net.Conn) {
	conn.Close()
	r.lock.Lock()
	delete(r.sockets, conn)
	delete(r.connectionBuffers, conn)
	r.lock.Unlock()
}

func (r *TcpReader) Close() {
	r.state = STOPPED
	r.listener.Close()
	for conn := range r.sockets {
		r.closeSocket(conn)
	}
}
