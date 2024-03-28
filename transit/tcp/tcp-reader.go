package tcp

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
)

type State int

const (
	LISTENING State = iota
	CLOSED
)

type OnMessageFunc func(msgType int, msgBytes *[]byte)

type TcpReader struct {
	port          int
	listener      net.Listener
	sockets       map[net.Conn]bool
	logger        *log.Entry
	lock          sync.Mutex
	state         State
	maxPacketSize int
	onMessage     OnMessageFunc
}

func NewTcpReader(port int, onMessage OnMessageFunc, logger *log.Entry) *TcpReader {
	return &TcpReader{
		port:      port,
		sockets:   make(map[net.Conn]bool),
		logger:    logger,
		onMessage: onMessage,
	}
}

func (r *TcpReader) Listen() {
	var err error
	r.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", r.port))
	if err != nil {
		r.logger.Fatal("Server error: ", err)
	}

	r.logger.Infof("TCP server is listening on port %d", r.port)

	r.state = LISTENING
	go func() {
		for r.state == LISTENING {
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
	r.logger.Debugf("New TCP client connected from '%s'\n", address)

	var err error

	for err == nil {
		msgType, msgBytes, e := r.readMessage(conn)
		err = e
		if err == nil {
			r.logger.Errorf("Error reading message from '%s': %s", address, err)
			break
		}
		r.onMessage(msgType, &msgBytes)
	}

	r.closeSocket(conn)
}

func (r *TcpReader) readMessage(conn net.Conn) (msgType int, msg []byte, err error) {
	var buf []byte

	for {
		// Read data from the connection
		chunk := make([]byte, 256)
		n, err := conn.Read(chunk)
		if err != nil {
			return 0, nil, err
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
			return 0, nil, fmt.Errorf("Incoming packet is larger than the 'maxPacketSize' limit (%d > %d)!", len(buf), r.maxPacketSize)
		}

		// Check the CRC
		crc := buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5]
		if crc != buf[0] {
			return 0, nil, fmt.Errorf("Invalid packet CRC! %d", crc)
		}

		length := int(binary.BigEndian.Uint32(buf[1:]))

		// If the buffer contains a complete message, return it
		if len(buf) >= length {
			msg = buf[6:length]
			msgType = int(buf[5]) // You'll need to replace this with your actual resolvePacketType function
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
	r.state = CLOSED
	r.listener.Close()
	for conn := range r.sockets {
		r.closeSocket(conn)
	}
}
