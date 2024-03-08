package tcp

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
)

type Parser struct {
	conn          *net.TCPConn
	maxPacketSize int
	logger        *log.Entry
}

func NewParser(conn *net.TCPConn, maxPacketSize int, logger *log.Entry) *Parser {
	return &Parser{
		conn:          conn,
		maxPacketSize: maxPacketSize,
		logger:        logger,
	}
}

const HeaderSize = 6

func (p *Parser) StartParsing(handler func(packetType byte, data []byte)) error {
	buf := make([]byte, 0, 4096) // Initial buffer size, adjust based on needs
	tmp := make([]byte, 1024)    // Temporary buffer for reading from connection

	for {
		n, err := p.conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				return err // Handle error (excluding EOF)
			}
			break
		}

		buf = append(buf, tmp[:n]...)

		for {
			if len(buf) < HeaderSize {
				break // Wait for more data
			}

			if p.maxPacketSize > 0 && len(buf) > p.maxPacketSize {
				return errors.New("incoming packet is larger than the 'maxPacketSize' limit")
			}

			length := int(binary.BigEndian.Uint32(buf[1:5]))
			if len(buf) < length {
				break // Wait for complete packet
			}

			crc := buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5]
			if crc != buf[0] {
				return errors.New("invalid packet CRC")
			}

			packetType := buf[5]
			data := buf[HeaderSize:length]
			handler(packetType, data)

			buf = buf[length:] // Move to next packet
		}
	}

	return nil
}
