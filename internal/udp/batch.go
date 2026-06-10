package udp

import (
	"errors"
	"io"
	"net"
)

const (
	MaxPacketSize    = 1500
	DefaultBatchSize = 32
)

var errNilConn = errors.New("nil udp connection")

type Packet struct {
	Payload []byte
	Addr    *net.UDPAddr

	buffers [][]byte
}

type Reader interface {
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
}

type Writer interface {
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
}

type BatchReader interface {
	ReadBatch([]Packet) (int, error)
}

type BatchWriter interface {
	WriteBatch([]Packet) (int, error)
}

func NormalizeBatchSize(size int) int {
	if size > 0 {
		return size
	}
	return DefaultBatchSize
}

func NewReadBatch(size int) []Packet {
	packets := make([]Packet, NormalizeBatchSize(size))
	prepareReadBatch(packets)
	return packets
}

func ReadBatch(conn Reader, packets []Packet, enabled bool) (int, error) {
	if conn == nil {
		return 0, errNilConn
	}
	if len(packets) == 0 {
		return 0, nil
	}
	if enabled && len(packets) > 1 {
		if reader, ok := conn.(BatchReader); ok {
			return reader.ReadBatch(packets)
		}
	}

	prepareReadBatch(packets[:1])
	n, addr, err := conn.ReadFromUDP(packets[0].Payload)
	if err == nil || n > 0 {
		packets[0].Payload = packets[0].Payload[:n]
		packets[0].Addr = addr
		return 1, err
	}
	return 0, err
}

func WriteBatch(conn Writer, packets []Packet, enabled bool) (int, error) {
	if conn == nil {
		return 0, errNilConn
	}
	if len(packets) == 0 {
		return 0, nil
	}
	if enabled && len(packets) > 1 {
		if writer, ok := conn.(BatchWriter); ok {
			return writer.WriteBatch(packets)
		}
	}

	for i, packet := range packets {
		n, err := conn.WriteToUDP(packet.Payload, packet.Addr)
		if err != nil {
			return i, err
		}
		if n != len(packet.Payload) {
			return i, io.ErrShortWrite
		}
	}
	return len(packets), nil
}

func WriteBatchChunks(conn Writer, packets []Packet, enabled bool, size int) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	batchSize := NormalizeBatchSize(size)
	written := 0
	for len(packets) > 0 {
		chunkSize := batchSize
		if chunkSize > len(packets) {
			chunkSize = len(packets)
		}
		n, err := WriteBatch(conn, packets[:chunkSize], enabled)
		written += n
		if err != nil {
			return written, err
		}
		if n != chunkSize {
			return written, io.ErrShortWrite
		}
		packets = packets[chunkSize:]
	}
	return written, nil
}

func prepareReadBatch(packets []Packet) {
	for i := range packets {
		packets[i].prepareReadBuffer()
	}
}

func (packet *Packet) prepareReadBuffer() {
	if cap(packet.Payload) < MaxPacketSize {
		packet.Payload = make([]byte, MaxPacketSize)
	} else {
		packet.Payload = packet.Payload[:MaxPacketSize]
	}
	packet.Addr = nil
	if cap(packet.buffers) == 0 {
		packet.buffers = make([][]byte, 1)
	} else {
		packet.buffers = packet.buffers[:1]
	}
	packet.buffers[0] = packet.Payload
}
