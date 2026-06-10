package udp

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"

	"golang.org/x/net/ipv4"
)

type Conn struct {
	*net.UDPConn
	packetConn *ipv4.PacketConn
	readMu     sync.Mutex
	readMsgs   []ipv4.Message
	writeMu    sync.Mutex
	writeMsgs  []ipv4.Message
	writeBufs  [][1][]byte
}

func Wrap(conn *net.UDPConn) *Conn {
	if conn == nil {
		return nil
	}
	return &Conn{UDPConn: conn, packetConn: ipv4.NewPacketConn(conn)}
}

func (conn *Conn) ReadBatch(packets []Packet) (int, error) {
	if conn == nil || conn.packetConn == nil {
		return 0, errNilConn
	}
	if len(packets) == 0 {
		return 0, nil
	}
	if runtime.GOOS != "linux" {
		n, addr, err := conn.UDPConn.ReadFromUDP(packets[0].Payload)
		if err == nil || n > 0 {
			packets[0].Payload = packets[0].Payload[:n]
			packets[0].Addr = addr
			return 1, err
		}
		return 0, err
	}
	prepareReadBatch(packets)
	conn.readMu.Lock()
	defer conn.readMu.Unlock()
	if cap(conn.readMsgs) < len(packets) {
		conn.readMsgs = make([]ipv4.Message, len(packets))
	} else {
		conn.readMsgs = conn.readMsgs[:len(packets)]
	}
	messages := conn.readMsgs
	for i := range packets {
		messages[i].Buffers = packets[i].buffers
	}

	n, err := conn.packetConn.ReadBatch(messages, 0)
	for i := 0; i < n; i++ {
		packets[i].Payload = packets[i].Payload[:messages[i].N]
		addr, ok := messages[i].Addr.(*net.UDPAddr)
		if !ok {
			if err == nil {
				err = fmt.Errorf("read batch address is %T, not *net.UDPAddr", messages[i].Addr)
			}
			continue
		}
		packets[i].Addr = addr
	}
	return n, err
}

func (conn *Conn) WriteBatch(packets []Packet) (int, error) {
	if conn == nil || conn.packetConn == nil {
		return 0, errNilConn
	}
	if len(packets) == 0 {
		return 0, nil
	}
	if runtime.GOOS != "linux" {
		for i, packet := range packets {
			n, err := conn.UDPConn.WriteToUDP(packet.Payload, packet.Addr)
			if err != nil {
				return i, err
			}
			if n != len(packet.Payload) {
				return i, io.ErrShortWrite
			}
		}
		return len(packets), nil
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	if cap(conn.writeMsgs) < len(packets) {
		conn.writeMsgs = make([]ipv4.Message, len(packets))
	} else {
		conn.writeMsgs = conn.writeMsgs[:len(packets)]
	}
	if cap(conn.writeBufs) < len(packets) {
		conn.writeBufs = make([][1][]byte, len(packets))
	} else {
		conn.writeBufs = conn.writeBufs[:len(packets)]
	}
	messages := conn.writeMsgs
	buffers := conn.writeBufs
	for i, packet := range packets {
		buffers[i][0] = packet.Payload
		messages[i].Buffers = buffers[i][:]
		messages[i].Addr = packet.Addr
	}
	return conn.packetConn.WriteBatch(messages, 0)
}
