package udp

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type fakeConn struct {
	reads       []Packet
	writes      []Packet
	readErr     error
	writeErr    error
	batchReads  int
	batchWrites int
	batchN      int
}

type fakeSingleConn struct {
	conn *fakeConn
}

func (conn *fakeSingleConn) ReadFromUDP(buffer []byte) (int, *net.UDPAddr, error) {
	return conn.conn.ReadFromUDP(buffer)
}

func (conn *fakeSingleConn) WriteToUDP(payload []byte, addr *net.UDPAddr) (int, error) {
	return conn.conn.WriteToUDP(payload, addr)
}

func (conn *fakeConn) ReadFromUDP(buffer []byte) (int, *net.UDPAddr, error) {
	if len(conn.reads) == 0 {
		return 0, nil, io.EOF
	}
	packet := conn.reads[0]
	conn.reads = conn.reads[1:]
	copy(buffer, packet.Payload)
	return len(packet.Payload), packet.Addr, conn.readErr
}

func (conn *fakeConn) WriteToUDP(payload []byte, addr *net.UDPAddr) (int, error) {
	conn.writes = append(conn.writes, Packet{Payload: append([]byte(nil), payload...), Addr: addr})
	if conn.writeErr != nil {
		return 0, conn.writeErr
	}
	return len(payload), nil
}

func (conn *fakeConn) ReadBatch(packets []Packet) (int, error) {
	conn.batchReads++
	n := copy(packets, conn.reads)
	conn.reads = conn.reads[n:]
	if conn.batchN > 0 && conn.batchN < n {
		n = conn.batchN
	}
	return n, conn.readErr
}

func (conn *fakeConn) WriteBatch(packets []Packet) (int, error) {
	conn.batchWrites++
	for _, packet := range packets {
		conn.writes = append(conn.writes, Packet{Payload: append([]byte(nil), packet.Payload...), Addr: packet.Addr})
	}
	if conn.batchN > 0 {
		return conn.batchN, conn.writeErr
	}
	return len(packets), conn.writeErr
}

func TestOpenUDP(t *testing.T) {
	conn, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("OpenUDP returned error: %v", err)
	}
	defer conn.Close()
	if conn.LocalAddr() == nil {
		t.Fatal("OpenUDP returned connection without local address")
	}
}

func TestReadBatchFallbackAndBatchPath(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	single := &fakeSingleConn{conn: &fakeConn{reads: []Packet{{Payload: []byte("single"), Addr: addr}}}}
	packets := NewReadBatch(2)
	n, err := ReadBatch(single, packets, true)
	if err != nil || n != 1 || string(packets[0].Payload) != "single" || packets[0].Addr != addr {
		t.Fatalf("fallback read = n:%d err:%v packet:%#v", n, err, packets[0])
	}

	batch := &fakeConn{reads: []Packet{{Payload: []byte("first"), Addr: addr}, {Payload: []byte("second"), Addr: addr}}}
	n, err = ReadBatch(batch, packets, true)
	if err != nil || n != 2 || batch.batchReads != 1 {
		t.Fatalf("batch read = n:%d err:%v batchReads:%d", n, err, batch.batchReads)
	}
}

func TestWriteBatchFallbackAndBatchPath(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	packets := []Packet{{Payload: []byte("first"), Addr: addr}, {Payload: []byte("second"), Addr: addr}}
	single := &fakeSingleConn{conn: &fakeConn{}}
	n, err := WriteBatch(single, packets, true)
	if err != nil || n != 2 || len(single.conn.writes) != 2 {
		t.Fatalf("fallback write = n:%d err:%v writes:%d", n, err, len(single.conn.writes))
	}

	batch := &fakeConn{}
	n, err = WriteBatch(batch, packets, true)
	if err != nil || n != 2 || batch.batchWrites != 1 || len(batch.writes) != 2 {
		t.Fatalf("batch write = n:%d err:%v batchWrites:%d writes:%d", n, err, batch.batchWrites, len(batch.writes))
	}
}

func TestWriteBatchReportsErrorsAndShortWrites(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3), Port: 3}
	writeErr := errors.New("write")
	conn := &fakeConn{writeErr: writeErr}
	packets := []Packet{{Payload: []byte("first"), Addr: addr}, {Payload: []byte("second"), Addr: addr}}
	n, err := WriteBatch(conn, packets, false)
	if !errors.Is(err, writeErr) || n != 0 {
		t.Fatalf("write error = n:%d err:%v", n, err)
	}

	conn = &fakeConn{batchN: 1}
	n, err = WriteBatchChunks(conn, packets, true, 2)
	if !errors.Is(err, io.ErrShortWrite) || n != 1 {
		t.Fatalf("short write = n:%d err:%v", n, err)
	}
}

func TestWrappedConnWriteBatchLoopback(t *testing.T) {
	receiver, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadDeadline(testDeadline()); err != nil {
		t.Fatal(err)
	}

	sender, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	wrapped := Wrap(sender)
	addr := receiver.LocalAddr().(*net.UDPAddr)
	packets := []Packet{{Payload: []byte("first"), Addr: addr}, {Payload: []byte("second"), Addr: addr}}
	n, err := WriteBatch(wrapped, packets, true)
	if err != nil || n != 2 {
		t.Fatalf("wrapped write batch = n:%d err:%v", n, err)
	}

	seen := map[string]bool{}
	buffer := make([]byte, MaxPacketSize)
	for i := 0; i < 2; i++ {
		n, _, err := receiver.ReadFromUDP(buffer)
		if err != nil {
			t.Fatalf("ReadFromUDP returned error: %v", err)
		}
		seen[string(buffer[:n])] = true
	}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("received payloads = %#v", seen)
	}
}

func TestWrappedConnReadBatchLoopback(t *testing.T) {
	receiver, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	wrapped := Wrap(receiver)
	if err := wrapped.SetReadDeadline(testDeadline()); err != nil {
		t.Fatal(err)
	}

	sender, err := OpenUDP(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("loopback"), receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}

	packets := NewReadBatch(2)
	n, err := ReadBatch(wrapped, packets, true)
	if err != nil || n != 1 || string(packets[0].Payload) != "loopback" || packets[0].Addr == nil {
		t.Fatalf("wrapped read batch = n:%d err:%v packet:%#v", n, err, packets[0])
	}
}

func testDeadline() time.Time {
	return time.Now().Add(time.Second)
}
