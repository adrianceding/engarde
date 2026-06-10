package relay

import (
	"net"
	"testing"

	"github.com/adrianceding/engarde/internal/udp"
)

func BenchmarkTargetWorkerWriteFallback(b *testing.B) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 20), Port: 20}
	writer := &fakeWriter{}
	worker := &connWorker{conn: writer, writeBatchEnabled: true, writeBatchSize: DefaultWriteBatchSize}
	queued, packets := benchmarkQueuedPackets(writer, addr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.writeChunk(queued, packets)
	}
}

func BenchmarkTargetWorkerWriteBatch(b *testing.B) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 21), Port: 21}
	writer := &fakeBatchWriter{}
	worker := &connWorker{conn: writer, writeBatchEnabled: true, writeBatchSize: DefaultWriteBatchSize}
	queued, packets := benchmarkQueuedPackets(writer, addr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.writeChunk(queued, packets)
	}
}

func benchmarkQueuedPackets(conn UDPWriter, addr *net.UDPAddr) ([]queuedPacket, []udp.Packet) {
	queued := make([]queuedPacket, DefaultWriteBatchSize)
	packets := make([]udp.Packet, DefaultWriteBatchSize)
	for i := range packets {
		queued[i] = queuedPacket{id: "target", conn: conn, addr: addr, addrKey: addr.String(), payload: []byte("payload")}
		packets[i] = udp.Packet{Payload: []byte("payload"), Addr: addr}
	}
	return queued, packets
}
