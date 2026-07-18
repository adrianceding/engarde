package tcpstream

import (
	"io"
	"net"
	"testing"
	"time"
)

const flowBenchmarkPayloadSize = 8 * 1024 * 1024

func BenchmarkFlow(b *testing.B) {
	payload := make([]byte, flowBenchmarkPayloadSize)
	for index := range payload {
		payload[index] = byte(index)
	}

	for _, carrierCount := range []int{1, 2} {
		b.Run(flowBenchmarkName(carrierCount), func(b *testing.B) {
			benchmarkFlowTransfer(b, payload, carrierCount)
		})
	}
}

func benchmarkFlowTransfer(b *testing.B, payload []byte, carrierCount int) {
	pair := newBenchmarkFlowPair(b, carrierCount)
	defer pair.close()

	totalBytes := int64(b.N) * int64(len(payload))
	readStarted := make(chan struct{})
	readDone := make(chan benchmarkFlowReadResult, 1)
	go func() {
		<-readStarted
		read, err := io.CopyN(io.Discard, pair.serverApp, totalBytes)
		readDone <- benchmarkFlowReadResult{bytes: read, err: err}
	}()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	close(readStarted)

	var writeErr error
	for range b.N {
		if writeErr = writeFull(pair.clientApp, payload); writeErr != nil {
			break
		}
	}
	result := <-readDone
	b.StopTimer()

	if writeErr != nil {
		b.Fatalf("write payload: %v", writeErr)
	}
	if result.err != nil {
		b.Fatalf("consume payload: read %d of %d bytes: %v", result.bytes, totalBytes, result.err)
	}
	if result.bytes != totalBytes {
		b.Fatalf("consumed %d bytes, want %d", result.bytes, totalBytes)
	}
}

type benchmarkFlowPair struct {
	clientApp  net.Conn
	serverApp  net.Conn
	clientFlow *Flow
	serverFlow *Flow
}

type benchmarkFlowReadResult struct {
	bytes int64
	err   error
}

func newBenchmarkFlowPair(b *testing.B, carrierCount int) *benchmarkFlowPair {
	b.Helper()

	clientApp, clientEndpoint := net.Pipe()
	serverEndpoint, serverApp := net.Pipe()
	deadline := time.Now().Add(30 * time.Second)
	if err := clientApp.SetDeadline(deadline); err != nil {
		b.Fatal(err)
	}
	if err := serverApp.SetDeadline(deadline); err != nil {
		b.Fatal(err)
	}

	streamID := StreamID{1}
	config := FlowConfig{
		ChunkSize:          16 * 1024,
		CarrierQueueBytes:  1024 * 1024,
		ReorderWindowBytes: 4 * 1024 * 1024,
		WriteTimeout:       10 * time.Second,
	}
	clientFlow := NewFlow(streamID, clientEndpoint, DirectionClientToServer, config)
	serverFlow := NewFlow(streamID, serverEndpoint, DirectionServerToClient, config)
	pair := &benchmarkFlowPair{
		clientApp:  clientApp,
		serverApp:  serverApp,
		clientFlow: clientFlow,
		serverFlow: serverFlow,
	}
	for range carrierCount {
		clientCarrier, serverCarrier := net.Pipe()
		if _, err := clientFlow.Attach(clientCarrier, MaxPayloadSize); err != nil {
			pair.close()
			b.Fatal(err)
		}
		if _, err := serverFlow.Attach(serverCarrier, MaxPayloadSize); err != nil {
			pair.close()
			b.Fatal(err)
		}
	}
	clientFlow.Start()
	serverFlow.Start()
	return pair
}

func (pair *benchmarkFlowPair) close() {
	_ = pair.clientFlow.Close()
	_ = pair.serverFlow.Close()
	_ = pair.clientApp.Close()
	_ = pair.serverApp.Close()
	<-pair.clientFlow.Done()
	<-pair.serverFlow.Done()
}

func flowBenchmarkName(carrierCount int) string {
	if carrierCount == 1 {
		return "1_carrier"
	}
	return "2_carriers"
}
