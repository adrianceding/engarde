package tcpstream

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type endpointWriteStartedConn struct {
	net.Conn
	once       sync.Once
	returnOnce sync.Once
	started    chan struct{}
	returned   chan struct{}
}

func (conn *endpointWriteStartedConn) Write(payload []byte) (int, error) {
	conn.once.Do(func() { close(conn.started) })
	written, err := conn.Conn.Write(payload)
	if conn.returned != nil {
		conn.returnOnce.Do(func() { close(conn.returned) })
	}
	return written, err
}

type immediateTimeoutError struct{}

func (immediateTimeoutError) Error() string   { return "controlled endpoint timeout" }
func (immediateTimeoutError) Timeout() bool   { return true }
func (immediateTimeoutError) Temporary() bool { return true }

type endpointTimeoutConn struct {
	net.Conn
	deadlineSet chan struct{}
	once        sync.Once
}

func (conn *endpointTimeoutConn) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		conn.once.Do(func() { close(conn.deadlineSet) })
	}
	return nil
}

func (conn *endpointTimeoutConn) Write([]byte) (int, error) {
	<-conn.deadlineSet
	return 0, immediateTimeoutError{}
}

func TestFlowCloseCancelsBlockedEndpointDelivery(t *testing.T) {
	flow, carrier, _, returned := newBlockedEndpointFlow(t, 0)
	if err := flow.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "read", carrier.readDone)
	waitForCarrierLoop(t, "process", carrier.processDone)
	waitForCarrierLoop(t, "flow", flow.Done())
	waitForCarrierLoop(t, "endpoint write return", returned)
}

func TestFlowCarrierCloseCancelsBlockedEndpointDelivery(t *testing.T) {
	flow, carrier, _, returned := newBlockedEndpointFlow(t, 0)
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "read", carrier.readDone)
	waitForCarrierLoop(t, "process", carrier.processDone)
	waitForCarrierLoop(t, "flow", flow.Done())
	waitForCarrierLoop(t, "endpoint write return", returned)
}

func TestFlowAttachLimitedRejectsOnlyCarrierBeyondExactLimit(t *testing.T) {
	endpoint, application := net.Pipe()
	const maxCarriers = 4
	flow := NewFlow(StreamID{1}, endpoint, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
	})
	carriers := make([]*Carrier, 0, maxCarriers)
	peers := make([]net.Conn, 0, maxCarriers+2)
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	for index := range maxCarriers {
		carrierConn, peer := net.Pipe()
		carrier, err := flow.AttachLimited(carrierConn, MaxPayloadSize, maxCarriers)
		if err != nil {
			t.Fatalf("carrier %d attach: %v", index+1, err)
		}
		carriers = append(carriers, carrier)
		peers = append(peers, peer)
		if got := flow.CarrierCount(); got != index+1 {
			t.Fatalf("carrier count after attach %d = %d, want %d", index+1, got, index+1)
		}
	}

	extraConn, extraPeer := net.Pipe()
	peers = append(peers, extraPeer)
	if carrier, err := flow.AttachLimited(extraConn, MaxPayloadSize, maxCarriers); err == nil {
		_ = carrier.Close()
		t.Fatal("carrier beyond the configured limit was accepted")
	}
	if got := flow.CarrierCount(); got != maxCarriers {
		t.Fatalf("carrier count after rejected attach = %d, want %d", got, maxCarriers)
	}

	if err := carriers[0].Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "detached carrier", carriers[0].Detached())
	if got := flow.CarrierCount(); got != maxCarriers-1 {
		t.Fatalf("carrier count after detach = %d, want %d", got, maxCarriers-1)
	}
	replacementConn, replacementPeer := net.Pipe()
	peers = append(peers, replacementPeer)
	if _, err := flow.AttachLimited(replacementConn, MaxPayloadSize, maxCarriers); err != nil {
		t.Fatal(err)
	}
	if got := flow.CarrierCount(); got != maxCarriers {
		t.Fatalf("carrier count after replacement = %d, want %d", got, maxCarriers)
	}
}

func TestFlowEndpointWriteDeadlineResetsStalledDelivery(t *testing.T) {
	endpoint, application := net.Pipe()
	deadlineSet := make(chan struct{})
	carrierConn, peer := net.Pipe()
	flow := NewFlow(StreamID{1}, &endpointTimeoutConn{Conn: endpoint, deadlineSet: deadlineSet}, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
		WriteTimeout:       time.Second,
	})
	carrier, err := flow.Attach(carrierConn, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})
	type frameResult struct {
		frame Frame
		err   error
	}
	resetFrame := make(chan frameResult, 1)
	go func() {
		frame, err := ReadFrame(peer, MaxPayloadSize)
		resetFrame <- frameResult{frame: frame, err: err}
	}()
	writeInboundFrame(t, peer, flow.ID(), []byte("controlled timeout"))
	waitForCarrierLoop(t, "write deadline", deadlineSet)
	result := waitForResult(t, resetFrame, "flow reset frame")
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.frame.Type != FrameRST {
		t.Fatalf("reset frame type = %d, want RST", result.frame.Type)
	}
	waitForCarrierLoop(t, "flow", flow.Done())
	var netErr net.Error
	if flowErr := flow.Err(); !errors.As(flowErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("flow error = %v, want timeout", flowErr)
	}
	waitForCarrierLoop(t, "read", carrier.readDone)
	waitForCarrierLoop(t, "process", carrier.processDone)
}

func TestFlowQueuesInboundBeforeStart(t *testing.T) {
	endpoint, application := net.Pipe()
	carrierConn, peer := net.Pipe()
	flow := NewFlow(StreamID{1}, endpoint, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
	})
	if _, err := flow.Attach(carrierConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})

	payload := []byte("queued before start")
	writeInboundFrame(t, peer, flow.ID(), payload)
	flow.Start()
	readDone := make(chan error, 1)
	got := make([]byte, len(payload))
	go func() {
		_, err := io.ReadFull(application, got)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued inbound payload was not delivered after Start")
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if _, err := ReadFrame(peer, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
}

func TestFlowDeliversLastFrameBeforePeerClose(t *testing.T) {
	endpoint, application := net.Pipe()
	carrierConn, peer := net.Pipe()
	started := make(chan struct{})
	flow := NewFlow(StreamID{1}, &endpointWriteStartedConn{Conn: endpoint, started: started}, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
	})
	carrier, err := flow.Attach(carrierConn, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})

	payload := []byte("last frame before peer close")
	peerClosed := make(chan error, 1)
	go func() {
		err := WriteFrame(peer, Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: flow.ID(), Payload: payload})
		if err == nil {
			err = peer.Close()
		}
		peerClosed <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("endpoint delivery did not start")
	}
	select {
	case err := <-peerClosed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not close after writing the last frame")
	}
	waitForCarrierLoop(t, "carrier", carrier.Done())
	waitForCarrierLoop(t, "carrier read", carrier.readDone)
	waitForCarrierLoop(t, "carrier process", carrier.processDone)

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	waitForCarrierLoop(t, "flow", flow.Done())
}

func TestFlowDeliversGracefulEOFTailWhenInboundQueueIsFull(t *testing.T) {
	endpoint, application := net.Pipe()
	started := make(chan struct{})
	flow := NewFlow(StreamID{1}, &endpointWriteStartedConn{Conn: endpoint, started: started}, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
	})
	type carrierPeer struct {
		carrier *Carrier
		peer    net.Conn
	}
	pairs := make([]carrierPeer, 0, 3)
	for range 3 {
		carrierConn, peer := net.Pipe()
		carrier, err := flow.Attach(carrierConn, MaxPayloadSize)
		if err != nil {
			t.Fatal(err)
		}
		pairs = append(pairs, carrierPeer{carrier: carrier, peer: peer})
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		for _, pair := range pairs {
			_ = pair.peer.Close()
		}
	})

	first := []byte("first-")
	second := []byte("second-")
	tail := []byte("tail")
	writeInboundFrameAt(t, pairs[0].peer, flow.ID(), 0, first)
	if err := pairs[0].peer.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "endpoint write", started)

	// The first frame is blocked in endpoint.Write. Queue the second frame
	// synchronously so the tail has a precisely established full queue.
	flow.mu.Lock()
	flow.pendingInbound++
	flow.mu.Unlock()
	flow.inbound <- inboundFrame{source: pairs[1].carrier, frame: Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  flow.ID(),
		Offset:    uint64(len(first)),
		Payload:   second,
	}}
	if err := pairs[1].peer.Close(); err != nil {
		t.Fatal(err)
	}

	tailOffset := uint64(len(first) + len(second))
	peerClosed := make(chan error, 1)
	go func() {
		err := WriteFrame(pairs[2].peer, Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: flow.ID(), Offset: tailOffset, Payload: tail})
		if err == nil {
			err = pairs[2].peer.Close()
		}
		peerClosed <- err
	}()
	if err := waitForResult(t, peerClosed, "tail peer close"); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "tail carrier", pairs[2].carrier.Done())
	select {
	case <-pairs[2].carrier.processDone:
		t.Fatal("full inbound queue dropped the graceful EOF tail")
	default:
	}

	want := append(append(append([]byte(nil), first...), second...), tail...)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(application, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	waitForCarrierLoop(t, "flow", flow.Done())
	for _, pair := range pairs {
		waitForCarrierLoop(t, "carrier read", pair.carrier.readDone)
		waitForCarrierLoop(t, "carrier process", pair.carrier.processDone)
	}
}

func newBlockedEndpointFlow(t *testing.T, writeTimeout time.Duration) (*Flow, *Carrier, net.Conn, <-chan struct{}) {
	t.Helper()
	endpoint, application := net.Pipe()
	carrierConn, peer := net.Pipe()
	started := make(chan struct{})
	returned := make(chan struct{})
	flow := NewFlow(StreamID{1}, &endpointWriteStartedConn{Conn: endpoint, started: started, returned: returned}, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
		WriteTimeout:       writeTimeout,
	})
	carrier, err := flow.Attach(carrierConn, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})
	writeInboundFrame(t, peer, flow.ID(), []byte("blocked endpoint payload"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("endpoint write did not start")
	}
	return flow, carrier, peer, returned
}

func writeInboundFrame(t *testing.T, peer net.Conn, streamID StreamID, payload []byte) {
	writeInboundFrameAt(t, peer, streamID, 0, payload)
}

func writeInboundFrameAt(t *testing.T, peer net.Conn, streamID StreamID, offset uint64, payload []byte) {
	t.Helper()
	written := make(chan error, 1)
	go func() {
		written <- WriteFrame(peer, Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: offset, Payload: payload})
	}()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("carrier peer could not write inbound frame")
	}
}

func waitForResult[T any](t *testing.T, result <-chan T, event string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", event)
		var zero T
		return zero
	}
}

func waitForCarrierLoop(t *testing.T, name string, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not stop", name)
	}
}
