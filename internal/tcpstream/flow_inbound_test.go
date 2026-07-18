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
	once    sync.Once
	started chan struct{}
}

func (conn *endpointWriteStartedConn) Write(payload []byte) (int, error) {
	conn.once.Do(func() { close(conn.started) })
	return conn.Conn.Write(payload)
}

func TestFlowPeerCloseCancelsBlockedEndpointDelivery(t *testing.T) {
	flow, carrier, peer, _ := newBlockedEndpointFlow(t, 25*time.Millisecond)
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "read", carrier.readDone)
	waitForCarrierLoop(t, "process", carrier.processDone)
	waitForCarrierLoop(t, "flow", flow.Done())
}

func TestFlowCarrierCloseCancelsBlockedEndpointDelivery(t *testing.T) {
	flow, carrier, _, _ := newBlockedEndpointFlow(t, time.Second)
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCarrierLoop(t, "read", carrier.readDone)
	waitForCarrierLoop(t, "process", carrier.processDone)
	waitForCarrierLoop(t, "flow", flow.Done())
}

func TestFlowSlowEndpointCarrierChurnRemainsBounded(t *testing.T) {
	endpoint, application := net.Pipe()
	started := make(chan struct{})
	const maxCarriers = 4
	flow := NewFlow(StreamID{1}, &endpointWriteStartedConn{Conn: endpoint, started: started}, DirectionServerToClient, FlowConfig{
		ChunkSize:          1024,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: MaxPayloadSize,
		WriteTimeout:       time.Minute,
	})
	anchorConn, anchorPeer := net.Pipe()
	anchor, err := flow.AttachLimited(anchorConn, MaxPayloadSize, maxCarriers)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = anchorPeer.Close()
	})

	carriers := []*Carrier{anchor}
	rejected := 0
	for index := range 64 {
		carrierConn, peer := net.Pipe()
		carrier, err := flow.AttachLimited(carrierConn, MaxPayloadSize, maxCarriers)
		if err != nil {
			rejected++
			_ = peer.Close()
			continue
		}
		carriers = append(carriers, carrier)
		writeInboundFrame(t, peer, flow.ID(), []byte("blocked"))
		if index == 0 {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("endpoint write did not start")
			}
		}
		if err := peer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if rejected == 0 {
		t.Fatal("slow endpoint churn never reached the carrier limit")
	}
	if got := flow.CarrierCount(); got > maxCarriers {
		t.Fatalf("carrier count after churn = %d, want at most %d", got, maxCarriers)
	}
	select {
	case <-flow.Done():
		t.Fatal("flow closed while the anchor carrier remained active")
	default:
	}
	if err := flow.Close(); err != nil {
		t.Fatal(err)
	}
	for _, carrier := range carriers {
		waitForCarrierLoop(t, "churn read", carrier.readDone)
		waitForCarrierLoop(t, "churn process", carrier.processDone)
	}
	waitForEmptyInbound(t, flow)
}

func TestFlowEndpointWriteDeadlineResetsStalledDelivery(t *testing.T) {
	flow, carrier, peer, _ := newBlockedEndpointFlow(t, 25*time.Millisecond)
	defer peer.Close()
	waitForCarrierLoop(t, "flow", flow.Done())
	var netErr net.Error
	if err := flow.Err(); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("flow error = %v, want timeout", err)
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
		WriteTimeout:       time.Second,
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
		WriteTimeout:       time.Second,
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

	parts := [][]byte{[]byte("first-"), []byte("second-"), []byte("tail")}
	offset := uint64(0)
	for index, part := range parts {
		writeInboundFrameAt(t, pairs[index].peer, flow.ID(), offset, part)
		offset += uint64(len(part))
		if err := pairs[index].peer.Close(); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("endpoint write did not start")
			}
		}
		if index == 1 {
			waitForInboundQueueLength(t, flow, 1)
		}
	}
	waitForCarrierLoop(t, "tail carrier", pairs[2].carrier.Done())
	select {
	case <-pairs[2].carrier.processDone:
		t.Fatal("full inbound queue dropped the graceful EOF tail")
	default:
	}

	want := []byte("first-second-tail")
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
	flow := NewFlow(StreamID{1}, &endpointWriteStartedConn{Conn: endpoint, started: started}, DirectionServerToClient, FlowConfig{
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
	return flow, carrier, peer, started
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

func waitForInboundQueueLength(t *testing.T, flow *Flow, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(flow.inbound) != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(flow.inbound); got != want {
		t.Fatalf("inbound queue length = %d, want %d", got, want)
	}
}

func waitForEmptyInbound(t *testing.T, flow *Flow) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		flow.mu.Lock()
		pendingInbound := flow.pendingInbound
		flow.mu.Unlock()
		queued := len(flow.inbound)
		if pendingInbound == 0 && queued == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("inbound state after close = pending %d, queued %d", pendingInbound, queued)
		}
		time.Sleep(time.Millisecond)
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
