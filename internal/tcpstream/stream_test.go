package tcpstream

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestFlowsDeduplicateAcrossTwoCarriers(t *testing.T) {
	clientApp, clientEndpoint := net.Pipe()
	serverEndpoint, serverApp := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 4, CarrierQueueBytes: 4096, ReorderWindowBytes: 4096, WriteTimeout: time.Second}
	clientFlow := NewFlow(streamID, clientEndpoint, DirectionClientToServer, config)
	serverFlow := NewFlow(streamID, serverEndpoint, DirectionServerToClient, config)
	for range 2 {
		clientCarrier, serverCarrier := net.Pipe()
		if _, err := clientFlow.Attach(clientCarrier, MaxPayloadSize); err != nil {
			t.Fatal(err)
		}
		if _, err := serverFlow.Attach(serverCarrier, MaxPayloadSize); err != nil {
			t.Fatal(err)
		}
	}
	clientFlow.Start()
	serverFlow.Start()
	t.Cleanup(func() {
		clientFlow.Close()
		serverFlow.Close()
		clientApp.Close()
		serverApp.Close()
	})

	want := []byte("hello over redundant tcp")
	go func() { _, _ = clientApp.Write(want) }()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(serverApp, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if clientFlow.CarrierCount() != 2 || serverFlow.CarrierCount() != 2 {
		t.Fatalf("carrier counts = %d/%d", clientFlow.CarrierCount(), serverFlow.CarrierCount())
	}
}

func TestFlowResetsWhenAllCarriersClose(t *testing.T) {
	application, endpoint := net.Pipe()
	carrierEndpoint, peer := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 16, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	if _, err := flow.Attach(carrierEndpoint, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	flow.Start()
	peer.Close()
	select {
	case <-flow.Done():
	case <-time.After(time.Second):
		t.Fatal("flow did not close after its last carrier")
	}
	application.Close()
}

func TestFlowContinuesAfterFirstCarrierCloses(t *testing.T) {
	clientApp, clientEndpoint := net.Pipe()
	serverEndpoint, serverApp := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 16, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second}
	clientFlow := NewFlow(streamID, clientEndpoint, DirectionClientToServer, config)
	serverFlow := NewFlow(streamID, serverEndpoint, DirectionServerToClient, config)
	firstClientCarrier, firstServerCarrier := net.Pipe()
	secondClientCarrier, secondServerCarrier := net.Pipe()
	if _, err := clientFlow.Attach(firstClientCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	if _, err := serverFlow.Attach(firstServerCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	if _, err := clientFlow.Attach(secondClientCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	if _, err := serverFlow.Attach(secondServerCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	clientFlow.Start()
	serverFlow.Start()
	t.Cleanup(func() {
		clientFlow.Close()
		serverFlow.Close()
		clientApp.Close()
		serverApp.Close()
	})

	firstClientCarrier.Close()
	deadline := time.Now().Add(time.Second)
	for clientFlow.CarrierCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go func() { _, _ = clientApp.Write([]byte("survives")) }()
	got := make([]byte, len("survives"))
	if _, err := io.ReadFull(serverApp, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "survives" {
		t.Fatalf("got %q", got)
	}
}

type closeBarrierConn struct {
	net.Conn
	started chan struct{}
	release <-chan struct{}
}

func (conn *closeBarrierConn) Close() error {
	close(conn.started)
	<-conn.release
	return conn.Conn.Close()
}

func TestFlowMigratesLatestAcknowledgementBeforePlatformCloseReturns(t *testing.T) {
	application, endpoint := net.Pipe()
	flow := NewFlow(StreamID{1}, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 16, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	firstConn, firstPeer := net.Pipe()
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	first, err := flow.Attach(&closeBarrierConn{Conn: firstConn, started: closeStarted, release: releaseClose}, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	secondConn, secondPeer := net.Pipe()
	if _, err := flow.Attach(secondConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClose) }) }
	t.Cleanup(func() {
		release()
		_ = flow.Close()
		_ = application.Close()
		_ = firstPeer.Close()
		_ = secondPeer.Close()
	})

	latest := flow.ackFrame(64, true)
	flow.mu.Lock()
	flow.latestAck = latest
	flow.hasLatestAck = true
	flow.mu.Unlock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- first.Close() }()
	waitForSignal(t, closeStarted, "platform carrier close")
	if err := secondPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadFrame(secondPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 64 || !ackHasFIN(ack) {
		t.Fatalf("migrated acknowledgement = %#v, want final offset 64", ack)
	}
	release()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("carrier close did not finish after release")
	}
}

func TestFlowSeedsLatestAcknowledgementOnNewCarrier(t *testing.T) {
	application, endpoint := net.Pipe()
	flow := NewFlow(StreamID{1}, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 16, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	flow.enqueueAck(nil, flow.ackFrame(96, false))
	carrierConn, peer := net.Pipe()
	if _, err := flow.Attach(carrierConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})

	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadFrame(peer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 96 {
		t.Fatalf("seeded acknowledgement = %#v, want offset 96", ack)
	}
}

func TestFlowDefersAcknowledgementUntilWriteProgressCommits(t *testing.T) {
	application, endpoint := net.Pipe()
	flow := NewFlow(StreamID{1}, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 16, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	carrierConn, peer := net.Pipe()
	writeObserved := make(chan struct{})
	releaseWrite := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWrite) }) }
	carrier, err := flow.AttachObserved(carrierConn, MaxPayloadSize, CarrierObserver{Write: func(Frame) {
		close(writeObserved)
		<-releaseWrite
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})

	payload := []byte("deferred acknowledgement")
	broadcastDone := make(chan error, 1)
	go func() {
		broadcastDone <- flow.broadcast(Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: flow.ID(), Payload: payload})
	}()
	if _, err := ReadFrame(peer, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, writeObserved, "carrier write observer")
	flow.acknowledge(carrier, uint64(len(payload)), false)
	flow.mu.Lock()
	deferred := flow.hasDeferredAck
	ackOffset := flow.globalAckOffset
	flow.mu.Unlock()
	if !deferred || ackOffset != 0 {
		t.Fatalf("early acknowledgement state = deferred %v, offset %d", deferred, ackOffset)
	}

	release()
	select {
	case err := <-broadcastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast did not finish")
	}
	deadline := time.Now().Add(time.Second)
	for {
		flow.mu.Lock()
		ackOffset = flow.globalAckOffset
		deferred = flow.hasDeferredAck
		historyLength := len(flow.history)
		flow.mu.Unlock()
		if ackOffset == uint64(len(payload)) && !deferred && historyLength == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed acknowledgement state = deferred %v, offset %d, history %d", deferred, ackOffset, historyLength)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFlowDoesNotWaitForSlowCarrierAndReplaysBacklog(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 32, CarrierQueueBytes: HeaderSize + 32, ReorderWindowBytes: 1024, WriteTimeout: time.Second}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, config)
	fastCarrier, fastPeer := net.Pipe()
	slowCarrier, slowPeer := net.Pipe()
	if _, err := flow.Attach(fastCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	if _, err := flow.Attach(slowCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		flow.Close()
		application.Close()
		fastPeer.Close()
		slowPeer.Close()
	})

	fastFrames := make(chan Frame, 2)
	go readFrames(fastPeer, fastFrames, 2)
	done := make(chan error, 1)
	go func() {
		if err := flow.broadcast(Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte("first")}); err != nil {
			done <- err
			return
		}
		done <- flow.broadcast(Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 5, Payload: []byte("second")})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("broadcast waited for slow carrier")
	}
	slowFrames := make(chan Frame, 2)
	go readFrames(slowPeer, slowFrames, 2)
	for _, frames := range []chan Frame{fastFrames, slowFrames} {
		var first, second Frame
		select {
		case first = <-frames:
		case <-time.After(time.Second):
			t.Fatal("first frame was not delivered")
		}
		select {
		case second = <-frames:
		case <-time.After(time.Second):
			t.Fatal("second frame was not delivered")
		}
		if string(first.Payload) != "first" || string(second.Payload) != "second" {
			t.Fatalf("payloads = %q/%q", first.Payload, second.Payload)
		}
	}
	if flow.CarrierCount() != 2 {
		t.Fatalf("carrier count = %d, want 2", flow.CarrierCount())
	}
}

func TestFlowBroadcastCompletesAfterAcknowledgedCarrierDetaches(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, FlowConfig{ReorderWindowBytes: 1024})
	first := new(Carrier)
	replacement := new(Carrier)
	flow.mu.Lock()
	flow.carriers = []*Carrier{first}
	flow.carrierStates[first] = &carrierState{}
	flow.mu.Unlock()
	t.Cleanup(func() {
		flow.mu.Lock()
		flow.carriers = nil
		flow.carrierStates = make(map[*Carrier]*carrierState)
		flow.mu.Unlock()
		flow.Close()
		application.Close()
		endpoint.Close()
	})

	frame := Frame{Type: FrameFIN, Direction: DirectionClientToServer, StreamID: streamID, Offset: 64}
	broadcastDone := make(chan error, 1)
	go func() {
		broadcastDone <- flow.broadcast(frame)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		flow.mu.Lock()
		remembered := len(flow.history) == 1
		flow.mu.Unlock()
		if remembered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FIN was not remembered")
		}
		time.Sleep(time.Millisecond)
	}

	flow.mu.Lock()
	sequence := flow.history[0].sequence
	state := flow.carrierStates[first]
	state.nextSequence = sequence + 1
	state.writtenSequence = sequence
	state.hasWritten = true
	state.writtenOffset = frame.Offset
	state.writtenFIN = true
	flow.writtenOffset = frame.Offset
	flow.writtenFIN = true
	flow.writtenFINOffset = frame.Offset
	flow.applyAcknowledgementLocked(frame.Offset, true)
	flow.carriers = []*Carrier{replacement}
	delete(flow.carrierStates, first)
	flow.carrierStates[replacement] = &carrierState{nextSequence: sequence + 1}
	flow.cond.Broadcast()
	flow.mu.Unlock()

	select {
	case err := <-broadcastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast waited after its FIN was globally acknowledged")
	}
}

func TestFlowReportsGloballyAcknowledgedSlowCarrierBacklogAsSkipped(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 32, CarrierQueueBytes: HeaderSize + 32, ReorderWindowBytes: 1024, WriteTimeout: time.Second}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, config)
	fastCarrier, fastPeer := net.Pipe()
	fastSkips := make(chan Frame, 1)
	fast, err := flow.AttachObserved(fastCarrier, MaxPayloadSize, CarrierObserver{Skip: func(frame Frame) {
		fastSkips <- frame
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		flow.Close()
		application.Close()
		fastPeer.Close()
	})

	fastFrames := make(chan Frame, 3)
	go readFrames(fastPeer, fastFrames, 3)
	for _, frame := range []Frame{
		{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 0, Payload: []byte("first")},
		{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 5, Payload: []byte("second")},
	} {
		if err := flow.broadcast(frame); err != nil {
			t.Fatal(err)
		}
	}
	if first := receiveFrame(t, fastFrames, "first fast frame"); string(first.Payload) != "first" {
		t.Fatalf("first fast payload = %q, want first", first.Payload)
	}
	if second := receiveFrame(t, fastFrames, "second fast frame"); string(second.Payload) != "second" {
		t.Fatalf("second fast payload = %q, want second", second.Payload)
	}

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlowWrite := func() {
		releaseOnce.Do(func() { close(releaseWrite) })
	}
	slowCarrier, slowPeer := net.Pipe()
	slowSkips := make(chan Frame, 2)
	slowWrites := make(chan Frame, 2)
	if _, err := flow.AttachObserved(&writeBarrierConn{Conn: slowCarrier, started: writeStarted, release: releaseWrite}, MaxPayloadSize, CarrierObserver{
		Write: func(frame Frame) { slowWrites <- frame },
		Skip:  func(frame Frame) { slowSkips <- frame },
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseSlowWrite()
		slowPeer.Close()
	})
	waitForSignal(t, writeStarted, "slow carrier write")

	flow.acknowledge(fast, 11, false)
	skipped := receiveFrame(t, slowSkips, "slow carrier skip")
	if string(skipped.Payload) != "second" {
		t.Fatalf("skipped payload = %q, want second", skipped.Payload)
	}
	select {
	case frame := <-slowSkips:
		t.Fatalf("unexpected additional slow skip %q", frame.Payload)
	default:
	}
	select {
	case frame := <-fastSkips:
		t.Fatalf("fast carrier unexpectedly skipped %q", frame.Payload)
	default:
	}

	releaseSlowWrite()
	first, err := ReadFrame(slowPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Payload) != "first" {
		t.Fatalf("first slow payload = %q, want first", first.Payload)
	}
	if written := receiveFrame(t, slowWrites, "first slow write"); string(written.Payload) != "first" {
		t.Fatalf("first observed slow write = %q, want first", written.Payload)
	}

	if err := flow.broadcast(Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 11, Payload: []byte("third")}); err != nil {
		t.Fatal(err)
	}
	next, err := ReadFrame(slowPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if string(next.Payload) != "third" {
		t.Fatalf("next slow payload = %q, want third", next.Payload)
	}
	if written := receiveFrame(t, slowWrites, "third slow write"); string(written.Payload) != "third" {
		t.Fatalf("next observed slow write = %q, want third", written.Payload)
	}
	if third := receiveFrame(t, fastFrames, "third fast frame"); string(third.Payload) != "third" {
		t.Fatalf("third fast payload = %q, want third", third.Payload)
	}
}

func TestFlowDoesNotReportSkipForDetachedCarrier(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 32, CarrierQueueBytes: HeaderSize + 32, ReorderWindowBytes: 1024, WriteTimeout: time.Second}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, config)
	fastCarrier, fastPeer := net.Pipe()
	fast, err := flow.Attach(fastCarrier, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		flow.Close()
		application.Close()
		fastPeer.Close()
	})

	fastFrames := make(chan Frame, 2)
	go readFrames(fastPeer, fastFrames, 2)
	for _, frame := range []Frame{
		{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 0, Payload: []byte("first")},
		{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 5, Payload: []byte("second")},
	} {
		if err := flow.broadcast(frame); err != nil {
			t.Fatal(err)
		}
	}

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlowWrite := func() {
		releaseOnce.Do(func() { close(releaseWrite) })
	}
	slowCarrier, slowPeer := net.Pipe()
	slowSkips := make(chan Frame, 2)
	slow, err := flow.AttachObserved(&writeBarrierConn{Conn: slowCarrier, started: writeStarted, release: releaseWrite}, MaxPayloadSize, CarrierObserver{
		Skip: func(frame Frame) { slowSkips <- frame },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseSlowWrite()
		slowPeer.Close()
	})
	waitForSignal(t, writeStarted, "detached carrier write")
	if err := slow.Close(); err != nil {
		t.Fatal(err)
	}

	flow.acknowledge(fast, 11, false)
	select {
	case frame := <-slowSkips:
		t.Fatalf("detached carrier unexpectedly skipped %q", frame.Payload)
	default:
	}
	releaseSlowWrite()
}

type writeBarrierConn struct {
	net.Conn
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (conn *writeBarrierConn) Write(payload []byte) (int, error) {
	conn.once.Do(func() {
		close(conn.started)
		<-conn.release
	})
	return conn.Conn.Write(payload)
}

func waitForSignal(t *testing.T, signal <-chan struct{}, event string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", event)
	}
}

func receiveFrame(t *testing.T, frames <-chan Frame, event string) Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", event)
		return Frame{}
	}
}

func TestFlowRejectsAckBeyondSourceCarrierProgress(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 32, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	carrierConn, peer := net.Pipe()
	carrier, err := flow.Attach(carrierConn, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		flow.Close()
		application.Close()
		peer.Close()
	})

	go func() { _, _ = ReadFrame(peer, MaxPayloadSize) }()
	frame := Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte("data")}
	if err := flow.broadcast(frame); err != nil {
		t.Fatal(err)
	}
	flow.acknowledge(carrier, 100, false)
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.globalAckOffset != 0 || len(flow.history) != 1 {
		t.Fatalf("invalid ACK advanced offset/history to %d/%d", flow.globalAckOffset, len(flow.history))
	}
}

func TestFlowAcceptsCumulativeAckBeyondSourceCarrierProgress(t *testing.T) {
	application, endpoint := net.Pipe()
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	flow := NewFlow(streamID, endpoint, DirectionClientToServer, FlowConfig{ChunkSize: 32, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	fastConn, fastPeer := net.Pipe()
	if _, err := flow.Attach(fastConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWrite) }) }
	slowConn, slowPeer := net.Pipe()
	slow, err := flow.Attach(&writeBarrierConn{Conn: slowConn, started: writeStarted, release: releaseWrite}, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		_ = flow.Close()
		_ = application.Close()
		_ = fastPeer.Close()
		_ = slowPeer.Close()
	})

	go func() { _, _ = ReadFrame(fastPeer, MaxPayloadSize) }()
	payload := []byte("complete")
	if err := flow.broadcast(Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, writeStarted, "slow carrier write")

	flow.mu.Lock()
	writtenOffset := flow.writtenOffset
	slowWrittenOffset := flow.carrierStates[slow].writtenOffset
	flow.mu.Unlock()
	if writtenOffset != uint64(len(payload)) || slowWrittenOffset != 0 {
		t.Fatalf("write progress = flow %d, source carrier %d; want %d/0", writtenOffset, slowWrittenOffset, len(payload))
	}

	flow.acknowledge(slow, uint64(len(payload)), false)
	flow.mu.Lock()
	ackOffset := flow.globalAckOffset
	historyLength := len(flow.history)
	flow.mu.Unlock()
	if ackOffset != uint64(len(payload)) || historyLength != 0 {
		t.Fatalf("cumulative ACK advanced offset/history to %d/%d, want %d/0", ackOffset, historyLength, len(payload))
	}
}

func TestFlowAcknowledgesOnlyContiguousInboundData(t *testing.T) {
	endpoint, application := net.Pipe()
	flow := NewFlow(StreamID{1}, endpoint, DirectionServerToClient, FlowConfig{ChunkSize: 32, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	firstConn, firstPeer := net.Pipe()
	if _, err := flow.Attach(firstConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	secondConn, secondPeer := net.Pipe()
	if _, err := flow.Attach(secondConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = firstPeer.Close()
		_ = secondPeer.Close()
	})

	writeInboundFrameAt(t, secondPeer, flow.ID(), 5, []byte("second"))
	if err := secondPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadFrame(secondPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 0 || ackHasFIN(ack) {
		t.Fatalf("ACK after out-of-order DATA = %#v, want non-FIN offset 0", ack)
	}

	want := []byte("firstsecond")
	type readResult struct {
		payload []byte
		err     error
	}
	readDone := make(chan readResult, 1)
	go func() {
		got := make([]byte, len(want))
		_, err := io.ReadFull(application, got)
		readDone <- readResult{payload: got, err: err}
	}()
	writeInboundFrameAt(t, firstPeer, flow.ID(), 0, []byte("first"))
	if err := firstPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err = ReadFrame(firstPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != uint64(len(want)) || ackHasFIN(ack) {
		t.Fatalf("ACK after gap closed = %#v, want non-FIN offset %d", ack, len(want))
	}
	select {
	case result := <-readDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if string(result.payload) != string(want) {
			t.Fatalf("application payload = %q, want %q", result.payload, want)
		}
	case <-time.After(time.Second):
		t.Fatal("application did not receive reassembled payload")
	}
}

func TestFlowAcknowledgesOutOfOrderFINAfterGapCloses(t *testing.T) {
	application, endpoint := tcpPipe(t)
	flow := NewFlow(StreamID{1}, endpoint, DirectionServerToClient, FlowConfig{ChunkSize: 32, CarrierQueueBytes: 1024, ReorderWindowBytes: 1024, WriteTimeout: time.Second})
	dataConn, dataPeer := net.Pipe()
	if _, err := flow.Attach(dataConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	finConn, finPeer := net.Pipe()
	if _, err := flow.Attach(finConn, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	flow.Start()
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = dataPeer.Close()
		_ = finPeer.Close()
	})

	if err := WriteFrame(finPeer, Frame{Type: FrameFIN, Direction: DirectionClientToServer, StreamID: flow.ID(), Offset: 5}); err != nil {
		t.Fatal(err)
	}
	if err := finPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadFrame(finPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 0 || ackHasFIN(ack) {
		t.Fatalf("ACK after out-of-order FIN = %#v, want non-FIN offset 0", ack)
	}

	type readResult struct {
		payload []byte
		err     error
	}
	readDone := make(chan readResult, 1)
	go func() {
		got, err := io.ReadAll(application)
		readDone <- readResult{payload: got, err: err}
	}()
	writeInboundFrameAt(t, dataPeer, flow.ID(), 0, []byte("hello"))
	if err := dataPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ack, err = ReadFrame(dataPeer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 5 || !ackHasFIN(ack) {
		t.Fatalf("ACK after FIN gap closed = %#v, want FIN offset 5", ack)
	}
	select {
	case result := <-readDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if string(result.payload) != "hello" {
			t.Fatalf("application payload = %q, want %q", result.payload, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("application did not observe EOF after reassembled FIN")
	}
}

func readFrames(conn net.Conn, frames chan<- Frame, count int) {
	for range count {
		frame, err := ReadFrame(conn, MaxPayloadSize)
		if err != nil {
			return
		}
		frames <- frame
	}
}

func TestFlowsDrainResponseAfterHalfClose(t *testing.T) {
	clientApp, clientEndpoint := tcpPipe(t)
	serverEndpoint, serverApp := tcpPipe(t)
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	config := FlowConfig{ChunkSize: 1024, CarrierQueueBytes: 4096, ReorderWindowBytes: 1 << 20, WriteTimeout: time.Second}
	clientFlow := NewFlow(streamID, clientEndpoint, DirectionClientToServer, config)
	serverFlow := NewFlow(streamID, serverEndpoint, DirectionServerToClient, config)
	clientCarrier, serverCarrier := net.Pipe()
	if _, err := clientFlow.Attach(clientCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	if _, err := serverFlow.Attach(serverCarrier, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	clientFlow.Start()
	serverFlow.Start()
	t.Cleanup(func() {
		clientFlow.Close()
		serverFlow.Close()
		clientApp.Close()
		serverApp.Close()
	})

	request := []byte("request")
	if _, err := clientApp.Write(request); err != nil {
		t.Fatal(err)
	}
	if err := clientApp.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	gotRequest, err := io.ReadAll(serverApp)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotRequest) != string(request) {
		t.Fatalf("request = %q", gotRequest)
	}
	response := make([]byte, 128*1024)
	for index := range response {
		response[index] = byte(index)
	}
	go func() {
		_, _ = serverApp.Write(response)
		_ = serverApp.CloseWrite()
	}()
	gotResponse, err := io.ReadAll(clientApp)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotResponse) != string(response) {
		t.Fatalf("response length = %d, want %d", len(gotResponse), len(response))
	}
	for name, flow := range map[string]*Flow{"client": clientFlow, "server": serverFlow} {
		select {
		case <-flow.Done():
		case <-time.After(time.Second):
			t.Fatalf("%s flow did not finish after both FIN acknowledgements", name)
		}
		if err := flow.Err(); err != nil {
			t.Fatalf("%s flow finished with error: %v", name, err)
		}
	}
}

func tcpPipe(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, _ := listener.AcceptTCP()
		accepted <- conn
	}()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server := <-accepted
	listener.Close()
	return client, server
}
