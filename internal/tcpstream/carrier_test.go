package tcpstream

import (
	"net"
	"sync"
	"testing"
	"time"
)

type writeStartedConn struct {
	net.Conn
	once    sync.Once
	started chan struct{}
}

func (conn *writeStartedConn) Write(payload []byte) (int, error) {
	conn.once.Do(func() { close(conn.started) })
	return conn.Conn.Write(payload)
}

func TestCarrierWriteFailureReleasesPendingEnqueues(t *testing.T) {
	testCarrierReleasesPendingEnqueues(t, func(_ *Carrier, peer net.Conn) error {
		return peer.Close()
	})
}

func TestCarrierAcknowledgementsDoNotBlockBehindDataWrite(t *testing.T) {
	carrierConn, peer := net.Pipe()
	started := make(chan struct{})
	carrier := newCarrier(
		&writeStartedConn{Conn: carrierConn, started: started},
		MaxPayloadSize,
		1,
		1,
		time.Second,
		func(*Carrier, Frame) error { return nil },
		nil,
		CarrierObserver{},
	)
	carrier.start()
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = peer.Close()
	})

	streamID := StreamID{1}
	data := Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte("payload")}
	dataResult := make(chan bool, 1)
	go func() { dataResult <- carrier.enqueue(data, true) }()
	waitForCarrierLoop(t, "first carrier write", started)

	ackResult := make(chan bool, 1)
	go func() {
		first := carrier.enqueueAck(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: streamID, Offset: 10, Payload: []byte{0}})
		second := carrier.enqueueAck(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: streamID, Offset: 20, Payload: []byte{1}})
		stale := carrier.enqueueAck(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: streamID, Offset: 15, Payload: []byte{0}})
		ackResult <- first && second && stale
	}()
	select {
	case queued := <-ackResult:
		if !queued {
			t.Fatal("acknowledgement was not queued")
		}
	case <-time.After(time.Second):
		t.Fatal("acknowledgement blocked behind an in-progress data write")
	}

	first, err := ReadFrame(peer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != FrameData {
		t.Fatalf("first frame type = %d, want DATA", first.Type)
	}
	select {
	case success := <-dataResult:
		if !success {
			t.Fatal("data write failed")
		}
	case <-time.After(time.Second):
		t.Fatal("data enqueue did not complete")
	}

	ack, err := ReadFrame(peer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != FrameAck || ack.Offset != 20 || !ackHasFIN(ack) {
		t.Fatalf("acknowledgement = %#v, want latest offset 20 with FIN", ack)
	}
}

func TestCarrierAlternatesReadyAcknowledgementAndData(t *testing.T) {
	carrierConn, peer := net.Pipe()
	carrier := newCarrier(carrierConn, MaxPayloadSize, 1, 1, time.Second, func(*Carrier, Frame) error { return nil }, nil, CarrierObserver{})
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = peer.Close()
	})

	streamID := StreamID{1}
	data := queuedFrame{frame: Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte("data")}}
	carrier.queue <- data
	if !carrier.enqueueAck(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: streamID, Offset: 1, Payload: []byte{0}}) {
		t.Fatal("first acknowledgement was not queued")
	}
	item, isAck, ok := carrier.nextQueuedFrame(true)
	if !ok || !isAck || item.frame.Type != FrameAck {
		t.Fatalf("first selected frame = %#v, isAck=%v, ok=%v", item.frame, isAck, ok)
	}
	if !carrier.enqueueAck(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: streamID, Offset: 2, Payload: []byte{0}}) {
		t.Fatal("second acknowledgement was not queued")
	}
	item, isAck, ok = carrier.nextQueuedFrame(false)
	if !ok || isAck || item.frame.Type != FrameData {
		t.Fatalf("second selected frame = %#v, isAck=%v, ok=%v", item.frame, isAck, ok)
	}
	item, isAck, ok = carrier.nextQueuedFrame(true)
	if !ok || !isAck || item.frame.Type != FrameAck || item.frame.Offset != 2 {
		t.Fatalf("third selected frame = %#v, isAck=%v, ok=%v", item.frame, isAck, ok)
	}
}

func TestCarrierCloseReleasesPendingEnqueuesAtCapacityOne(t *testing.T) {
	testCarrierReleasesPendingEnqueues(t, func(carrier *Carrier, _ net.Conn) error {
		return carrier.Close()
	})
}

func TestCarrierDetachedWaitsForCloseCallbackCompletion(t *testing.T) {
	carrierConn, peer := net.Pipe()
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	carrier := newCarrier(
		carrierConn,
		MaxPayloadSize,
		1024,
		1024,
		time.Second,
		func(*Carrier, Frame) error { return nil },
		func(*Carrier, bool) {
			close(callbackStarted)
			<-releaseCallback
		},
		CarrierObserver{},
	)
	carrier.start()
	t.Cleanup(func() { _ = peer.Close() })

	closeResult := make(chan error, 1)
	go func() { closeResult <- carrier.Close() }()
	waitForCarrierLoop(t, "carrier close callback start", callbackStarted)
	select {
	case <-carrier.Detached():
		t.Fatal("Detached closed before the close callback returned")
	default:
	}
	close(releaseCallback)
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Carrier.Close did not return after the close callback was released")
	}
	waitForCarrierLoop(t, "carrier detached", carrier.Detached())
}

func TestCarrierPeerEOFWaitsToDetachUntilLastFrameProcessing(t *testing.T) {
	carrierConn, peer := net.Pipe()
	started := make(chan struct{})
	release := make(chan struct{})
	detached := make(chan struct{})
	carrier := newCarrier(
		carrierConn,
		MaxPayloadSize,
		1024,
		1024,
		time.Second,
		func(*Carrier, Frame) error {
			close(started)
			<-release
			return nil
		},
		func(*Carrier, bool) { close(detached) },
		CarrierObserver{},
	)
	carrier.start()
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = peer.Close()
	})

	peerClosed := make(chan error, 1)
	go func() {
		err := WriteFrame(peer, Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: StreamID{1}, Payload: []byte("tail")})
		if err == nil {
			err = peer.Close()
		}
		peerClosed <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("last frame processing did not start")
	}
	select {
	case err := <-peerClosed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not close after writing the last frame")
	}
	waitForCarrierLoop(t, "carrier stop", carrier.Done())
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-detached:
		t.Fatal("carrier detached before the last frame was processed after Close")
	default:
	}

	close(release)
	waitForCarrierLoop(t, "carrier detach", detached)
	waitForCarrierLoop(t, "carrier read", carrier.readDone)
	waitForCarrierLoop(t, "carrier process", carrier.processDone)
}

func TestCarrierWriteFailureStillProcessesCompletedInboundFrame(t *testing.T) {
	carrierConn, peer := net.Pipe()
	readObserved := make(chan struct{})
	releaseRead := make(chan struct{})
	processed := make(chan struct{})
	carrier := newCarrier(
		carrierConn,
		MaxPayloadSize,
		1024,
		1024,
		time.Second,
		func(*Carrier, Frame) error {
			close(processed)
			return nil
		},
		nil,
		CarrierObserver{Read: func(Frame) {
			close(readObserved)
			<-releaseRead
		}},
	)
	carrier.start()
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = peer.Close()
	})

	peerClosed := make(chan error, 1)
	go func() {
		err := WriteFrame(peer, Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: StreamID{1}, Payload: []byte("completed")})
		if err == nil {
			err = peer.Close()
		}
		peerClosed <- err
	}()
	select {
	case <-readObserved:
	case <-time.After(time.Second):
		t.Fatal("completed inbound frame was not observed")
	}
	select {
	case err := <-peerClosed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not close")
	}
	if carrier.enqueue(Frame{Type: FrameAck, Direction: DirectionServerToClient, StreamID: StreamID{1}, Payload: []byte{0}}, true) {
		t.Fatal("write to closed peer unexpectedly succeeded")
	}
	waitForCarrierLoop(t, "carrier stop", carrier.Done())
	close(releaseRead)
	waitForCarrierLoop(t, "inbound processing", processed)
	waitForCarrierLoop(t, "carrier read", carrier.readDone)
	waitForCarrierLoop(t, "carrier process", carrier.processDone)
}

func testCarrierReleasesPendingEnqueues(t *testing.T, stop func(*Carrier, net.Conn) error) {
	t.Helper()
	carrierConn, peer := net.Pipe()
	started := make(chan struct{})
	finished := make(chan struct{})
	carrier := newCarrier(
		&writeStartedConn{Conn: carrierConn, started: started},
		MaxPayloadSize,
		1,
		1,
		time.Second,
		func(*Carrier, Frame) error { return nil },
		func(*Carrier, bool) { close(finished) },
		CarrierObserver{},
	)
	carrier.start()
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = peer.Close()
	})

	var streamID StreamID
	streamID[0] = 1
	frame := Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte("payload")}
	results := make(chan bool, 2)
	go func() { results <- carrier.enqueue(frame, true) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first carrier write did not start")
	}

	if !carrier.enqueue(frame, false) {
		t.Fatal("carrier rejected the frame that should fill its queue")
	}
	if !carrier.beginEnqueue() {
		t.Fatal("carrier rejected the third enqueue before shutdown")
	}
	stopResult := make(chan error, 1)
	go func() { stopResult <- stop(carrier, peer) }()
	waitForCarrierLoop(t, "carrier stop", carrier.Done())
	results <- carrier.enqueueBegun(frame, true)
	for range 2 {
		select {
		case success := <-results:
			if success {
				t.Fatal("enqueue unexpectedly succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("pending enqueue was not released")
		}
	}
	select {
	case err := <-stopResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("carrier stop did not finish after pending enqueues were released")
	}
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	if len(carrier.queue) != 0 {
		t.Fatalf("pending queue length = %d, want 0", len(carrier.queue))
	}
	waitForCarrierLoop(t, "carrier close callback", finished)
}
