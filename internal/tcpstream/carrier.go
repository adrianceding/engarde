package tcpstream

import (
	"errors"
	"net"
	"sync"
	"time"
)

type Carrier struct {
	conn          net.Conn
	maxPayload    uint32
	writeTimeout  time.Duration
	queue         chan queuedFrame
	ackReady      chan struct{}
	inbound       chan Frame
	done          chan struct{}
	readDone      chan struct{}
	processDone   chan struct{}
	closeOnce     sync.Once
	finishOnce    sync.Once
	stateMu       sync.Mutex
	closing       bool
	preserveRead  bool
	ackMu         sync.Mutex
	pendingAck    Frame
	hasPendingAck bool
	enqueueWG     sync.WaitGroup
	onFrame       func(*Carrier, Frame) error
	onStop        func(*Carrier)
	onWrite       func(*Carrier, Frame)
	onClose       func(*Carrier, bool)
	observer      CarrierObserver
}

type CarrierObserver struct {
	Read  func(Frame)
	Write func(Frame)
	Drop  func(Frame)
	Skip  func(Frame)
}

type queuedFrame struct {
	frame      Frame
	completion chan bool
}

var frameCompletionPool = sync.Pool{New: func() any { return make(chan bool, 1) }}

func newCarrier(conn net.Conn, maxPayload uint32, queueBytes, chunkSize int, writeTimeout time.Duration, onFrame func(*Carrier, Frame) error, onClose func(*Carrier, bool), observer CarrierObserver) *Carrier {
	queueFrames := queueBytes / (chunkSize + HeaderSize)
	if queueFrames < 1 {
		queueFrames = 1
	}
	return &Carrier{
		conn:         conn,
		maxPayload:   maxPayload,
		writeTimeout: writeTimeout,
		queue:        make(chan queuedFrame, queueFrames),
		ackReady:     make(chan struct{}, 1),
		inbound:      make(chan Frame),
		done:         make(chan struct{}),
		readDone:     make(chan struct{}),
		processDone:  make(chan struct{}),
		onFrame:      onFrame,
		onClose:      onClose,
		observer:     observer,
	}
}

func (carrier *Carrier) start() {
	go carrier.readLoop()
	go carrier.processLoop()
	go carrier.writeLoop()
}

func (carrier *Carrier) Done() <-chan struct{} {
	return carrier.done
}

func (carrier *Carrier) preservesInbound() bool {
	carrier.stateMu.Lock()
	defer carrier.stateMu.Unlock()
	return carrier.closing && carrier.preserveRead
}

func (carrier *Carrier) reportSkip(frame Frame) {
	if carrier.observer.Skip != nil {
		carrier.observer.Skip(frame)
	}
}

func (carrier *Carrier) enqueue(frame Frame, block bool) bool {
	item := queuedFrame{frame: frame}
	if block {
		item.completion = frameCompletionPool.Get().(chan bool)
		defer frameCompletionPool.Put(item.completion)
	}
	if !carrier.beginEnqueue() {
		return false
	}
	sent := false
	if block {
		select {
		case carrier.queue <- item:
			sent = true
		case <-carrier.done:
		}
	} else {
		select {
		case carrier.queue <- item:
			sent = true
		case <-carrier.done:
		default:
		}
	}
	carrier.enqueueWG.Done()
	if !sent {
		return false
	}
	if !block {
		return true
	}
	select {
	case success := <-item.completion:
		return success
	case <-carrier.done:
		return <-item.completion
	}
}

func (carrier *Carrier) enqueueAck(frame Frame) bool {
	if !carrier.beginEnqueue() {
		return false
	}
	defer carrier.enqueueWG.Done()
	select {
	case <-carrier.done:
		return false
	default:
	}

	carrier.ackMu.Lock()
	if carrier.hasPendingAck {
		frame = laterAck(carrier.pendingAck, frame)
	}
	carrier.pendingAck = frame
	carrier.hasPendingAck = true
	select {
	case carrier.ackReady <- struct{}{}:
	default:
	}
	carrier.ackMu.Unlock()
	return true
}

func laterAck(current, next Frame) Frame {
	if current.Offset > next.Offset || (current.Offset == next.Offset && ackHasFIN(current)) {
		return current
	}
	return next
}

func ackHasFIN(frame Frame) bool {
	return len(frame.Payload) == 1 && frame.Payload[0] != 0
}

func (carrier *Carrier) takePendingAck() (queuedFrame, bool) {
	carrier.ackMu.Lock()
	defer carrier.ackMu.Unlock()
	if !carrier.hasPendingAck {
		return queuedFrame{}, false
	}
	item := queuedFrame{frame: carrier.pendingAck}
	carrier.pendingAck = Frame{}
	carrier.hasPendingAck = false
	return item, true
}

func (carrier *Carrier) beginEnqueue() bool {
	carrier.stateMu.Lock()
	defer carrier.stateMu.Unlock()
	if carrier.closing {
		return false
	}
	carrier.enqueueWG.Add(1)
	return true
}

func (carrier *Carrier) readLoop() {
	defer close(carrier.readDone)
	defer carrier.finishClose()
	decoder := new(frameReader)
	for {
		frame, err := decoder.read(carrier.conn, carrier.maxPayload)
		if err != nil {
			_ = carrier.stop(true)
			close(carrier.inbound)
			preserveRead := carrier.preservesInbound()
			if preserveRead {
				<-carrier.processDone
			}
			return
		}
		if carrier.observer.Read != nil {
			carrier.observer.Read(frame)
		}
		if !carrier.forwardInbound(frame) {
			close(carrier.inbound)
			return
		}
	}
}

func (carrier *Carrier) forwardInbound(frame Frame) bool {
	done := carrier.done
	for {
		select {
		case carrier.inbound <- frame:
			return true
		case <-carrier.processDone:
			return false
		case <-done:
			if !carrier.preservesInbound() {
				return false
			}
			done = nil
		}
	}
}

func (carrier *Carrier) processLoop() {
	defer close(carrier.processDone)
	for {
		var frame Frame
		var ok bool
		select {
		case frame, ok = <-carrier.inbound:
			if !ok {
				return
			}
		case <-carrier.done:
			if !carrier.preservesInbound() {
				return
			}
			for frame = range carrier.inbound {
				if err := carrier.onFrame(carrier, frame); err != nil {
					_ = carrier.stop(false)
					return
				}
			}
			return
		}
		if err := carrier.onFrame(carrier, frame); err != nil {
			_ = carrier.stop(false)
			return
		}
	}
}

func (carrier *Carrier) writeLoop() {
	preferAck := true
	for {
		item, isAck, ok := carrier.nextQueuedFrame(preferAck)
		if !ok {
			return
		}
		if carrier.writeTimeout > 0 {
			if err := carrier.conn.SetWriteDeadline(time.Now().Add(carrier.writeTimeout)); err != nil {
				completeQueuedFrame(item, false)
				_ = carrier.stop(true)
				return
			}
		}
		if err := WriteFrame(carrier.conn, item.frame); err != nil {
			if carrier.observer.Drop != nil {
				carrier.observer.Drop(item.frame)
			}
			completeQueuedFrame(item, false)
			_ = carrier.stop(true)
			return
		}
		if carrier.onWrite != nil {
			carrier.onWrite(carrier, item.frame)
		}
		if carrier.observer.Write != nil {
			carrier.observer.Write(item.frame)
		}
		completeQueuedFrame(item, true)
		preferAck = !isAck
	}
}

func (carrier *Carrier) nextQueuedFrame(preferAck bool) (queuedFrame, bool, bool) {
	for {
		select {
		case <-carrier.done:
			return queuedFrame{}, false, false
		default:
		}
		if preferAck {
			select {
			case <-carrier.ackReady:
				if item, ok := carrier.takePendingAck(); ok {
					return item, true, true
				}
				continue
			default:
			}
		} else {
			select {
			case item := <-carrier.queue:
				return item, false, true
			default:
			}
		}
		select {
		case <-carrier.done:
			return queuedFrame{}, false, false
		case <-carrier.ackReady:
			if item, ok := carrier.takePendingAck(); ok {
				return item, true, true
			}
		case item := <-carrier.queue:
			return item, false, true
		}
	}
}

func (carrier *Carrier) Close() error {
	closeErr := carrier.stop(false)
	if !carrier.preservesInbound() {
		carrier.finishClose()
	}
	if errors.Is(closeErr, net.ErrClosed) {
		return nil
	}
	return closeErr
}

func (carrier *Carrier) stop(preserveRead bool) error {
	var closeErr error
	carrier.closeOnce.Do(func() {
		carrier.stateMu.Lock()
		carrier.closing = true
		carrier.preserveRead = preserveRead
		close(carrier.done)
		onStop := carrier.onStop
		carrier.stateMu.Unlock()
		if onStop != nil {
			onStop(carrier)
		}
		closeErr = carrier.conn.Close()
		carrier.enqueueWG.Wait()
		carrier.failPending()
	})
	return closeErr
}

func (carrier *Carrier) finishClose() {
	carrier.finishOnce.Do(func() {
		if carrier.onClose != nil {
			carrier.stateMu.Lock()
			preserveRead := carrier.preserveRead
			carrier.stateMu.Unlock()
			carrier.onClose(carrier, preserveRead)
		}
	})
}

func (carrier *Carrier) failPending() {
	for {
		select {
		case item := <-carrier.queue:
			completeQueuedFrame(item, false)
		default:
			carrier.ackMu.Lock()
			carrier.pendingAck = Frame{}
			carrier.hasPendingAck = false
			carrier.ackMu.Unlock()
			return
		}
	}
}

func completeQueuedFrame(item queuedFrame, success bool) {
	if item.completion != nil {
		item.completion <- success
	}
}
