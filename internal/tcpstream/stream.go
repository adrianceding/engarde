package tcpstream

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

var ErrNoCarriers = errors.New("tcp stream has no active carriers")

type flowRecoveryTimer interface {
	Stop() bool
}

var newFlowRecoveryTimer = func(delay time.Duration, callback func()) flowRecoveryTimer {
	return time.AfterFunc(delay, callback)
}

var flowRecoveryNow = time.Now

type FlowState string

const (
	FlowStatePending    FlowState = "pending"
	FlowStateActive     FlowState = "active"
	FlowStateRecovering FlowState = "recovering"
	FlowStateCompleted  FlowState = "completed"
	FlowStateFailed     FlowState = "failed"
)

type FlowConfig struct {
	ChunkSize          int
	CarrierQueueBytes  int
	ReorderWindowBytes int
	WriteTimeout       time.Duration
	RecoveryTimeout    time.Duration
	SingleCarrier      bool
}

type Flow struct {
	id            StreamID
	endpoint      net.Conn
	sendDirection Direction
	reassembler   *Reassembler
	config        FlowConfig
	inbound       chan inboundFrame
	inboundStop   chan struct{}

	mu                sync.Mutex
	carriers          []*Carrier
	started           bool
	localFIN          bool
	localFINAck       bool
	remoteFIN         bool
	remoteFINAckSent  bool
	closed            bool
	pendingInbound    int
	inboundEnqueueWG  sync.WaitGroup
	closeOnce         sync.Once
	done              chan struct{}
	terminalErr       error
	terminalNotify    bool
	terminalAbortive  bool
	recovering        bool
	recoveryDeadline  time.Time
	recoveryTimer     flowRecoveryTimer
	recoveryEpoch     uint64
	carrierGeneration uint64
	history           []replayFrame
	historyBytes      int
	nextSequence      uint64
	globalAckOffset   uint64
	globalAckFIN      bool
	writtenOffset     uint64
	writtenFIN        bool
	writtenFINOffset  uint64
	latestAck         Frame
	hasLatestAck      bool
	deferredAckOffset uint64
	deferredAckFIN    bool
	hasDeferredAck    bool
	carrierStates     map[*Carrier]*carrierState
	cond              *sync.Cond
}

type replayFrame struct {
	sequence uint64
	frame    Frame
}

type inboundFrame struct {
	source *Carrier
	frame  Frame
}

type endpointDeadlineWriter struct {
	conn        net.Conn
	timeout     time.Duration
	deadlineSet bool
}

func (writer *endpointDeadlineWriter) Write(payload []byte) (int, error) {
	if writer.timeout > 0 {
		if err := writer.conn.SetWriteDeadline(time.Now().Add(writer.timeout)); err != nil {
			return 0, err
		}
		writer.deadlineSet = true
	}
	return writer.conn.Write(payload)
}

func (writer *endpointDeadlineWriter) clearDeadline() error {
	if !writer.deadlineSet {
		return nil
	}
	writer.deadlineSet = false
	return writer.conn.SetWriteDeadline(time.Time{})
}

type carrierState struct {
	nextSequence     uint64
	inFlightSequence uint64
	hasInFlight      bool
	writtenSequence  uint64
	hasWritten       bool
	writtenOffset    uint64
	writtenFIN       bool
}

type skipNotification struct {
	carrier *Carrier
	frame   Frame
}

func NewFlow(id StreamID, endpoint net.Conn, sendDirection Direction, config FlowConfig) *Flow {
	flow := &Flow{
		id:            id,
		endpoint:      endpoint,
		sendDirection: sendDirection,
		reassembler:   NewReassembler(config.ReorderWindowBytes),
		config:        config,
		inbound:       make(chan inboundFrame, inboundQueueCapacity(config.ReorderWindowBytes)),
		inboundStop:   make(chan struct{}),
		done:          make(chan struct{}),
		carrierStates: make(map[*Carrier]*carrierState),
	}
	flow.cond = sync.NewCond(&flow.mu)
	return flow
}

func (flow *Flow) ID() StreamID {
	return flow.id
}

func (flow *Flow) Done() <-chan struct{} {
	return flow.done
}

func (flow *Flow) Err() error {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.terminalErr
}

func (flow *Flow) CarrierCount() int {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return len(flow.carriers)
}

func (flow *Flow) CarrierGeneration() uint64 {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.carrierGeneration
}

func (flow *Flow) State() FlowState {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.closed {
		if flow.terminalErr == nil {
			return FlowStateCompleted
		}
		return FlowStateFailed
	}
	if flow.recovering {
		return FlowStateRecovering
	}
	if flow.started && len(flow.carriers) > 0 {
		return FlowStateActive
	}
	return FlowStatePending
}

func (flow *Flow) RecoveryDeadline() time.Time {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.recoveryDeadline
}

func (flow *Flow) HistoryBytes() int {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.historyBytes
}

func (flow *Flow) Attach(conn net.Conn, maxPayload uint32) (*Carrier, error) {
	return flow.AttachObserved(conn, maxPayload, CarrierObserver{})
}

func (flow *Flow) AttachObserved(conn net.Conn, maxPayload uint32, observer CarrierObserver) (*Carrier, error) {
	return flow.attachLimited(conn, maxPayload, 0, observer)
}

func (flow *Flow) AttachLimited(conn net.Conn, maxPayload uint32, maxCarriers int) (*Carrier, error) {
	return flow.attachLimited(conn, maxPayload, maxCarriers, CarrierObserver{})
}

func (flow *Flow) AttachLimitedObserved(conn net.Conn, maxPayload uint32, maxCarriers int, observer CarrierObserver) (*Carrier, error) {
	return flow.attachLimited(conn, maxPayload, maxCarriers, observer)
}

func (flow *Flow) ReplaceObserved(conn net.Conn, maxPayload uint32, generation uint64, observer CarrierObserver) (*Carrier, error) {
	return flow.attach(conn, maxPayload, 1, generation, true, observer)
}

func (flow *Flow) attachLimited(conn net.Conn, maxPayload uint32, maxCarriers int, observer CarrierObserver) (*Carrier, error) {
	return flow.attach(conn, maxPayload, maxCarriers, 0, false, observer)
}

func (flow *Flow) attach(conn net.Conn, maxPayload uint32, maxCarriers int, generation uint64, replace bool, observer CarrierObserver) (*Carrier, error) {
	if maxPayload == 0 || maxPayload > MaxPayloadSize {
		maxPayload = MaxPayloadSize
	}
	flow.mu.Lock()
	if flow.closed {
		flow.mu.Unlock()
		conn.Close()
		return nil, net.ErrClosed
	}
	if replace {
		if !flow.config.SingleCarrier || generation <= flow.carrierGeneration {
			flow.mu.Unlock()
			conn.Close()
			return nil, errors.New("invalid TCP carrier generation")
		}
	} else if flow.config.SingleCarrier {
		if len(flow.carriers) > 0 || flow.started {
			flow.mu.Unlock()
			conn.Close()
			return nil, errors.New("active-standby flow requires carrier replacement")
		}
		generation = 1
		maxCarriers = 1
	}
	if !replace && maxCarriers > 0 && len(flow.carriers) >= maxCarriers {
		flow.mu.Unlock()
		conn.Close()
		return nil, errors.New("maximum TCP carriers reached")
	}
	carrier := newCarrier(conn, maxPayload, flow.config.CarrierQueueBytes, flow.config.ChunkSize, flow.config.WriteTimeout, flow.handleFrame, flow.detach, observer)
	carrier.onStop = flow.carrierStopping
	carrier.onWrite = flow.carrierFrameWritten
	retired := []*Carrier(nil)
	if replace {
		retired = append(retired, flow.carriers...)
		flow.carriers = nil
		flow.carrierStates = make(map[*Carrier]*carrierState)
	}
	flow.carriers = append(flow.carriers, carrier)
	if flow.config.SingleCarrier {
		flow.carrierGeneration = generation
	}
	flow.stopRecoveryLocked()
	nextSequence := flow.nextSequence
	if len(flow.history) > 0 {
		nextSequence = flow.history[0].sequence
	}
	flow.carrierStates[carrier] = &carrierState{nextSequence: nextSequence}
	latestAck := flow.latestAck
	hasLatestAck := flow.hasLatestAck
	flow.mu.Unlock()
	for _, previous := range retired {
		_ = previous.Close()
	}
	carrier.start()
	if hasLatestAck {
		carrier.enqueueAck(latestAck)
	}
	go flow.sendCarrier(carrier)
	flow.mu.Lock()
	flow.cond.Broadcast()
	flow.mu.Unlock()
	return carrier, nil
}

func (flow *Flow) Start() {
	flow.mu.Lock()
	if flow.started || flow.closed {
		flow.mu.Unlock()
		return
	}
	flow.started = true
	if len(flow.carriers) == 0 {
		if flow.beginRecoveryLocked() {
			flow.mu.Unlock()
			go flow.writeEndpoint()
			go flow.readEndpoint()
			return
		}
		finishReset := flow.claimResetLocked(ErrNoCarriers, true)
		flow.mu.Unlock()
		if finishReset {
			flow.finishReset()
		}
		return
	}
	flow.mu.Unlock()
	go flow.writeEndpoint()
	go flow.readEndpoint()
}

func inboundQueueCapacity(windowBytes int) int {
	capacity := windowBytes / MaxPayloadSize
	if capacity < 1 {
		capacity = 1
	}
	if capacity > MaxReorderSegments {
		capacity = MaxReorderSegments
	}
	return capacity
}

func (flow *Flow) Close() error {
	flow.reset(nil, false)
	return nil
}

func (flow *Flow) Reset(err error) {
	flow.reset(err, true)
}

func (flow *Flow) readEndpoint() {
	chunkSize := flow.config.ChunkSize
	if chunkSize <= 0 || chunkSize > MaxPayloadSize {
		chunkSize = 16 * 1024
	}
	buffer := make([]byte, chunkSize)
	var offset uint64
	for {
		read, err := flow.endpoint.Read(buffer)
		if read > 0 {
			if broadcastErr := flow.broadcast(Frame{Type: FrameData, Direction: flow.sendDirection, StreamID: flow.id, Offset: offset, Payload: buffer[:read]}); broadcastErr != nil {
				flow.Reset(broadcastErr)
				return
			}
			offset += uint64(read)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				flow.mu.Lock()
				flow.localFIN = true
				flow.mu.Unlock()
				if broadcastErr := flow.broadcast(Frame{Type: FrameFIN, Direction: flow.sendDirection, StreamID: flow.id, Offset: offset}); broadcastErr != nil {
					if errors.Is(broadcastErr, ErrNoCarriers) {
						flow.resetAfterCarrierFailure()
					} else {
						flow.Reset(broadcastErr)
					}
					return
				}
				flow.mu.Lock()
				complete := flow.completeLocked()
				flow.mu.Unlock()
				if complete {
					flow.reset(nil, false)
				}
				return
			}
			flow.Reset(err)
			return
		}
	}
}

func (flow *Flow) handleFrame(source *Carrier, frame Frame) error {
	flow.mu.Lock()
	_, sourceActive := flow.carrierStates[source]
	closed := flow.closed
	flow.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	if !sourceActive {
		return nil
	}
	if frame.StreamID != flow.id || frame.Type == FrameOpen {
		flow.Reset(ErrInvalidFrame)
		return ErrInvalidFrame
	}
	if frame.Type == FrameAck {
		if frame.Direction != flow.sendDirection {
			flow.Reset(ErrInvalidFrame)
			return ErrInvalidFrame
		}
		flow.acknowledge(source, frame.Offset, frame.Payload[0] == 1)
		return nil
	}
	if frame.Direction == flow.sendDirection {
		flow.Reset(ErrInvalidFrame)
		return ErrInvalidFrame
	}
	switch frame.Type {
	case FrameData:
		return flow.enqueueInbound(source, frame)
	case FrameFIN:
		return flow.enqueueInbound(source, frame)
	case FrameRST:
		flow.Reset(ErrInvalidFrame)
		return ErrInvalidFrame
	default:
		flow.Reset(ErrInvalidFrame)
		return ErrInvalidFrame
	}
}

func (flow *Flow) enqueueInbound(source *Carrier, frame Frame) error {
	flow.mu.Lock()
	if flow.closed {
		flow.mu.Unlock()
		return net.ErrClosed
	}
	flow.pendingInbound++
	flow.inboundEnqueueWG.Add(1)
	flow.mu.Unlock()
	item := inboundFrame{source: source, frame: frame}
	sourceDone := source.Done()
	sent := false
	for !sent {
		select {
		case flow.inbound <- item:
			sent = true
		case <-sourceDone:
			if source.preservesInbound() {
				sourceDone = nil
				continue
			}
			flow.inboundEnqueueWG.Done()
			flow.releaseInbound()
			return net.ErrClosed
		case <-flow.inboundStop:
			flow.inboundEnqueueWG.Done()
			flow.releaseInbound()
			return net.ErrClosed
		}
	}
	flow.inboundEnqueueWG.Done()
	return nil
}

func (flow *Flow) writeEndpoint() {
	writer := endpointDeadlineWriter{conn: flow.endpoint, timeout: flow.config.WriteTimeout}
	for {
		select {
		case item := <-flow.inbound:
			flow.mu.Lock()
			closed := flow.closed
			flow.mu.Unlock()
			if closed {
				flow.releaseInbound()
				return
			}
			err := flow.deliverInbound(item, &writer)
			shouldReset := flow.releaseInbound()
			if err != nil {
				flow.Reset(err)
				return
			}
			if shouldReset {
				flow.resetAfterLastCarrier()
				return
			}
		case <-flow.done:
			return
		}
	}
}

func (flow *Flow) releaseInbound() bool {
	flow.mu.Lock()
	if flow.pendingInbound > 0 {
		flow.pendingInbound--
	}
	lastCarrierDrained := flow.started && !flow.closed && flow.pendingInbound == 0 && len(flow.carriers) == 0
	shouldReset := lastCarrierDrained && flow.config.RecoveryTimeout <= 0
	if lastCarrierDrained && flow.config.RecoveryTimeout > 0 {
		flow.beginRecoveryLocked()
	}
	flow.mu.Unlock()
	return shouldReset
}

func (flow *Flow) discardInbound() {
	for {
		select {
		case <-flow.inbound:
			flow.releaseInbound()
		default:
			return
		}
	}
}

func (flow *Flow) deliverInbound(item inboundFrame, writer *endpointDeadlineWriter) error {
	frame := item.frame
	finished := false
	var err error
	switch frame.Type {
	case FrameData:
		if err = flow.reassembler.pushOwned(frame.Offset, frame.Payload); err == nil {
			finished, err = flow.reassembler.DrainTo(writer)
		}
	case FrameFIN:
		if err = flow.reassembler.SetFIN(frame.Offset); err == nil {
			finished, err = flow.reassembler.DrainTo(writer)
		}
	default:
		return ErrInvalidFrame
	}
	if err != nil {
		return err
	}
	if err := writer.clearDeadline(); err != nil {
		return err
	}
	flow.enqueueAck(item.source, flow.ackFrame(flow.reassembler.NextOffset(), finished))
	if finished {
		flow.finishRemote()
	}
	return nil
}

func (flow *Flow) enqueueAck(source *Carrier, frame Frame) {
	flow.mu.Lock()
	if flow.hasLatestAck {
		frame = laterAck(flow.latestAck, frame)
	}
	flow.latestAck = frame
	flow.hasLatestAck = true
	carriers := append([]*Carrier(nil), flow.carriers...)
	flow.mu.Unlock()
	if source != nil && source.enqueueAck(frame) {
		return
	}
	enqueueAckOnCarriers(carriers, source, frame)
}

func enqueueAckOnCarriers(carriers []*Carrier, excluded *Carrier, frame Frame) {
	for _, carrier := range carriers {
		if carrier != excluded {
			carrier.enqueueAck(frame)
		}
	}
}

func (flow *Flow) carrierStopping(stopping *Carrier) {
	flow.mu.Lock()
	if flow.closed || !flow.hasLatestAck {
		flow.mu.Unlock()
		return
	}
	latestAck := flow.latestAck
	carriers := append([]*Carrier(nil), flow.carriers...)
	flow.mu.Unlock()
	enqueueAckOnCarriers(carriers, stopping, latestAck)
}

func (flow *Flow) carrierFrameWritten(_ *Carrier, frame Frame) {
	if frame.Type != FrameAck || !ackHasFIN(frame) {
		return
	}
	flow.mu.Lock()
	flow.remoteFINAckSent = true
	complete := flow.completeLocked()
	flow.mu.Unlock()
	if complete {
		flow.reset(nil, false)
	}
}

func (flow *Flow) finishRemote() {
	flow.mu.Lock()
	if flow.remoteFIN {
		flow.mu.Unlock()
		return
	}
	flow.remoteFIN = true
	complete := flow.completeLocked()
	flow.mu.Unlock()
	if closeWriter, ok := flow.endpoint.(interface{ CloseWrite() error }); ok {
		_ = closeWriter.CloseWrite()
	}
	if complete {
		flow.reset(nil, false)
	}
}

func (flow *Flow) broadcast(frame Frame) error {
	if frame.Type != FrameData && frame.Type != FrameFIN {
		return flow.broadcastControl(frame)
	}
	sequence, err := flow.remember(frame)
	if err != nil {
		return err
	}
	flow.mu.Lock()
	for {
		// ACK processing can prune this frame before the carrier that wrote it
		// detaches, so peer acknowledgement must outlive carrier state.
		if flow.globalAcknowledged(frame) {
			flow.mu.Unlock()
			return nil
		}
		if flow.closed {
			flow.mu.Unlock()
			return ErrNoCarriers
		}
		if len(flow.carriers) == 0 {
			if !flow.beginRecoveryLocked() {
				flow.mu.Unlock()
				return ErrNoCarriers
			}
			flow.cond.Wait()
			continue
		}
		for _, state := range flow.carrierStates {
			if state.hasWritten && state.writtenSequence >= sequence {
				flow.mu.Unlock()
				return nil
			}
		}
		flow.cond.Wait()
	}
}

func (flow *Flow) broadcastControl(frame Frame) error {
	flow.mu.Lock()
	carriers := append([]*Carrier(nil), flow.carriers...)
	flow.mu.Unlock()
	for _, carrier := range carriers {
		if carrier.enqueue(frame, true) {
			return nil
		}
	}
	return ErrNoCarriers
}

func (flow *Flow) remember(frame Frame) (uint64, error) {
	flow.mu.Lock()
	var skipped []skipNotification
	defer func() {
		flow.mu.Unlock()
		reportSkipped(skipped)
	}()
	for flow.config.ReorderWindowBytes > 0 && flow.historyBytes+len(frame.Payload) > flow.config.ReorderWindowBytes {
		skipped = append(skipped, flow.pruneHistoryLocked()...)
		if flow.historyBytes+len(frame.Payload) <= flow.config.ReorderWindowBytes {
			break
		}
		if flow.closed {
			return 0, ErrNoCarriers
		}
		if len(flow.carriers) == 0 && !flow.beginRecoveryLocked() {
			return 0, ErrNoCarriers
		}
		flow.cond.Wait()
	}
	if flow.closed {
		return 0, ErrNoCarriers
	}
	if len(flow.carriers) == 0 && !flow.beginRecoveryLocked() {
		return 0, ErrNoCarriers
	}
	sequence := flow.nextSequence
	flow.nextSequence++
	frame.Payload = append([]byte(nil), frame.Payload...)
	flow.history = append(flow.history, replayFrame{sequence: sequence, frame: frame})
	flow.historyBytes += len(frame.Payload)
	flow.cond.Broadcast()
	return sequence, nil
}

func (flow *Flow) sendCarrier(carrier *Carrier) {
	for {
		flow.mu.Lock()
		state, ok := flow.carrierStates[carrier]
		if !ok || flow.closed {
			flow.mu.Unlock()
			return
		}
		var item replayFrame
		found := false
		for _, candidate := range flow.history {
			if candidate.sequence >= state.nextSequence {
				item = candidate
				found = true
				break
			}
		}
		if !found {
			flow.cond.Wait()
			flow.mu.Unlock()
			continue
		}
		state.inFlightSequence = item.sequence
		state.hasInFlight = true
		flow.mu.Unlock()
		if !carrier.enqueue(item.frame, true) {
			return
		}
		var skipped []skipNotification
		complete := false
		flow.mu.Lock()
		if state, ok = flow.carrierStates[carrier]; ok {
			state.hasInFlight = false
			state.nextSequence = item.sequence + 1
			state.writtenSequence = item.sequence
			state.hasWritten = true
			if item.frame.Type == FrameData {
				state.writtenOffset = item.frame.Offset + uint64(len(item.frame.Payload))
				flow.writtenOffset = max(flow.writtenOffset, state.writtenOffset)
			} else if item.frame.Type == FrameFIN {
				state.writtenOffset = item.frame.Offset
				state.writtenFIN = true
				flow.writtenOffset = max(flow.writtenOffset, item.frame.Offset)
				flow.writtenFIN = true
				flow.writtenFINOffset = item.frame.Offset
			}
			skipped, complete = flow.applyDeferredAcknowledgementLocked()
			flow.cond.Broadcast()
		}
		flow.mu.Unlock()
		reportSkipped(skipped)
		if complete {
			flow.reset(nil, false)
			return
		}
	}
}

func (flow *Flow) acknowledge(carrier *Carrier, offset uint64, fin bool) {
	flow.mu.Lock()
	_, ok := flow.carrierStates[carrier]
	if !ok || flow.acknowledgementRegressesLocked(offset, fin) {
		flow.mu.Unlock()
		return
	}
	if !flow.acknowledgementWrittenLocked(offset, fin) {
		if flow.acknowledgementRememberedLocked(offset, fin) {
			flow.deferAcknowledgementLocked(offset, fin)
		}
		flow.mu.Unlock()
		return
	}
	skipped, complete := flow.applyAcknowledgementLocked(offset, fin)
	flow.cond.Broadcast()
	flow.mu.Unlock()
	reportSkipped(skipped)
	if complete {
		flow.reset(nil, false)
	}
}

func (flow *Flow) acknowledgementRegressesLocked(offset uint64, fin bool) bool {
	return offset < flow.globalAckOffset || (flow.globalAckFIN && !fin)
}

func (flow *Flow) acknowledgementWrittenLocked(offset uint64, fin bool) bool {
	return offset <= flow.writtenOffset && (!fin || (flow.writtenFIN && offset == flow.writtenFINOffset))
}

func (flow *Flow) acknowledgementRememberedLocked(offset uint64, fin bool) bool {
	for _, item := range flow.history {
		if fin {
			if item.frame.Type == FrameFIN && offset == item.frame.Offset {
				return true
			}
			continue
		}
		end := item.frame.Offset
		if item.frame.Type == FrameData {
			end += uint64(len(item.frame.Payload))
		}
		if offset <= end {
			return true
		}
	}
	return false
}

func (flow *Flow) deferAcknowledgementLocked(offset uint64, fin bool) {
	if flow.hasDeferredAck && (flow.deferredAckOffset > offset || (flow.deferredAckOffset == offset && flow.deferredAckFIN)) {
		return
	}
	flow.deferredAckOffset = offset
	flow.deferredAckFIN = fin
	flow.hasDeferredAck = true
}

func (flow *Flow) applyDeferredAcknowledgementLocked() ([]skipNotification, bool) {
	if !flow.hasDeferredAck {
		return nil, false
	}
	offset := flow.deferredAckOffset
	fin := flow.deferredAckFIN
	if flow.acknowledgementRegressesLocked(offset, fin) {
		flow.hasDeferredAck = false
		return nil, false
	}
	if !flow.acknowledgementWrittenLocked(offset, fin) {
		return nil, false
	}
	flow.hasDeferredAck = false
	return flow.applyAcknowledgementLocked(offset, fin)
}

func (flow *Flow) applyAcknowledgementLocked(offset uint64, fin bool) ([]skipNotification, bool) {
	flow.globalAckOffset = offset
	flow.globalAckFIN = flow.globalAckFIN || fin
	if fin {
		flow.localFINAck = true
	}
	if flow.hasDeferredAck && (offset > flow.deferredAckOffset || (offset == flow.deferredAckOffset && (fin || !flow.deferredAckFIN))) {
		flow.hasDeferredAck = false
	}
	skipped := flow.pruneHistoryLocked()
	return skipped, flow.completeLocked()
}

func (flow *Flow) completeLocked() bool {
	return flow.exchangeCompleteLocked() && flow.remoteFINAckSent
}

func (flow *Flow) exchangeCompleteLocked() bool {
	return flow.localFIN && flow.remoteFIN && flow.localFINAcknowledgedLocked()
}

func (flow *Flow) localFINAcknowledgedLocked() bool {
	return flow.localFINAck || (flow.hasDeferredAck && flow.deferredAckFIN && flow.acknowledgementRememberedLocked(flow.deferredAckOffset, true))
}

func (flow *Flow) pruneHistoryLocked() []skipNotification {
	var skipped []skipNotification
	kept := flow.history[:0]
	for _, item := range flow.history {
		if flow.globalAcknowledged(item.frame) {
			for carrier, state := range flow.carrierStates {
				if item.sequence < state.nextSequence || (state.hasInFlight && item.sequence == state.inFlightSequence) {
					continue
				}
				skipped = append(skipped, skipNotification{carrier: carrier, frame: item.frame})
			}
			flow.historyBytes -= len(item.frame.Payload)
		} else {
			kept = append(kept, item)
		}
	}
	flow.history = kept
	return skipped
}

func reportSkipped(skipped []skipNotification) {
	for _, notification := range skipped {
		notification.carrier.reportSkip(notification.frame)
	}
}

func (flow *Flow) globalAcknowledged(frame Frame) bool {
	if frame.Type == FrameFIN {
		return flow.globalAckFIN && flow.globalAckOffset >= frame.Offset
	}
	return flow.globalAckOffset >= frame.Offset+uint64(len(frame.Payload))
}

func (flow *Flow) ackFrame(offset uint64, fin bool) Frame {
	flag := byte(0)
	if fin {
		flag = 1
	}
	return Frame{Type: FrameAck, Direction: oppositeDirection(flow.sendDirection), StreamID: flow.id, Offset: offset, Payload: []byte{flag}}
}

func oppositeDirection(direction Direction) Direction {
	if direction == DirectionClientToServer {
		return DirectionServerToClient
	}
	return DirectionClientToServer
}

func (flow *Flow) detach(carrier *Carrier, preserveInbound bool) {
	flow.mu.Lock()
	var skipped []skipNotification
	finishReset := false
	for index, current := range flow.carriers {
		if current == carrier {
			flow.carriers = append(flow.carriers[:index], flow.carriers[index+1:]...)
			delete(flow.carrierStates, carrier)
			skipped = flow.pruneHistoryLocked()
			break
		}
	}
	lastCarrier := flow.started && !flow.closed && len(flow.carriers) == 0
	shouldHandleLoss := lastCarrier && (flow.config.RecoveryTimeout > 0 || !preserveInbound || flow.pendingInbound == 0)
	if shouldHandleLoss {
		if flow.lossCompleteLocked() {
			finishReset = flow.claimResetLocked(nil, false)
		} else if !flow.beginRecoveryLocked() {
			finishReset = flow.claimResetLocked(ErrNoCarriers, true)
		}
	}
	resendAck := flow.hasLatestAck && !flow.closed && len(flow.carriers) > 0
	latestAck := flow.latestAck
	carriers := append([]*Carrier(nil), flow.carriers...)
	flow.cond.Broadcast()
	flow.mu.Unlock()
	reportSkipped(skipped)
	if resendAck {
		enqueueAckOnCarriers(carriers, nil, latestAck)
	}
	if finishReset {
		flow.finishReset()
	}
}

func (flow *Flow) resetAfterLastCarrier() {
	flow.mu.Lock()
	if flow.closed || len(flow.carriers) != 0 {
		flow.mu.Unlock()
		return
	}
	if flow.lossCompleteLocked() {
		finishReset := flow.claimResetLocked(nil, false)
		flow.mu.Unlock()
		if finishReset {
			flow.finishReset()
		}
		return
	}
	if flow.beginRecoveryLocked() {
		flow.mu.Unlock()
		return
	}
	finishReset := flow.claimResetLocked(ErrNoCarriers, true)
	flow.mu.Unlock()
	if finishReset {
		flow.finishReset()
	}
}

func (flow *Flow) resetAfterCarrierFailure() {
	flow.mu.Lock()
	if flow.closed {
		flow.mu.Unlock()
		return
	}
	if flow.lossCompleteLocked() {
		finishReset := flow.claimResetLocked(nil, false)
		flow.mu.Unlock()
		if finishReset {
			flow.finishReset()
		}
		return
	}
	if flow.beginRecoveryLocked() {
		flow.mu.Unlock()
		return
	}
	finishReset := flow.claimResetLocked(ErrNoCarriers, true)
	flow.mu.Unlock()
	if finishReset {
		flow.finishReset()
	}
}

func (flow *Flow) lossCompleteLocked() bool {
	if flow.config.RecoveryTimeout > 0 {
		return flow.completeLocked()
	}
	return flow.exchangeCompleteLocked()
}

func (flow *Flow) reset(err error, notify bool) {
	flow.mu.Lock()
	finishReset := flow.claimResetLocked(err, notify)
	flow.mu.Unlock()
	if finishReset {
		flow.finishReset()
	}
}

func (flow *Flow) claimResetLocked(err error, notify bool) bool {
	if flow.closed {
		return false
	}
	flow.closed = true
	flow.terminalErr = err
	flow.terminalNotify = notify
	flow.terminalAbortive = flow.config.RecoveryTimeout > 0 && errors.Is(err, ErrNoCarriers)
	flow.stopRecoveryLocked()
	close(flow.inboundStop)
	flow.cond.Broadcast()
	return true
}

func (flow *Flow) finishReset() {
	flow.closeOnce.Do(func() {
		flow.mu.Lock()
		carriers := append([]*Carrier(nil), flow.carriers...)
		notify := flow.terminalNotify
		abortive := flow.terminalAbortive
		flow.carriers = nil
		flow.carrierStates = make(map[*Carrier]*carrierState)
		flow.cond.Broadcast()
		flow.mu.Unlock()
		flow.inboundEnqueueWG.Wait()
		flow.discardInbound()
		if abortive {
			if linger, ok := flow.endpoint.(interface{ SetLinger(int) error }); ok {
				_ = linger.SetLinger(0)
			}
		}
		_ = flow.endpoint.Close()
		if notify && len(carriers) > 0 {
			rst := Frame{Type: FrameRST, Direction: flow.sendDirection, StreamID: flow.id, Payload: []byte{0, 1}}
			for _, carrier := range carriers {
				if carrier.enqueue(rst, true) {
					break
				}
			}
		}
		for _, carrier := range carriers {
			_ = carrier.Close()
		}
		close(flow.done)
	})
}

func (flow *Flow) beginRecoveryLocked() bool {
	if flow.config.RecoveryTimeout <= 0 || flow.closed || !flow.started || len(flow.carriers) != 0 {
		return false
	}
	if flow.recovering {
		return true
	}
	flow.recovering = true
	flow.recoveryEpoch++
	epoch := flow.recoveryEpoch
	flow.recoveryDeadline = flowRecoveryNow().Add(flow.config.RecoveryTimeout)
	flow.recoveryTimer = newFlowRecoveryTimer(flow.config.RecoveryTimeout, func() {
		flow.expireRecovery(epoch)
	})
	flow.cond.Broadcast()
	return true
}

func (flow *Flow) stopRecoveryLocked() {
	if flow.recoveryTimer != nil {
		flow.recoveryTimer.Stop()
		flow.recoveryTimer = nil
	}
	flow.recovering = false
	flow.recoveryDeadline = time.Time{}
}

func (flow *Flow) expireRecovery(epoch uint64) {
	flow.mu.Lock()
	if flow.closed || !flow.recovering || flow.recoveryEpoch != epoch || len(flow.carriers) != 0 {
		flow.mu.Unlock()
		return
	}
	finishReset := flow.claimResetLocked(ErrNoCarriers, true)
	flow.mu.Unlock()
	if finishReset {
		flow.finishReset()
	}
}
