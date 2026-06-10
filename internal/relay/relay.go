package relay

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/udp"
)

const (
	DefaultQueueSize              = 16
	DefaultWriteBufferBytes       = 4 * 1024 * 1024
	DefaultWriteBufferTargetBytes = 4 * 1024 * 1024
	DefaultWriteBatchSize         = udp.DefaultBatchSize
)

type UDPWriter interface {
	SetWriteDeadline(time.Time) error
	SetWriteBuffer(int) error
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
}

func SetWriteBufferForTargets(conn UDPWriter, targetCount int) error {
	if conn == nil {
		return nil
	}
	if targetCount < 1 {
		targetCount = 1
	}
	return conn.SetWriteBuffer(DefaultWriteBufferBytes + (targetCount-1)*DefaultWriteBufferTargetBytes)
}

type Target struct {
	ID   string
	Conn UDPWriter
	Addr *net.UDPAddr
}

type Result struct {
	ID  string
	Err error
}

type Dispatcher struct {
	writeTimeoutMillis int64
	queueSize          int
	writeBatchEnabled  bool
	writeBatchSize     int
	onError            func(Result)

	mu      sync.Mutex
	closed  bool
	targets map[string]*targetState
	workers map[UDPWriter]*connWorker
}

type targetState struct {
	id      string
	conn    UDPWriter
	addr    *net.UDPAddr
	addrKey string
	worker  *connWorker
	active  atomic.Bool
}

type connWorker struct {
	conn               UDPWriter
	writeTimeoutMillis int64
	writeBatchEnabled  bool
	writeBatchSize     int
	packets            chan queuedBatch
	stop               chan struct{}
	stopOnce           sync.Once
	dispatcher         *Dispatcher
}

type queuedBatch struct {
	packets []queuedPacket
}

type queuedPacket struct {
	state   *targetState
	id      string
	conn    UDPWriter
	addr    *net.UDPAddr
	addrKey string
	payload []byte
}

func NewDispatcher(writeTimeoutMillis int64, queueSize int, onError func(Result)) *Dispatcher {
	return NewDispatcherWithBatch(writeTimeoutMillis, queueSize, true, DefaultWriteBatchSize, onError)
}

func NewDispatcherWithBatch(writeTimeoutMillis int64, queueSize int, writeBatchEnabled bool, writeBatchSize int, onError func(Result)) *Dispatcher {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	writeBatchSize = udp.NormalizeBatchSize(writeBatchSize)
	return &Dispatcher{
		writeTimeoutMillis: writeTimeoutMillis,
		queueSize:          queueSize,
		writeBatchEnabled:  writeBatchEnabled,
		writeBatchSize:     writeBatchSize,
		onError:            onError,
		targets:            make(map[string]*targetState),
		workers:            make(map[UDPWriter]*connWorker),
	}
}

func (dispatcher *Dispatcher) Fanout(payload []byte, targets []Target) {
	payloads := [1][]byte{payload}
	dispatcher.FanoutBatch(payloads[:], targets)
}

func (dispatcher *Dispatcher) FanoutBatch(payloads [][]byte, targets []Target) {
	if len(payloads) == 0 {
		return
	}
	states := dispatcher.syncTargets(targets)
	if len(states) == 0 {
		return
	}
	groups := make([]queuedBatchGroup, 0, len(states))
	for _, state := range states {
		groupIndex := -1
		for i := range groups {
			if groups[i].worker == state.worker {
				groupIndex = i
				break
			}
		}
		if groupIndex < 0 {
			groups = append(groups, queuedBatchGroup{worker: state.worker})
			groupIndex = len(groups) - 1
		}
		groups[groupIndex].states = append(groups[groupIndex].states, state)
	}
	payloadCopies := make([][]byte, 0, len(payloads))
	for _, payload := range payloads {
		payloadCopies = append(payloadCopies, append([]byte(nil), payload...))
	}
	for i := range groups {
		groups[i].packets = make([]queuedPacket, 0, len(groups[i].states)*len(payloadCopies))
		for _, payload := range payloadCopies {
			for _, state := range groups[i].states {
				groups[i].packets = append(groups[i].packets, queuedPacket{
					state:   state,
					id:      state.id,
					conn:    state.conn,
					addr:    state.addr,
					addrKey: state.addrKey,
					payload: payload,
				})
			}
		}
	}
	for _, group := range groups {
		group.worker.enqueue(queuedBatch{packets: group.packets})
	}
}

type queuedBatchGroup struct {
	worker  *connWorker
	states  []*targetState
	packets []queuedPacket
}

func (dispatcher *Dispatcher) Remove(id string) {
	dispatcher.mu.Lock()
	target, ok := dispatcher.targets[id]
	if ok {
		delete(dispatcher.targets, id)
		target.active.Store(false)
		dispatcher.stopWorkerIfUnusedLocked(target.worker)
	}
	dispatcher.mu.Unlock()
}

func (dispatcher *Dispatcher) Close() {
	dispatcher.mu.Lock()
	workers := dispatcher.workers
	for _, target := range dispatcher.targets {
		target.active.Store(false)
	}
	dispatcher.workers = make(map[UDPWriter]*connWorker)
	dispatcher.targets = make(map[string]*targetState)
	dispatcher.closed = true
	dispatcher.mu.Unlock()

	for _, worker := range workers {
		worker.stopWorker()
	}
}

func (dispatcher *Dispatcher) syncTargets(targets []Target) []*targetState {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed {
		return nil
	}

	active := make(map[string]struct{}, len(targets))
	states := make([]*targetState, 0, len(targets))
	for _, target := range targets {
		if target.Conn == nil || target.Addr == nil {
			continue
		}
		if _, exists := active[target.ID]; exists {
			continue
		}
		active[target.ID] = struct{}{}

		addrKey := target.Addr.String()
		worker := dispatcher.workerForConnLocked(target.Conn)
		state := dispatcher.targets[target.ID]
		if state == nil || state.conn != target.Conn || state.addrKey != addrKey {
			oldWorker := (*connWorker)(nil)
			if state != nil {
				oldWorker = state.worker
				state.active.Store(false)
			}
			state = &targetState{id: target.ID, conn: target.Conn, addr: target.Addr, addrKey: addrKey, worker: worker}
			state.active.Store(true)
			dispatcher.targets[target.ID] = state
			if oldWorker != nil && oldWorker != worker {
				dispatcher.stopWorkerIfUnusedLocked(oldWorker)
			}
		}
		states = append(states, state)
	}

	for id, target := range dispatcher.targets {
		if _, ok := active[id]; !ok {
			delete(dispatcher.targets, id)
			target.active.Store(false)
			dispatcher.stopWorkerIfUnusedLocked(target.worker)
		}
	}
	return states
}

func (dispatcher *Dispatcher) workerForConnLocked(conn UDPWriter) *connWorker {
	worker := dispatcher.workers[conn]
	if worker != nil {
		return worker
	}
	worker = &connWorker{
		conn:               conn,
		writeTimeoutMillis: dispatcher.writeTimeoutMillis,
		writeBatchEnabled:  dispatcher.writeBatchEnabled,
		writeBatchSize:     dispatcher.writeBatchSize,
		packets:            make(chan queuedBatch, dispatcher.queueSize),
		stop:               make(chan struct{}),
		dispatcher:         dispatcher,
	}
	dispatcher.workers[conn] = worker
	go worker.run()
	return worker
}

func (dispatcher *Dispatcher) stopWorkerIfUnusedLocked(worker *connWorker) {
	if worker == nil {
		return
	}
	for _, target := range dispatcher.targets {
		if target.worker == worker {
			return
		}
	}
	delete(dispatcher.workers, worker.conn)
	worker.stopWorker()
}

func (dispatcher *Dispatcher) targetActive(packet queuedPacket) bool {
	return packet.state != nil && packet.state.active.Load()
}

func (dispatcher *Dispatcher) failTarget(packet queuedPacket, err error) {
	dispatcher.mu.Lock()
	target, ok := dispatcher.targets[packet.id]
	if ok && target == packet.state {
		delete(dispatcher.targets, packet.id)
		target.active.Store(false)
		dispatcher.stopWorkerIfUnusedLocked(target.worker)
	} else {
		ok = false
	}
	dispatcher.mu.Unlock()

	if ok && dispatcher.onError != nil {
		dispatcher.onError(Result{ID: packet.id, Err: err})
	}
}

func (worker *connWorker) enqueue(batch queuedBatch) {
	select {
	case <-worker.stop:
	case worker.packets <- batch:
	default:
	}
}

func (worker *connWorker) run() {
	queued := make([]queuedPacket, 0, worker.writeBatchSize)
	chunk := make([]queuedPacket, 0, worker.writeBatchSize)
	packets := make([]udp.Packet, 0, worker.writeBatchSize)
	for {
		select {
		case <-worker.stop:
			return
		case batch := <-worker.packets:
			queued = worker.collectBatch(queued, batch)
			chunk, packets = worker.writeQueued(queued, chunk, packets)
		}
	}
}

func (worker *connWorker) collectBatch(queued []queuedPacket, batch queuedBatch) []queuedPacket {
	if len(batch.packets) >= worker.writeBatchSize {
		return batch.packets
	}
	queued = append(queued[:0], batch.packets...)
	for len(queued) < worker.writeBatchSize {
		select {
		case batch := <-worker.packets:
			queued = append(queued, batch.packets...)
		default:
			return queued
		}
	}
	return queued
}

func (worker *connWorker) writeQueued(queued []queuedPacket, chunk []queuedPacket, packets []udp.Packet) ([]queuedPacket, []udp.Packet) {
	chunk = chunk[:0]
	packets = packets[:0]
	for _, packet := range queued {
		if !worker.dispatcher.targetActive(packet) {
			continue
		}
		chunk = append(chunk, packet)
		packets = append(packets, udp.Packet{Payload: packet.payload, Addr: packet.addr})
		if len(packets) == worker.writeBatchSize {
			worker.writeChunk(chunk, packets)
			chunk = chunk[:0]
			packets = packets[:0]
		}
	}
	if len(packets) > 0 {
		worker.writeChunk(chunk, packets)
	}
	return chunk[:0], packets[:0]
}

func (worker *connWorker) writeChunk(queued []queuedPacket, packets []udp.Packet) {
	if len(packets) == 0 {
		return
	}
	if worker.writeTimeoutMillis > 0 {
		deadline := time.Now().Add(time.Duration(worker.writeTimeoutMillis) * time.Millisecond)
		if err := worker.conn.SetWriteDeadline(deadline); err != nil {
			for _, packet := range queued {
				worker.dispatcher.failTarget(packet, err)
			}
			return
		}
	}
	batchCapable := worker.writeBatchEnabled && len(packets) > 1
	if _, ok := worker.conn.(udp.BatchWriter); !ok {
		batchCapable = false
	}
	written, err := udp.WriteBatch(worker.conn, packets, worker.writeBatchEnabled)
	if err == nil && written == len(packets) {
		return
	}
	if written < 0 {
		written = 0
	}
	if written > len(packets) {
		written = len(packets)
	}
	if !batchCapable {
		worker.failSequentialWrite(queued, written, err)
		return
	}
	worker.writeRemainingIndividually(queued, packets, written)
}

func (worker *connWorker) failSequentialWrite(queued []queuedPacket, written int, err error) {
	if written >= len(queued) {
		return
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	worker.dispatcher.failTarget(queued[written], err)
	worker.writeRemainingIndividually(queued, nil, written+1)
}

func (worker *connWorker) writeRemainingIndividually(queued []queuedPacket, packets []udp.Packet, start int) {
	if start < 0 {
		start = 0
	}
	for i := start; i < len(queued); i++ {
		packet := queued[i]
		if !worker.dispatcher.targetActive(packet) {
			continue
		}
		payload := packet.payload
		if len(packets) > i {
			payload = packets[i].Payload
		}
		n, err := worker.conn.WriteToUDP(payload, packet.addr)
		if err != nil {
			worker.dispatcher.failTarget(packet, err)
			continue
		}
		if n != len(payload) {
			worker.dispatcher.failTarget(packet, io.ErrShortWrite)
		}
	}
}

func (worker *connWorker) stopWorker() {
	worker.stopOnce.Do(func() { close(worker.stop) })
}
