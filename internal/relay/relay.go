package relay

import (
	"net"
	"sync"
	"time"
)

const (
	DefaultQueueSize              = 16
	DefaultWriteBufferBytes       = 4 * 1024 * 1024
	DefaultWriteBufferTargetBytes = 4 * 1024 * 1024
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
	onError            func(Result)

	mu      sync.Mutex
	closed  bool
	workers map[string]*targetWorker
}

type targetWorker struct {
	id                 string
	conn               UDPWriter
	addr               *net.UDPAddr
	writeTimeoutMillis int64
	packets            chan []byte
	stop               chan struct{}
	stopOnce           sync.Once
	dispatcher         *Dispatcher
}

func NewDispatcher(writeTimeoutMillis int64, queueSize int, onError func(Result)) *Dispatcher {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	return &Dispatcher{
		writeTimeoutMillis: writeTimeoutMillis,
		queueSize:          queueSize,
		onError:            onError,
		workers:            make(map[string]*targetWorker),
	}
}

func (dispatcher *Dispatcher) Fanout(payload []byte, targets []Target) {
	workers := dispatcher.syncTargets(targets)
	if len(workers) == 0 {
		return
	}
	payloadCopy := append([]byte(nil), payload...)
	for _, worker := range workers {
		worker.enqueue(payloadCopy)
	}
}

func (dispatcher *Dispatcher) Remove(id string) {
	dispatcher.mu.Lock()
	worker, ok := dispatcher.workers[id]
	if ok {
		delete(dispatcher.workers, id)
	}
	dispatcher.mu.Unlock()
	if ok {
		worker.stopWorker()
	}
}

func (dispatcher *Dispatcher) Close() {
	dispatcher.mu.Lock()
	workers := dispatcher.workers
	dispatcher.workers = make(map[string]*targetWorker)
	dispatcher.closed = true
	dispatcher.mu.Unlock()

	for _, worker := range workers {
		worker.stopWorker()
	}
}

func (dispatcher *Dispatcher) syncTargets(targets []Target) []*targetWorker {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed {
		return nil
	}

	active := make(map[string]struct{}, len(targets))
	workers := make([]*targetWorker, 0, len(targets))
	for _, target := range targets {
		if target.Conn == nil || target.Addr == nil {
			continue
		}
		if _, exists := active[target.ID]; exists {
			continue
		}
		active[target.ID] = struct{}{}

		worker := dispatcher.workers[target.ID]
		if worker == nil || worker.conn != target.Conn || worker.addr.String() != target.Addr.String() {
			if worker != nil {
				worker.stopWorker()
			}
			worker = &targetWorker{
				id:                 target.ID,
				conn:               target.Conn,
				addr:               target.Addr,
				writeTimeoutMillis: dispatcher.writeTimeoutMillis,
				packets:            make(chan []byte, dispatcher.queueSize),
				stop:               make(chan struct{}),
				dispatcher:         dispatcher,
			}
			dispatcher.workers[target.ID] = worker
			go worker.run()
		}
		workers = append(workers, worker)
	}

	for id, worker := range dispatcher.workers {
		if _, ok := active[id]; !ok {
			delete(dispatcher.workers, id)
			worker.stopWorker()
		}
	}
	return workers
}

func (dispatcher *Dispatcher) fail(worker *targetWorker, err error) {
	dispatcher.mu.Lock()
	current, ok := dispatcher.workers[worker.id]
	if ok && current == worker {
		delete(dispatcher.workers, worker.id)
	}
	dispatcher.mu.Unlock()
	worker.stopWorker()

	if ok && dispatcher.onError != nil {
		dispatcher.onError(Result{ID: worker.id, Err: err})
	}
}

func (worker *targetWorker) enqueue(payload []byte) {
	select {
	case worker.packets <- payload:
	default:
	}
}

func (worker *targetWorker) run() {
	for {
		select {
		case <-worker.stop:
			return
		case payload := <-worker.packets:
			if err := worker.write(payload); err != nil {
				if !worker.stopped() {
					worker.dispatcher.fail(worker, err)
				}
				return
			}
		}
	}
}

func (worker *targetWorker) write(payload []byte) error {
	if worker.writeTimeoutMillis > 0 {
		deadline := time.Now().Add(time.Duration(worker.writeTimeoutMillis) * time.Millisecond)
		if err := worker.conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	_, err := worker.conn.WriteToUDP(payload, worker.addr)
	return err
}

func (worker *targetWorker) stopWorker() {
	worker.stopOnce.Do(func() { close(worker.stop) })
}

func (worker *targetWorker) stopped() bool {
	select {
	case <-worker.stop:
		return true
	default:
		return false
	}
}
