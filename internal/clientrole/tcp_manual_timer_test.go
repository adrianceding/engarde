package clientrole

import (
	"sync"
	"testing"
	"time"
)

type tcpManualTimer struct {
	delay time.Duration
	ticks chan time.Time

	mu      sync.Mutex
	stopped bool
	fired   bool
}

func (timer *tcpManualTimer) fire(t *testing.T) {
	t.Helper()
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if timer.stopped {
		t.Fatal("attempted to fire a stopped manual timer")
	}
	if timer.fired {
		t.Fatal("attempted to fire a manual timer more than once")
	}
	timer.fired = true
	timer.ticks <- time.Time{}
}

func (timer *tcpManualTimer) stop() {
	timer.mu.Lock()
	timer.stopped = true
	timer.mu.Unlock()
}

type tcpManualTimerFactory struct {
	mu      sync.Mutex
	pending []*tcpManualTimer
	ready   chan struct{}
}

func newTCPManualTimerFactory() *tcpManualTimerFactory {
	return &tcpManualTimerFactory{ready: make(chan struct{}, 1)}
}

func (factory *tcpManualTimerFactory) newTimer(delay time.Duration) (<-chan time.Time, func()) {
	timer := &tcpManualTimer{
		delay: delay,
		ticks: make(chan time.Time, 1),
	}
	factory.mu.Lock()
	factory.pending = append(factory.pending, timer)
	factory.mu.Unlock()
	select {
	case factory.ready <- struct{}{}:
	default:
	}
	return timer.ticks, timer.stop
}

func (factory *tcpManualTimerFactory) next(t *testing.T) *tcpManualTimer {
	t.Helper()
	deadline := time.NewTimer(tcpSessionManagerTestTimeout)
	defer deadline.Stop()
	for {
		factory.mu.Lock()
		if len(factory.pending) != 0 {
			timer := factory.pending[0]
			factory.pending[0] = nil
			factory.pending = factory.pending[1:]
			factory.mu.Unlock()
			return timer
		}
		factory.mu.Unlock()
		select {
		case <-factory.ready:
		case <-deadline.C:
			t.Fatal("retry did not request a timer")
			return nil
		}
	}
}

func (factory *tcpManualTimerFactory) assertNoPending(t *testing.T) {
	t.Helper()
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if len(factory.pending) != 0 {
		t.Fatalf("unexpected additional timers = %d (first delay %v)", len(factory.pending), factory.pending[0].delay)
	}
}

func installTCPManualSessionRetryTimer(t *testing.T) *tcpManualTimerFactory {
	t.Helper()
	factory := newTCPManualTimerFactory()
	previousTimer := newTCPSessionRetryTimer
	previousJitter := jitterTCPSessionRetryDelay
	newTCPSessionRetryTimer = factory.newTimer
	jitterTCPSessionRetryDelay = func(delay time.Duration) time.Duration { return delay }
	t.Cleanup(func() {
		newTCPSessionRetryTimer = previousTimer
		jitterTCPSessionRetryDelay = previousJitter
	})
	return factory
}

func installTCPManualFlowRetryTimer(t *testing.T) *tcpManualTimerFactory {
	t.Helper()
	factory := newTCPManualTimerFactory()
	previousTimer := newTCPFlowRetryTimer
	previousJitter := jitterTCPSessionRetryDelay
	newTCPFlowRetryTimer = factory.newTimer
	jitterTCPSessionRetryDelay = func(delay time.Duration) time.Duration { return delay }
	t.Cleanup(func() {
		newTCPFlowRetryTimer = previousTimer
		jitterTCPSessionRetryDelay = previousJitter
	})
	return factory
}

func installTCPManualFlowOpenTimer(t *testing.T) *tcpManualTimerFactory {
	t.Helper()
	factory := newTCPManualTimerFactory()
	previousTimer := newTCPFlowOpenTimer
	newTCPFlowOpenTimer = factory.newTimer
	t.Cleanup(func() { newTCPFlowOpenTimer = previousTimer })
	return factory
}
