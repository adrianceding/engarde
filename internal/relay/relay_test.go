package relay

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type fakeWriter struct {
	mu           sync.Mutex
	deadline     time.Time
	writes       int
	deadlineErr  error
	writeErr     error
	writtenBytes []byte
	block        chan struct{}
}

func (writer *fakeWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.deadline = deadline
	return writer.deadlineErr
}

func (writer *fakeWriter) SetWriteBuffer(int) error { return nil }

func (writer *fakeWriter) WriteToUDP(payload []byte, addr *net.UDPAddr) (int, error) {
	if writer.block != nil {
		<-writer.block
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.writes++
	writer.writtenBytes = append(writer.writtenBytes[:0], payload...)
	if writer.writeErr != nil {
		return 0, writer.writeErr
	}
	return len(payload), nil
}

func (writer *fakeWriter) writeCount() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writes
}

func (writer *fakeWriter) hasDeadline() bool {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return !writer.deadline.IsZero()
}

func waitForWrites(t *testing.T, writer *fakeWriter, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if writer.writeCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("writes = %d, want at least %d", writer.writeCount(), want)
}

func TestDispatcherWritesAllTargets(t *testing.T) {
	first := &fakeWriter{}
	second := &fakeWriter{}
	targets := []Target{
		{ID: "first", Conn: first, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}},
		{ID: "second", Conn: second, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}},
	}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Fanout([]byte("packet"), targets)
	waitForWrites(t, first, 1)
	waitForWrites(t, second, 1)
	if !first.hasDeadline() || !second.hasDeadline() {
		t.Fatal("deadline was not set for positive write timeout")
	}
}

func TestDispatcherReportsDeadlineAndWriteErrors(t *testing.T) {
	deadlineErr := errors.New("deadline")
	writeErr := errors.New("write")
	first := &fakeWriter{deadlineErr: deadlineErr}
	second := &fakeWriter{writeErr: writeErr}
	targets := []Target{
		{ID: "first", Conn: first, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}},
		{ID: "second", Conn: second, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}},
	}
	errorsCh := make(chan Result, 2)
	dispatcher := NewDispatcher(10, 2, func(result Result) { errorsCh <- result })
	t.Cleanup(dispatcher.Close)

	dispatcher.Fanout([]byte("packet"), targets)
	results := []Result{readResult(t, errorsCh), readResult(t, errorsCh)}
	byID := map[string]error{}
	for _, result := range results {
		byID[result.ID] = result.Err
	}

	if !errors.Is(byID["first"], deadlineErr) {
		t.Fatalf("first error = %v, want %v", byID["first"], deadlineErr)
	}
	if !errors.Is(byID["second"], writeErr) {
		t.Fatalf("second error = %v, want %v", byID["second"], writeErr)
	}
	if first.writeCount() != 0 {
		t.Fatalf("first writes = %d, want 0 after deadline error", first.writeCount())
	}
}

func TestDispatcherNegativeTimeoutDoesNotSetDeadline(t *testing.T) {
	writer := &fakeWriter{}
	dispatcher := NewDispatcher(-1, 2, nil)
	t.Cleanup(dispatcher.Close)
	dispatcher.Fanout([]byte("packet"), []Target{{ID: "target", Conn: writer, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}}})
	waitForWrites(t, writer, 1)
	if writer.hasDeadline() {
		t.Fatal("deadline was set for negative timeout")
	}
}

func TestDispatcherSkipsInvalidTargets(t *testing.T) {
	writer := &fakeWriter{}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)
	dispatcher.Fanout([]byte("packet"), []Target{{ID: "nil-conn", Addr: &net.UDPAddr{}}, {ID: "nil-addr", Conn: writer}})
	time.Sleep(20 * time.Millisecond)
	if writer.writeCount() != 0 {
		t.Fatalf("writes = %d, want 0", writer.writeCount())
	}
}

func TestDispatcherDoesNotBlockOnFullTargetQueue(t *testing.T) {
	blocked := &fakeWriter{block: make(chan struct{})}
	fast := &fakeWriter{}
	targets := []Target{
		{ID: "blocked", Conn: blocked, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}},
		{ID: "fast", Conn: fast, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}},
	}
	dispatcher := NewDispatcher(10, 1, nil)
	t.Cleanup(func() {
		close(blocked.block)
		dispatcher.Close()
	})

	dispatcher.Fanout([]byte("first"), targets)
	waitForWrites(t, fast, 1)
	dispatcher.Fanout([]byte("second"), targets)
	waitForWrites(t, fast, 2)
	start := time.Now()
	dispatcher.Fanout([]byte("third"), targets)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Fanout blocked for %v with full target queue", elapsed)
	}
	waitForWrites(t, fast, 3)
	if blocked.writeCount() != 0 {
		t.Fatalf("blocked writes = %d, want 0 while blocked", blocked.writeCount())
	}
}

func readResult(t *testing.T, results <-chan Result) Result {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatcher error")
	}
	return Result{}
}
