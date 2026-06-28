package relay

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/udp"
)

type fakeWriter struct {
	mu           sync.Mutex
	deadline     time.Time
	writes       int
	writeBuffer  int
	deadlineErr  error
	writeErr     error
	writtenBytes []byte
	block        chan struct{}
}

type fakeBatchWriter struct {
	fakeWriter
	batchWrites int
	partial     int
	batchErr    error
	addrs       []string
}

func (writer *fakeBatchWriter) WriteBatch(packets []udp.Packet) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.batchWrites++
	writer.writes += len(packets)
	if len(packets) > 0 {
		writer.writtenBytes = append(writer.writtenBytes[:0], packets[len(packets)-1].Payload...)
	}
	for _, packet := range packets {
		if packet.Addr != nil {
			writer.addrs = append(writer.addrs, packet.Addr.String())
		}
	}
	if writer.batchErr != nil {
		return 0, writer.batchErr
	}
	if writer.partial > 0 {
		return writer.partial, nil
	}
	return len(packets), nil
}

func (writer *fakeWriter) SetWriteDeadline(deadline time.Time) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.deadline = deadline
	return writer.deadlineErr
}

func (writer *fakeWriter) SetWriteBuffer(size int) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.writeBuffer = size
	return nil
}

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

func (writer *fakeWriter) bufferSize() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writeBuffer
}

func (writer *fakeBatchWriter) batchWriteCount() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.batchWrites
}

func (writer *fakeBatchWriter) writtenAddrs() []string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return append([]string(nil), writer.addrs...)
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

func TestSetWriteBufferForTargetsCapsSize(t *testing.T) {
	writer := &fakeWriter{}

	if err := SetWriteBufferForTargets(writer, 10000); err != nil {
		t.Fatalf("SetWriteBufferForTargets returned error: %v", err)
	}
	if got := writer.bufferSize(); got != DefaultMaxWriteBufferBytes {
		t.Fatalf("write buffer = %d, want cap %d", got, DefaultMaxWriteBufferBytes)
	}
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

func TestDispatcherSendWritesSingleTarget(t *testing.T) {
	first := &fakeWriter{}
	second := &fakeWriter{}
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Fanout([]byte("fanout"), []Target{{ID: "first", Conn: first, Addr: firstAddr}, {ID: "second", Conn: second, Addr: secondAddr}})
	waitForWrites(t, first, 1)
	waitForWrites(t, second, 1)
	dispatcher.Send([]byte("direct"), Target{ID: "first", Conn: first, Addr: firstAddr})
	waitForWrites(t, first, 2)
	time.Sleep(20 * time.Millisecond)
	if second.writeCount() != 1 {
		t.Fatalf("second writes = %d, want 1", second.writeCount())
	}
}

func TestDispatcherSendDoesNotRegisterFanoutTarget(t *testing.T) {
	first := &fakeWriter{}
	second := &fakeWriter{}
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Send([]byte("direct"), Target{ID: "second", Conn: second, Addr: secondAddr})
	waitForWrites(t, second, 1)
	dispatcher.Fanout([]byte("fanout"), []Target{{ID: "first", Conn: first, Addr: firstAddr}})
	waitForWrites(t, first, 1)
	time.Sleep(20 * time.Millisecond)
	if second.writeCount() != 1 {
		t.Fatalf("second writes = %d, want 1", second.writeCount())
	}

	dispatcher.Send([]byte("again"), Target{ID: "second", Conn: second, Addr: secondAddr})
	waitForWrites(t, second, 2)
}

func TestDispatcherRemoveCleansOneShotTarget(t *testing.T) {
	writer := &fakeWriter{}
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3), Port: 3}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Send([]byte("first"), Target{ID: "target", Conn: writer, Addr: addr})
	waitForWrites(t, writer, 1)
	dispatcher.Remove("target")
	dispatcher.Send([]byte("second"), Target{ID: "target", Conn: writer, Addr: addr})
	waitForWrites(t, writer, 2)
}

func TestDispatcherSendReleasesOneShotStateAfterSuccess(t *testing.T) {
	writer := &fakeWriter{}
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 30), Port: 30}
	dispatcher := NewDispatcher(10, 2, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Send([]byte("first"), Target{ID: "target", Conn: writer, Addr: addr})
	waitForWrites(t, writer, 1)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		dispatcher.mu.Lock()
		_, ok := dispatcher.oneShots["target"]
		dispatcher.mu.Unlock()
		if !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("one-shot state was retained after successful send")
}

func TestDispatcherQueuedOneShotsSharingTargetAllWrite(t *testing.T) {
	writer := &fakeWriter{block: make(chan struct{})}
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 33), Port: 33}
	dispatcher := NewDispatcher(10, 8, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Send([]byte("first"), Target{ID: "target", Conn: writer, Addr: addr})
	dispatcher.Send([]byte("second"), Target{ID: "target", Conn: writer, Addr: addr})
	close(writer.block)
	waitForWrites(t, writer, 2)
}

func TestDispatcherRemoveCancelsQueuedOneShot(t *testing.T) {
	writer := &fakeWriter{block: make(chan struct{})}
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 4), Port: 4}
	dispatcher := NewDispatcher(10, 2, nil)
	dispatcher.Send([]byte("first"), Target{ID: "target", Conn: writer, Addr: addr})
	dispatcher.Remove("target")
	close(writer.block)
	t.Cleanup(dispatcher.Close)
	time.Sleep(20 * time.Millisecond)
	if writer.writeCount() != 0 {
		t.Fatalf("writes = %d, want 0 after one-shot remove", writer.writeCount())
	}
}

func TestDispatcherOneShotPartialBatchReportsFailedTarget(t *testing.T) {
	writeErr := errors.New("individual write")
	writer := &fakeBatchWriter{partial: 1}
	writer.writeErr = writeErr
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 31), Port: 31}
	errorsCh := make(chan Result, 1)
	dispatcher := NewDispatcherWithBatch(10, 4, true, 4, func(result Result) { errorsCh <- result })
	t.Cleanup(dispatcher.Close)

	dispatcher.SendBatch([][]byte{[]byte("first"), []byte("second")}, Target{ID: "target", Conn: writer, Addr: firstAddr})
	result := readResult(t, errorsCh)
	if result.ID != "target" || !errors.Is(result.Err, writeErr) {
		t.Fatalf("result = %#v, want target individual write error", result)
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

func TestDispatcherReportsFanoutQueueOverflow(t *testing.T) {
	blocked := &fakeWriter{block: make(chan struct{})}
	fast := &fakeWriter{}
	errorsCh := make(chan Result, 1)
	targets := []Target{
		{ID: "blocked", Conn: blocked, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}},
		{ID: "fast", Conn: fast, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}},
	}
	dispatcher := NewDispatcher(10, 1, func(result Result) { errorsCh <- result })
	t.Cleanup(func() {
		close(blocked.block)
		dispatcher.Close()
	})

	dispatcher.Fanout([]byte("first"), targets)
	waitForWrites(t, fast, 1)
	dispatcher.Fanout([]byte("second"), targets)
	waitForWrites(t, fast, 2)
	dispatcher.Fanout([]byte("third"), targets)

	result := readResult(t, errorsCh)
	if result.ID != "blocked" {
		t.Fatalf("overflow result ID = %q, want blocked", result.ID)
	}
	if !errors.Is(result.Err, ErrQueueFull) {
		t.Fatalf("overflow error = %v, want ErrQueueFull", result.Err)
	}
	if result.Packets != 1 || result.Bytes != len("third") {
		t.Fatalf("overflow result size = %d packets/%d bytes, want 1/%d", result.Packets, result.Bytes, len("third"))
	}
	dispatcher.mu.Lock()
	_, targetStillActive := dispatcher.targets["blocked"]
	dispatcher.mu.Unlock()
	if !targetStillActive {
		t.Fatal("queue overflow removed target; want target retained")
	}
	waitForWrites(t, fast, 3)
}

func TestDispatcherBatchesTargetsSharingConnection(t *testing.T) {
	writer := &fakeBatchWriter{}
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 10}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 11), Port: 11}
	dispatcher := NewDispatcherWithBatch(10, 4, true, 4, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.Fanout([]byte("packet"), []Target{{ID: "first", Conn: writer, Addr: firstAddr}, {ID: "second", Conn: writer, Addr: secondAddr}})
	waitForWrites(t, &writer.fakeWriter, 2)
	if writer.batchWriteCount() != 1 {
		t.Fatalf("batchWrites = %d, want 1", writer.batchWriteCount())
	}
	addrs := writer.writtenAddrs()
	if len(addrs) != 2 || addrs[0] != firstAddr.String() || addrs[1] != secondAddr.String() {
		t.Fatalf("written addrs = %#v", addrs)
	}
}

func TestDispatcherBatchesPayloadsAndTargetsSharingConnection(t *testing.T) {
	writer := &fakeBatchWriter{}
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 10}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 11), Port: 11}
	dispatcher := NewDispatcherWithBatch(10, 4, true, 8, nil)
	t.Cleanup(dispatcher.Close)

	dispatcher.FanoutBatch([][]byte{[]byte("first"), []byte("second")}, []Target{{ID: "first", Conn: writer, Addr: firstAddr}, {ID: "second", Conn: writer, Addr: secondAddr}})
	waitForWrites(t, &writer.fakeWriter, 4)
	if writer.batchWriteCount() != 1 {
		t.Fatalf("batchWrites = %d, want 1", writer.batchWriteCount())
	}
	addrs := writer.writtenAddrs()
	want := []string{firstAddr.String(), secondAddr.String(), firstAddr.String(), secondAddr.String()}
	if len(addrs) != len(want) {
		t.Fatalf("written addrs = %#v, want %#v", addrs, want)
	}
	for i := range want {
		if addrs[i] != want[i] {
			t.Fatalf("written addrs = %#v, want %#v", addrs, want)
		}
	}
}

func TestConnWorkerReportsPartialBatchWrite(t *testing.T) {
	writeErr := errors.New("retry")
	writer := &fakeBatchWriter{partial: 1}
	writer.writeErr = writeErr
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 12), Port: 12}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 13), Port: 13}
	errorsCh := make(chan Result, 1)
	dispatcher := NewDispatcherWithBatch(10, 4, true, 4, func(result Result) { errorsCh <- result })
	t.Cleanup(dispatcher.Close)

	dispatcher.Fanout([]byte("packet"), []Target{{ID: "first", Conn: writer, Addr: firstAddr}, {ID: "second", Conn: writer, Addr: secondAddr}})
	result := readResult(t, errorsCh)
	if result.ID != "second" || !errors.Is(result.Err, writeErr) {
		t.Fatalf("result = %#v, want second retry error", result)
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
