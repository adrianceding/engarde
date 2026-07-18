package clientrole

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/serverrole"
	"github.com/adrianceding/engarde/internal/socks5"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

const (
	tcpFullStackBenchmarkPayloadSize = 8 * 1024 * 1024
	tcpFullStackBenchmarkTimeout     = 30 * time.Second
)

func BenchmarkTCPSOCKS5FullStackWarmConnect(b *testing.B) {
	target := newTCPFullStackBenchmarkEcho(b)
	stack := newTCPFullStackBenchmark(b, 1)
	request, err := tcpFullStackBenchmarkConnectRequest(target.listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for iteration := range b.N {
		stack.waitIdle(b, target)
		b.StartTimer()
		conn, dialErr := tcpFullStackBenchmarkDialSOCKS5(stack.clientAddress, request)
		b.StopTimer()
		if dialErr != nil {
			b.Fatal(dialErr)
		}
		target.waitAccepted(b, int64(iteration+1))
		if closeErr := conn.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
	}
	stack.waitIdle(b, target)
	if got := target.accepted.Load(); got != int64(b.N) {
		b.Fatalf("destination accepts = %d, want %d", got, b.N)
	}
	target.failOnError(b)
}

func BenchmarkTCPSOCKS5FullStackTransfer8MiB(b *testing.B) {
	for _, pathCount := range []int{1, 2} {
		b.Run(fmt.Sprintf("paths=%d", pathCount), func(b *testing.B) {
			benchmarkTCPSOCKS5FullStackTransfer(b, pathCount, 0)
		})
	}
}

func BenchmarkTCPSOCKS5FullStackTransferChunkSize(b *testing.B) {
	for _, chunkSize := range []int{16 * 1024, 32 * 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("chunk=%dKiB", chunkSize/1024), func(b *testing.B) {
			benchmarkTCPSOCKS5FullStackTransfer(b, 1, chunkSize)
		})
	}
}

func benchmarkTCPSOCKS5FullStackTransfer(b *testing.B, pathCount, chunkSize int) {
	target := newTCPFullStackBenchmarkEcho(b)
	stack := newTCPFullStackBenchmark(b, pathCount, chunkSize)
	request, err := tcpFullStackBenchmarkConnectRequest(target.listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	conn, err := tcpFullStackBenchmarkDialSOCKS5(stack.clientAddress, request)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	stack.waitActive(b, pathCount)
	target.waitAccepted(b, 1)
	target.waitReady(b, 1)

	payload := make([]byte, tcpFullStackBenchmarkPayloadSize)
	for index := range payload {
		payload[index] = byte(index*31 + 17)
	}
	received := make([]byte, len(payload))
	readerStart := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		<-readerStart
		for iteration := range b.N {
			if _, readErr := io.ReadFull(conn, received); readErr != nil {
				readerDone <- fmt.Errorf("iteration %d: read echo: %w", iteration, readErr)
				return
			}
			if !bytes.Equal(received, payload) {
				readerDone <- fmt.Errorf("iteration %d: echoed payload differs", iteration)
				return
			}
		}
		readerDone <- nil
	}()

	b.SetBytes(tcpFullStackBenchmarkPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	close(readerStart)
	var writeErr error
	for iteration := range b.N {
		if writeErr = tcpFullStackBenchmarkWriteAll(conn, payload); writeErr != nil {
			writeErr = fmt.Errorf("iteration %d: write payload: %w", iteration, writeErr)
			_ = conn.Close()
			break
		}
	}
	readErr := <-readerDone
	b.StopTimer()
	if writeErr != nil {
		b.Fatal(writeErr)
	}
	if readErr != nil {
		b.Fatal(readErr)
	}

	if closeWriter, ok := conn.(interface{ CloseWrite() error }); !ok {
		b.Fatal("SOCKS5 connection does not support CloseWrite")
	} else if err := closeWriter.CloseWrite(); err != nil {
		b.Fatal(err)
	}
	target.waitIdle(b)
	wantBytes := int64(b.N) * tcpFullStackBenchmarkPayloadSize
	if got := target.readBytes.Load(); got != wantBytes {
		b.Fatalf("destination read bytes = %d, want %d", got, wantBytes)
	}
	if got := target.writtenBytes.Load(); got != wantBytes {
		b.Fatalf("destination written bytes = %d, want %d", got, wantBytes)
	}
	if got := target.accepted.Load(); got != 1 {
		b.Fatalf("destination accepts = %d, want 1", got)
	}
	target.failOnError(b)
}

type tcpFullStackBenchmark struct {
	client        *Client
	server        *serverrole.Server
	clientAddress string
	pathCount     int
	cancel        context.CancelFunc
	clientRun     *tcpFullStackBenchmarkRun
	serverRun     *tcpFullStackBenchmarkRun
}

func newTCPFullStackBenchmark(b *testing.B, pathCount int, chunkSizes ...int) *tcpFullStackBenchmark {
	b.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	if len(chunkSizes) > 0 && chunkSizes[0] > 0 {
		transfer.TCP.ChunkSize = chunkSizes[0]
	}
	serverAddress := freeTCPAddress(b)
	clientAddress := freeTCPAddress(b)
	server := serverrole.New(config.Server{
		ListenAddr:                    serverAddress,
		AllowUnsafeDynamicDestination: true,
		Transfer:                      transfer,
	}, "benchmark", nil)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		Transfer:   transfer,
	}, "benchmark", nil)
	interfaces := make([]net.Interface, pathCount)
	for index := range pathCount {
		interfaces[index] = net.Interface{Index: index + 1, Name: fmt.Sprintf("benchmark-path-%d", index+1)}
	}
	client.listInterfaces = func() ([]net.Interface, error) {
		return append([]net.Interface(nil), interfaces...), nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }

	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stack := &tcpFullStackBenchmark{
		client:        client,
		server:        server,
		clientAddress: clientAddress,
		pathCount:     pathCount,
		cancel:        cancel,
		clientRun:     newTCPFullStackBenchmarkRun(),
		serverRun:     newTCPFullStackBenchmarkRun(),
	}
	b.Cleanup(func() {
		stack.cancel()
		stack.clientRun.wait(b, "client")
		stack.serverRun.wait(b, "server")
		dialTCPOnInterface = previousDial
	})
	go stack.serverRun.run(func() error { return server.Run(ctx) })
	tcpFullStackBenchmarkWaitListener(b, serverAddress, stack.serverRun)
	go stack.clientRun.run(func() error { return client.Run(ctx) })
	tcpFullStackBenchmarkWaitListener(b, clientAddress, stack.clientRun)
	stack.waitIdle(b, nil)
	return stack
}

func (stack *tcpFullStackBenchmark) waitIdle(b *testing.B, target *tcpFullStackBenchmarkEcho) {
	b.Helper()
	tcpFullStackBenchmarkEventually(b, "idle stack with warm sessions", func() bool {
		clientStatus, serverStatus, ok := stack.status()
		if !ok || clientStatus.Streams != 0 || clientStatus.Carriers != 0 ||
			serverStatus.Streams != 0 || serverStatus.Carriers != 0 ||
			clientStatus.Sessions != stack.pathCount || serverStatus.Sessions != stack.pathCount {
			return false
		}
		return target == nil || target.active.Load() == 0
	})
}

func (stack *tcpFullStackBenchmark) waitActive(b *testing.B, carrierCount int) {
	b.Helper()
	tcpFullStackBenchmarkEventually(b, "active stream carriers", func() bool {
		clientStatus, serverStatus, ok := stack.status()
		return ok && clientStatus.Streams == 1 && clientStatus.Carriers == carrierCount &&
			serverStatus.Streams == 1 && serverStatus.Carriers == carrierCount
	})
}

func (stack *tcpFullStackBenchmark) status() (control.ClientStatus, control.ServerStatus, bool) {
	clientValue, clientErr := stack.client.Status()
	serverValue, serverErr := stack.server.Status()
	if clientErr != nil || serverErr != nil {
		return control.ClientStatus{}, control.ServerStatus{}, false
	}
	clientStatus, clientOK := clientValue.(control.ClientStatus)
	serverStatus, serverOK := serverValue.(control.ServerStatus)
	return clientStatus, serverStatus, clientOK && serverOK
}

type tcpFullStackBenchmarkRun struct {
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func newTCPFullStackBenchmarkRun() *tcpFullStackBenchmarkRun {
	return &tcpFullStackBenchmarkRun{done: make(chan struct{})}
}

func (run *tcpFullStackBenchmarkRun) run(worker func() error) {
	err := worker()
	run.mu.Lock()
	run.err = err
	run.mu.Unlock()
	close(run.done)
}

func (run *tcpFullStackBenchmarkRun) stopped() (bool, error) {
	select {
	case <-run.done:
		run.mu.Lock()
		defer run.mu.Unlock()
		return true, run.err
	default:
		return false, nil
	}
}

func (run *tcpFullStackBenchmarkRun) wait(b *testing.B, name string) {
	b.Helper()
	select {
	case <-run.done:
		run.mu.Lock()
		err := run.err
		run.mu.Unlock()
		if err != nil {
			b.Errorf("%s Run error: %v", name, err)
		}
	case <-time.After(tcpFullStackBenchmarkTimeout):
		b.Errorf("%s did not stop", name)
	}
}

type tcpFullStackBenchmarkEcho struct {
	listener     net.Listener
	accepted     atomic.Int64
	ready        atomic.Int64
	active       atomic.Int64
	readBytes    atomic.Int64
	writtenBytes atomic.Int64

	mu           sync.Mutex
	connections  map[net.Conn]struct{}
	connectionWG sync.WaitGroup
	acceptDone   chan struct{}
	errors       chan error
	stopOnce     sync.Once
	stopping     chan struct{}
}

func newTCPFullStackBenchmarkEcho(b *testing.B) *tcpFullStackBenchmarkEcho {
	b.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	target := &tcpFullStackBenchmarkEcho{
		listener:    listener,
		connections: make(map[net.Conn]struct{}),
		acceptDone:  make(chan struct{}),
		errors:      make(chan error, 1),
		stopping:    make(chan struct{}),
	}
	go target.acceptLoop()
	b.Cleanup(func() { target.stop(b) })
	return target
}

func (target *tcpFullStackBenchmarkEcho) acceptLoop() {
	defer close(target.acceptDone)
	for {
		conn, err := target.listener.Accept()
		if err != nil {
			select {
			case <-target.stopping:
			default:
				target.recordError(err)
			}
			return
		}
		target.accepted.Add(1)
		target.active.Add(1)
		target.mu.Lock()
		target.connections[conn] = struct{}{}
		target.connectionWG.Add(1)
		target.mu.Unlock()
		go target.echo(conn)
	}
}

func (target *tcpFullStackBenchmarkEcho) echo(conn net.Conn) {
	defer target.connectionWG.Done()
	defer func() {
		target.mu.Lock()
		delete(target.connections, conn)
		target.mu.Unlock()
		target.active.Add(-1)
		_ = conn.Close()
	}()
	if err := conn.SetDeadline(time.Now().Add(tcpFullStackBenchmarkTimeout)); err != nil {
		target.recordError(err)
		return
	}
	buffer := make([]byte, 64*1024)
	target.ready.Add(1)
	for {
		read, err := conn.Read(buffer)
		if read > 0 {
			target.readBytes.Add(int64(read))
			if writeErr := tcpFullStackBenchmarkWriteAll(conn, buffer[:read]); writeErr != nil {
				target.recordError(writeErr)
				return
			}
			target.writtenBytes.Add(int64(read))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				select {
				case <-target.stopping:
				default:
					target.recordError(err)
				}
			}
			return
		}
	}
}

func (target *tcpFullStackBenchmarkEcho) waitIdle(b *testing.B) {
	b.Helper()
	tcpFullStackBenchmarkEventually(b, "destination connection cleanup", func() bool {
		return target.active.Load() == 0
	})
}

func (target *tcpFullStackBenchmarkEcho) waitAccepted(b *testing.B, count int64) {
	b.Helper()
	tcpFullStackBenchmarkEventually(b, "destination accept", func() bool {
		return target.accepted.Load() == count
	})
}

func (target *tcpFullStackBenchmarkEcho) waitReady(b *testing.B, count int64) {
	b.Helper()
	tcpFullStackBenchmarkEventually(b, "destination handler readiness", func() bool {
		return target.ready.Load() == count
	})
}

func (target *tcpFullStackBenchmarkEcho) failOnError(b *testing.B) {
	b.Helper()
	select {
	case err := <-target.errors:
		b.Fatalf("destination server: %v", err)
	default:
	}
}

func (target *tcpFullStackBenchmarkEcho) recordError(err error) {
	select {
	case target.errors <- err:
	default:
	}
}

func (target *tcpFullStackBenchmarkEcho) stop(b *testing.B) {
	b.Helper()
	target.stopOnce.Do(func() {
		close(target.stopping)
		_ = target.listener.Close()
		select {
		case <-target.acceptDone:
		case <-time.After(tcpFullStackBenchmarkTimeout):
			b.Error("destination listener did not stop")
		}
		target.mu.Lock()
		connections := make([]net.Conn, 0, len(target.connections))
		for conn := range target.connections {
			connections = append(connections, conn)
		}
		target.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		done := make(chan struct{})
		go func() {
			target.connectionWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(tcpFullStackBenchmarkTimeout):
			b.Error("destination connections did not stop")
		}
		target.failOnError(b)
	})
}

func tcpFullStackBenchmarkDialSOCKS5(proxyAddress string, request []byte) (net.Conn, error) {
	dialer := net.Dialer{Timeout: tcpFullStackBenchmarkTimeout}
	conn, err := dialer.DialContext(context.Background(), "tcp4", proxyAddress)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(tcpFullStackBenchmarkTimeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := tcpFullStackBenchmarkWriteAll(conn, []byte{5, 1, 0}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var methodReply [2]byte
	if _, err := io.ReadFull(conn, methodReply[:]); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if methodReply != [2]byte{5, 0} {
		_ = conn.Close()
		return nil, fmt.Errorf("SOCKS5 method reply = %v", methodReply)
	}
	if err := tcpFullStackBenchmarkWriteAll(conn, request); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reply, err := tcpFullStackBenchmarkReadSOCKS5Reply(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if reply != socks5.ReplySucceeded {
		_ = conn.Close()
		return nil, fmt.Errorf("SOCKS5 CONNECT reply = %d", reply)
	}
	return conn, nil
}

func tcpFullStackBenchmarkConnectRequest(address string) ([]byte, error) {
	destination, err := tcpstream.ParseDestination(address)
	if err != nil {
		return nil, err
	}
	payload, err := destination.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte{5, 1, 0}, payload...), nil
}

func tcpFullStackBenchmarkReadSOCKS5Reply(conn net.Conn) (socks5.Reply, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, err
	}
	if header[0] != 5 || header[2] != 0 {
		return 0, fmt.Errorf("invalid SOCKS5 reply header %v", header)
	}
	addressLength := 0
	switch header[3] {
	case 1:
		addressLength = net.IPv4len
	case 4:
		addressLength = net.IPv6len
	case 3:
		var length [1]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return 0, err
		}
		addressLength = int(length[0])
	default:
		return 0, fmt.Errorf("invalid SOCKS5 reply address type %d", header[3])
	}
	var address [net.IPv6len]byte
	if _, err := io.ReadFull(conn, address[:addressLength]); err != nil {
		return 0, err
	}
	var port [2]byte
	if _, err := io.ReadFull(conn, port[:]); err != nil {
		return 0, err
	}
	return socks5.Reply(header[1]), nil
}

func tcpFullStackBenchmarkWriteAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if written > 0 {
			payload = payload[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func tcpFullStackBenchmarkWaitListener(b *testing.B, address string, run *tcpFullStackBenchmarkRun) {
	b.Helper()
	deadline := time.Now().Add(tcpFullStackBenchmarkTimeout)
	for time.Now().Before(deadline) {
		if stopped, err := run.stopped(); stopped {
			b.Fatalf("listener %s stopped during startup: %v", address, err)
		}
		conn, err := net.DialTimeout("tcp4", address, 20*time.Millisecond)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			_ = conn.Close()
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("listener %s did not start", address)
}

func tcpFullStackBenchmarkEventually(b *testing.B, description string, condition func() bool) {
	b.Helper()
	deadline := time.Now().Add(tcpFullStackBenchmarkTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		b.Fatalf("timed out waiting for %s", description)
	}
}
