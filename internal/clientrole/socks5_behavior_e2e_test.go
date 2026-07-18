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

const behaviorSocketTimeout = 30 * time.Second

type behaviorStack struct {
	client        *Client
	server        *serverrole.Server
	clientAddress string
	interfaces    *behaviorInterfaceSet
	cancel        context.CancelFunc
	clientDone    <-chan error
	serverDone    <-chan error
}

type behaviorInterfaceSet struct {
	mu         sync.Mutex
	interfaces []net.Interface
}

func (set *behaviorInterfaceSet) snapshot() []net.Interface {
	set.mu.Lock()
	defer set.mu.Unlock()
	return append([]net.Interface(nil), set.interfaces...)
}

func (set *behaviorInterfaceSet) remove(name string) bool {
	set.mu.Lock()
	defer set.mu.Unlock()
	for index, iface := range set.interfaces {
		if iface.Name != name {
			continue
		}
		set.interfaces = append(set.interfaces[:index], set.interfaces[index+1:]...)
		return true
	}
	return false
}

type behaviorEchoResult struct {
	payload []byte
	err     error
}

type behaviorEchoServer struct {
	listener       net.Listener
	progress       chan int
	results        chan behaviorEchoResult
	accepted       atomic.Int32
	acceptedEvents chan struct{}

	progressAt      int
	throttle        time.Duration
	socketTimeout   time.Duration
	gate            <-chan struct{}
	progressRelease <-chan struct{}
	progressed      sync.Once

	mu           sync.Mutex
	connections  map[net.Conn]struct{}
	acceptDone   chan struct{}
	connectionWG sync.WaitGroup
	stopOnce     sync.Once
	stopping     chan struct{}
}

func TestTCPSOCKS5PipelinedLargeTransferSurvivesCarrierLoss(t *testing.T) {
	resumeDestination := make(chan struct{})
	const faultAfterBytes = 128 * 1024
	target := startBehaviorEchoServer(t, faultAfterBytes, 0, nil, resumeDestination)
	stack := startBehaviorStack(t, 2, func(transfer *config.Transfer) {
		transfer.TCP.ChunkSize = 1024
		transfer.TCP.CarrierQueueBytes = 64 * 1024
		transfer.TCP.ReorderWindowBytes = 2 * 1024 * 1024
	})

	application, err := behaviorDialTCP(stack.clientAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	if err := application.SetDeadline(time.Now().Add(behaviorSocketTimeout)); err != nil {
		t.Fatal(err)
	}
	if err := behaviorNegotiateNoAuth(application); err != nil {
		t.Fatal(err)
	}

	payload := behaviorPayload(1<<20+65537, 17)
	const earlyBytes = 32 * 1024
	request, err := behaviorSOCKS5ConnectRequest(target.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	pipelined := make([]byte, 0, len(request)+earlyBytes)
	pipelined = append(pipelined, request...)
	pipelined = append(pipelined, payload[:earlyBytes]...)
	if err := behaviorWriteAll(application, pipelined); err != nil {
		t.Fatal(err)
	}
	reply, err := behaviorReadSOCKS5Reply(application)
	if err != nil {
		t.Fatal(err)
	}
	if reply != socks5.ReplySucceeded {
		t.Fatalf("SOCKS5 CONNECT reply = %d, want success", reply)
	}
	behaviorWaitStackCounts(t, stack, 1, 2, 1, 2)

	readDone := make(chan behaviorEchoResult, 1)
	go func() {
		got, readErr := io.ReadAll(application)
		readDone <- behaviorEchoResult{payload: got, err: readErr}
	}()
	if err := behaviorWriteAll(application, payload[earlyBytes:faultAfterBytes]); err != nil {
		t.Fatalf("application prefix write: %v", err)
	}

	select {
	case received := <-target.progress:
		if received != faultAfterBytes {
			t.Fatalf("destination progress = %d bytes, want exactly %d", received, faultAfterBytes)
		}
	case result := <-target.results:
		t.Fatalf("destination ended before carrier fault injection: %v", result.err)
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("destination did not receive the transfer prefix")
	}
	if err := behaviorDropPath(stack, "path-b"); err != nil {
		t.Fatal(err)
	}
	close(resumeDestination)
	if err := behaviorWriteAll(application, payload[faultAfterBytes:]); err != nil {
		t.Fatalf("application post-fault write: %v", err)
	}
	if err := behaviorCloseWrite(application); err != nil {
		t.Fatalf("application CloseWrite: %v", err)
	}
	var applicationResult behaviorEchoResult
	select {
	case applicationResult = <-readDone:
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("application did not receive EOF")
	}
	if applicationResult.err != nil {
		t.Fatalf("application read: %v", applicationResult.err)
	}
	if !bytes.Equal(applicationResult.payload, payload) {
		t.Fatalf("application echo length/content = %d/%v, want %d/exact", len(applicationResult.payload), bytes.Equal(applicationResult.payload, payload), len(payload))
	}

	var destinationResult behaviorEchoResult
	select {
	case destinationResult = <-target.results:
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("destination did not observe EOF")
	}
	if destinationResult.err != nil {
		t.Fatalf("destination transfer: %v", destinationResult.err)
	}
	if !bytes.Equal(destinationResult.payload, payload) {
		t.Fatalf("destination payload length/content = %d/%v, want %d/exact", len(destinationResult.payload), bytes.Equal(destinationResult.payload, payload), len(payload))
	}
	behaviorWaitStackCounts(t, stack, 0, 0, 0, 0)
	if got := target.accepted.Load(); got != 1 {
		t.Fatalf("destination accepts = %d, want exactly one logical connection", got)
	}
}

func TestTCPSOCKS5HalfCloseDrainsResponseAndEOF(t *testing.T) {
	response := behaviorPayload(192*1024+31, 29)
	target, exchange := startBehaviorHalfCloseServer(t, response)
	stack := startBehaviorStack(t, 2, nil)
	application, err := behaviorDialSOCKS5(stack.clientAddress, target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()

	request := behaviorPayload(64*1024+19, 7)
	if err := behaviorWriteAll(application, request); err != nil {
		t.Fatal(err)
	}
	if err := behaviorCloseWrite(application); err != nil {
		t.Fatal(err)
	}
	gotResponse, err := io.ReadAll(application)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotResponse, response) {
		t.Fatalf("response length/content = %d/%v, want %d/exact", len(gotResponse), bytes.Equal(gotResponse, response), len(response))
	}

	select {
	case result := <-exchange:
		if result.err != nil {
			t.Fatalf("destination exchange: %v", result.err)
		}
		if !bytes.Equal(result.payload, request) {
			t.Fatalf("destination request length/content = %d/%v, want %d/exact", len(result.payload), bytes.Equal(result.payload, request), len(request))
		}
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("destination did not receive application EOF")
	}
	behaviorWaitStackCounts(t, stack, 0, 0, 0, 0)
}

func TestTCPSOCKS5ConcurrentStreamsRemainIsolated(t *testing.T) {
	const streamCount = 12
	gate := make(chan struct{})
	target := startBehaviorEchoServer(t, 0, 0, gate, nil)
	stack := startBehaviorStack(t, 2, nil)

	type streamResult struct {
		index int
		err   error
	}
	start := make(chan struct{})
	results := make(chan streamResult, streamCount)
	expected := make([][]byte, streamCount)
	for index := range streamCount {
		expected[index] = behaviorPayload(40*1024+index*37, 101+index)
		go func(index int) {
			<-start
			conn, err := behaviorDialSOCKS5(stack.clientAddress, target.listener.Addr().String())
			if err != nil {
				results <- streamResult{index: index, err: err}
				return
			}
			defer conn.Close()
			if err := behaviorWriteAll(conn, expected[index]); err != nil {
				results <- streamResult{index: index, err: err}
				return
			}
			if err := behaviorCloseWrite(conn); err != nil {
				results <- streamResult{index: index, err: err}
				return
			}
			got, err := io.ReadAll(conn)
			if err == nil && !bytes.Equal(got, expected[index]) {
				err = fmt.Errorf("echo length/content = %d/%v, want %d/exact", len(got), bytes.Equal(got, expected[index]), len(expected[index]))
			}
			results <- streamResult{index: index, err: err}
		}(index)
	}
	acceptedEvents := target.captureAcceptEvents(streamCount)
	close(start)
	behaviorWaitEvents(t, acceptedEvents, streamCount, "all destination connections")
	behaviorWaitStackCounts(t, stack, streamCount, -1, streamCount, -1)
	close(gate)

	for range streamCount {
		select {
		case result := <-results:
			if result.err != nil {
				t.Errorf("stream %d: %v", result.index, result.err)
			}
		case <-time.After(behaviorSocketTimeout):
			t.Fatal("concurrent SOCKS5 stream did not finish")
		}
	}

	matched := make([]bool, streamCount)
	for range streamCount {
		select {
		case result := <-target.results:
			if result.err != nil {
				t.Fatalf("destination stream: %v", result.err)
			}
			match := -1
			for index, want := range expected {
				if !matched[index] && bytes.Equal(result.payload, want) {
					match = index
					break
				}
			}
			if match < 0 {
				t.Fatalf("destination received an unknown or duplicate stream payload of %d bytes", len(result.payload))
			}
			matched[match] = true
		case <-time.After(behaviorSocketTimeout):
			t.Fatal("destination stream did not observe EOF")
		}
	}
	behaviorWaitStackCounts(t, stack, 0, 0, 0, 0)
	if got := target.accepted.Load(); got != streamCount {
		t.Fatalf("destination accepts = %d, want %d", got, streamCount)
	}
}

func TestTCPSOCKS5NoInterfacesReturnsGeneralFailureAndCloses(t *testing.T) {
	openTimers := installTCPManualFlowOpenTimer(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.OpenTimeoutMillis = 100
	clientAddress := freeTCPAddress(t)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    "127.0.0.1:1",
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, nil }
	client.interfaceAddress = func(net.Interface) string { return "" }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		behaviorWaitRun(t, "client", done)
	})
	waitForTCPListener(t, clientAddress)

	conn, err := behaviorDialTCP(clientAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := behaviorNegotiateNoAuth(conn); err != nil {
		t.Fatal(err)
	}
	request, err := behaviorSOCKS5ConnectRequest("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if err := behaviorWriteAll(conn, request); err != nil {
		t.Fatal(err)
	}
	type replyResult struct {
		reply socks5.Reply
		err   error
	}
	replyDone := make(chan replyResult, 1)
	go func() {
		reply, replyErr := behaviorReadSOCKS5Reply(conn)
		replyDone <- replyResult{reply: reply, err: replyErr}
	}()
	openTimer := openTimers.next(t)
	if openTimer.delay != 100*time.Millisecond {
		t.Fatalf("flow open timeout = %v, want 100ms", openTimer.delay)
	}
	select {
	case result := <-replyDone:
		t.Fatalf("SOCKS5 request completed before open timeout fired: reply %d/error %v", result.reply, result.err)
	default:
	}
	statusValue, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	status := statusValue.(control.ClientStatus)
	if status.Streams != 1 || status.Carriers != 0 || status.Sessions != 0 {
		t.Fatalf("pre-timeout status = streams %d/carriers %d/sessions %d, want 1/0/0", status.Streams, status.Carriers, status.Sessions)
	}
	openTimers.assertNoPending(t)
	openTimer.fire(t)

	var completed replyResult
	select {
	case completed = <-replyDone:
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("SOCKS5 request did not complete after open timeout fired")
	}
	if completed.err != nil {
		t.Fatal(completed.err)
	}
	if completed.reply != socks5.ReplyGeneralFailure {
		t.Fatalf("SOCKS5 CONNECT reply = %d, want general failure", completed.reply)
	}
	openTimers.assertNoPending(t)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	read, err := conn.Read(buffer)
	if read != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read after failure = %d/%v, want 0/EOF", read, err)
	}

	behaviorEventually(t, 5*time.Second, "failed SOCKS5 stream cleanup", func() bool {
		statusValue, statusErr := client.Status()
		if statusErr != nil {
			return false
		}
		status := statusValue.(control.ClientStatus)
		if status.Streams != 0 || status.Carriers != 0 || status.Sessions != 0 {
			return false
		}
		runtime := client.getTCPRuntime()
		if runtime == nil {
			return false
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.accepted) == 0 && len(runtime.flows) == 0 && len(runtime.carriers) == 0
	})
}

func startBehaviorStack(t *testing.T, pathCount int, configure func(*config.Transfer)) *behaviorStack {
	t.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	if configure != nil {
		configure(&transfer)
	}
	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	server := serverrole.New(config.Server{
		ListenAddr:                    serverAddress,
		AllowUnsafeDynamicDestination: true,
		Transfer:                      transfer,
	}, "test", nil)
	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		Transfer:   transfer,
	}, "test", nil)
	interfaces := make([]net.Interface, pathCount)
	for index := range pathCount {
		interfaces[index] = net.Interface{Index: index + 1, Name: fmt.Sprintf("path-%c", 'a'+index)}
	}
	interfaceSet := &behaviorInterfaceSet{interfaces: interfaces}
	client.listInterfaces = func() ([]net.Interface, error) { return interfaceSet.snapshot(), nil }
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }

	previousDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}
	t.Cleanup(func() { dialTCPOnInterface = previousDial })

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	waitForTCPListener(t, clientAddress)
	stack := &behaviorStack{
		client:        client,
		server:        server,
		clientAddress: clientAddress,
		interfaces:    interfaceSet,
		cancel:        cancel,
		clientDone:    clientDone,
		serverDone:    serverDone,
	}
	t.Cleanup(func() {
		stack.cancel()
		behaviorWaitRun(t, "client", stack.clientDone)
		behaviorWaitRun(t, "server", stack.serverDone)
	})
	behaviorEventually(t, 10*time.Second, "physical sessions", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return false
		}
		clientStatus := clientValue.(control.ClientStatus)
		serverStatus := serverValue.(control.ServerStatus)
		return clientStatus.Sessions == pathCount && serverStatus.Sessions == pathCount
	})
	return stack
}

func startBehaviorEchoServer(t *testing.T, progressAt int, throttle time.Duration, gate, progressRelease <-chan struct{}) *behaviorEchoServer {
	t.Helper()
	return startBehaviorEchoServerWithTimeout(t, progressAt, throttle, behaviorSocketTimeout, gate, progressRelease)
}

func startBehaviorEchoServerWithoutTimeout(t *testing.T, gate <-chan struct{}) *behaviorEchoServer {
	t.Helper()
	return startBehaviorEchoServerWithTimeout(t, 0, 0, 0, gate, nil)
}

func startBehaviorEchoServerWithTimeout(t *testing.T, progressAt int, throttle, socketTimeout time.Duration, gate, progressRelease <-chan struct{}) *behaviorEchoServer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &behaviorEchoServer{
		listener:        listener,
		progress:        make(chan int, 1),
		results:         make(chan behaviorEchoResult, 64),
		progressAt:      progressAt,
		throttle:        throttle,
		socketTimeout:   socketTimeout,
		gate:            gate,
		progressRelease: progressRelease,
		connections:     make(map[net.Conn]struct{}),
		acceptDone:      make(chan struct{}),
		stopping:        make(chan struct{}),
	}
	go server.acceptLoop()
	t.Cleanup(func() { server.stop(t) })
	return server
}

func (server *behaviorEchoServer) acceptLoop() {
	defer close(server.acceptDone)
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		server.accepted.Add(1)
		server.mu.Lock()
		server.connections[conn] = struct{}{}
		acceptedEvents := server.acceptedEvents
		server.connectionWG.Add(1)
		server.mu.Unlock()
		if acceptedEvents != nil {
			acceptedEvents <- struct{}{}
		}
		go server.echo(conn)
	}
}

func (server *behaviorEchoServer) captureAcceptEvents(count int) <-chan struct{} {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.accepted.Load() != 0 || server.acceptedEvents != nil {
		panic("accept event capture must be installed before the first connection")
	}
	server.acceptedEvents = make(chan struct{}, count)
	return server.acceptedEvents
}

func (server *behaviorEchoServer) echo(conn net.Conn) {
	defer server.connectionWG.Done()
	defer func() {
		server.mu.Lock()
		delete(server.connections, conn)
		server.mu.Unlock()
		_ = conn.Close()
	}()
	if server.socketTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(server.socketTimeout)); err != nil {
			server.results <- behaviorEchoResult{err: err}
			return
		}
	}
	if server.gate != nil {
		var timeout <-chan time.Time
		var timer *time.Timer
		if server.socketTimeout > 0 {
			timer = time.NewTimer(server.socketTimeout)
			timeout = timer.C
			defer timer.Stop()
		}
		select {
		case <-server.gate:
		case <-server.stopping:
			return
		case <-timeout:
			server.results <- behaviorEchoResult{err: errors.New("destination gate timed out")}
			return
		}
	}
	received := make([]byte, 0, 64*1024)
	buffer := make([]byte, 4096)
	for {
		read, err := conn.Read(buffer)
		if read > 0 {
			received = append(received, buffer[:read]...)
			pause := false
			if server.progressAt > 0 && len(received) >= server.progressAt {
				server.progressed.Do(func() {
					pause = true
					server.progress <- len(received)
				})
			}
			if pause && server.progressRelease != nil {
				select {
				case <-server.progressRelease:
				case <-server.stopping:
					return
				case <-time.After(5 * time.Second):
					server.results <- behaviorEchoResult{payload: received, err: errors.New("carrier fault injection timed out")}
					return
				}
			}
			if writeErr := behaviorWriteAll(conn, buffer[:read]); writeErr != nil {
				server.results <- behaviorEchoResult{payload: received, err: writeErr}
				return
			}
			if server.throttle > 0 {
				time.Sleep(server.throttle)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if closeWriter, ok := conn.(interface{ CloseWrite() error }); ok {
					if closeErr := closeWriter.CloseWrite(); closeErr != nil {
						server.results <- behaviorEchoResult{payload: received, err: closeErr}
						return
					}
				}
				server.results <- behaviorEchoResult{payload: received}
				return
			}
			server.results <- behaviorEchoResult{payload: received, err: err}
			return
		}
	}
}

func (server *behaviorEchoServer) stop(t *testing.T) {
	t.Helper()
	server.stopOnce.Do(func() {
		close(server.stopping)
		_ = server.listener.Close()
		<-server.acceptDone
		server.mu.Lock()
		connections := make([]net.Conn, 0, len(server.connections))
		for conn := range server.connections {
			connections = append(connections, conn)
		}
		server.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		done := make(chan struct{})
		go func() {
			server.connectionWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("destination connections did not stop")
		}
	})
}

func startBehaviorHalfCloseServer(t *testing.T, response []byte) (net.Listener, <-chan behaviorEchoResult) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan behaviorEchoResult, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			results <- behaviorEchoResult{err: acceptErr}
			return
		}
		defer conn.Close()
		if deadlineErr := conn.SetDeadline(time.Now().Add(behaviorSocketTimeout)); deadlineErr != nil {
			results <- behaviorEchoResult{err: deadlineErr}
			return
		}
		request, readErr := io.ReadAll(conn)
		if readErr == nil {
			readErr = behaviorWriteAll(conn, response)
		}
		if readErr == nil {
			if closeWriter, ok := conn.(interface{ CloseWrite() error }); ok {
				readErr = closeWriter.CloseWrite()
			}
		}
		results <- behaviorEchoResult{payload: request, err: readErr}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener, results
}

func behaviorDialSOCKS5(proxyAddress, destinationAddress string) (net.Conn, error) {
	conn, err := behaviorDialTCP(proxyAddress)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(behaviorSocketTimeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := behaviorNegotiateNoAuth(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	request, err := behaviorSOCKS5ConnectRequest(destinationAddress)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := behaviorWriteAll(conn, request); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reply, err := behaviorReadSOCKS5Reply(conn)
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

func behaviorDialTCP(address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	return dialer.DialContext(context.Background(), "tcp4", address)
}

func behaviorNegotiateNoAuth(conn net.Conn) error {
	if err := behaviorWriteAll(conn, []byte{5, 1, 0}); err != nil {
		return err
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if !bytes.Equal(reply, []byte{5, 0}) {
		return fmt.Errorf("SOCKS5 method reply = %v, want [5 0]", reply)
	}
	return nil
}

func behaviorSOCKS5ConnectRequest(address string) ([]byte, error) {
	destination, err := tcpstream.ParseDestination(address)
	if err != nil {
		return nil, err
	}
	payload, err := destination.Encode()
	if err != nil {
		return nil, err
	}
	request := []byte{5, 1, 0}
	return append(request, payload...), nil
}

func behaviorReadSOCKS5Reply(conn net.Conn) (socks5.Reply, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
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
		length := []byte{0}
		if _, err := io.ReadFull(conn, length); err != nil {
			return 0, err
		}
		addressLength = int(length[0])
	default:
		return 0, fmt.Errorf("invalid SOCKS5 reply address type %d", header[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, addressLength+2)); err != nil {
		return 0, err
	}
	return socks5.Reply(header[1]), nil
}

func behaviorWriteAll(writer io.Writer, payload []byte) error {
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

func behaviorCloseWrite(conn net.Conn) error {
	closeWriter, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		return errors.New("connection does not support CloseWrite")
	}
	return closeWriter.CloseWrite()
}

func behaviorPayload(size, seed int) []byte {
	payload := make([]byte, size)
	state := uint32(seed) + 1
	for index := range payload {
		state = state*1664525 + 1013904223
		payload[index] = byte(state >> 24)
	}
	return payload
}

func behaviorDropPath(stack *behaviorStack, interfaceName string) error {
	runtime := stack.client.getTCPRuntime()
	if runtime == nil {
		return errors.New("TCP runtime was not initialized")
	}
	runtime.mu.Lock()
	var carrier *tcpstream.Carrier
	var flow *tcpstream.Flow
	for streamID, carriers := range runtime.carriers {
		if current := carriers[interfaceName]; current != nil {
			if carrier != nil {
				runtime.mu.Unlock()
				return fmt.Errorf("multiple carriers registered for %s", interfaceName)
			}
			carrier = current
			flow = runtime.flows[streamID]
		}
	}
	runtime.mu.Unlock()
	if carrier == nil || flow == nil {
		return fmt.Errorf("carrier for %s was not registered", interfaceName)
	}
	if !stack.interfaces.remove(interfaceName) {
		return fmt.Errorf("interface %s was not configured", interfaceName)
	}

	runtime.refresh()
	select {
	case <-carrier.Detached():
	case <-time.After(behaviorSocketTimeout):
		return fmt.Errorf("carrier for %s did not detach after its path was removed", interfaceName)
	}
	if count := flow.CarrierCount(); count != 1 {
		return fmt.Errorf("flow carrier count after removing path %s = %d, want 1", interfaceName, count)
	}
	runtime.mu.Lock()
	_, pathExists := runtime.paths[interfaceName]
	_, sessionExists := runtime.sessions[interfaceName]
	runtime.mu.Unlock()
	if pathExists || sessionExists {
		return fmt.Errorf("removed path %s remained registered: path=%v session=%v", interfaceName, pathExists, sessionExists)
	}
	return nil
}

// behaviorCloseCarrier is retained for the opt-in soak diagnostic, where a
// virtual carrier is expected to be recreated on the same physical session.
func behaviorCloseCarrier(client *Client, interfaceName string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime := client.getTCPRuntime()
		if runtime != nil {
			runtime.mu.Lock()
			var carrier *tcpstream.Carrier
			for _, carriers := range runtime.carriers {
				if current := carriers[interfaceName]; current != nil {
					carrier = current
					break
				}
			}
			runtime.mu.Unlock()
			if carrier != nil {
				return carrier.Close()
			}
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("carrier for %s was not registered", interfaceName)
}

func behaviorWaitStackCounts(t *testing.T, stack *behaviorStack, clientStreams, clientCarriers, serverStreams, serverCarriers int) {
	t.Helper()
	behaviorEventually(t, 10*time.Second, "TCP stream/carrier counts", func() bool {
		clientValue, clientErr := stack.client.Status()
		serverValue, serverErr := stack.server.Status()
		if clientErr != nil || serverErr != nil {
			return false
		}
		clientStatus := clientValue.(control.ClientStatus)
		serverStatus := serverValue.(control.ServerStatus)
		return (clientStreams < 0 || clientStatus.Streams == clientStreams) &&
			(clientCarriers < 0 || clientStatus.Carriers == clientCarriers) &&
			(serverStreams < 0 || serverStatus.Streams == serverStreams) &&
			(serverCarriers < 0 || serverStatus.Carriers == serverCarriers)
	})
}

func behaviorWaitEvents(t *testing.T, events <-chan struct{}, count int, description string) {
	t.Helper()
	timer := time.NewTimer(behaviorSocketTimeout)
	defer timer.Stop()
	for index := 0; index < count; index++ {
		select {
		case <-events:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s: observed %d of %d events", description, index, count)
		}
	}
}

func behaviorEventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if condition() {
		return
	}
	t.Fatalf("timed out waiting for %s", description)
}

func behaviorWaitRun(t *testing.T, name string, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s Run error: %v", name, err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("%s did not stop", name)
	}
}
