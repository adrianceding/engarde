package clientrole

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/serverrole"
)

func TestTCPClientBackoffRecoversWhenServerBecomesAvailable(t *testing.T) {
	retryTimers := installTCPManualSessionRetryTimer(t)
	target := startBehaviorEchoServer(t, 0, 0, nil, nil)
	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.DialTimeoutMillis = 50

	client := New(config.Client{
		ListenAddr: clientAddress,
		DstAddr:    serverAddress,
		Transfer:   transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }

	previousDial := dialTCPOnInterface
	var attemptsMu sync.Mutex
	attemptCount := 0
	attempts := make(chan int, 4)
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		attemptsMu.Lock()
		attemptCount++
		attempt := attemptCount
		attemptsMu.Unlock()
		attempts <- attempt
		dialer := net.Dialer{Timeout: timeout}
		return dialer.DialContext(ctx, "tcp4", destination)
	}

	ctx, cancel := context.WithCancel(context.Background())
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx) }()
	var serverDone chan error
	t.Cleanup(func() {
		cancel()
		behaviorWaitRun(t, "client", clientDone)
		if serverDone != nil {
			behaviorWaitRun(t, "server", serverDone)
		}
		dialTCPOnInterface = previousDial
	})
	waitForTCPListener(t, clientAddress)

	waitTCPDialAttempt(t, attempts, 1)
	firstRetry := retryTimers.next(t)
	if firstRetry.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("first retry delay = %v, want %v", firstRetry.delay, tcpSessionRetryInitialDelay)
	}
	assertTCPDialCount(t, &attemptsMu, &attemptCount, 1)
	retryTimers.assertNoPending(t)
	firstRetry.fire(t)
	waitTCPDialAttempt(t, attempts, 2)
	secondRetry := retryTimers.next(t)
	if secondRetry.delay != 2*tcpSessionRetryInitialDelay {
		t.Fatalf("second retry delay = %v, want %v", secondRetry.delay, 2*tcpSessionRetryInitialDelay)
	}
	assertTCPDialCount(t, &attemptsMu, &attemptCount, 2)
	retryTimers.assertNoPending(t)

	server := serverrole.New(config.Server{
		ListenAddr:                    serverAddress,
		AllowUnsafeDynamicDestination: true,
		Transfer:                      transfer,
	}, "test", nil)
	serverDone = make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	waitForTCPListener(t, serverAddress)
	secondRetry.fire(t)
	waitTCPDialAttempt(t, attempts, 3)
	behaviorEventually(t, 5*time.Second, "session recovery after server startup", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return false
		}
		return clientValue.(control.ClientStatus).Sessions == 1 &&
			serverValue.(control.ServerStatus).Sessions == 1
	})
	retryTimers.assertNoPending(t)

	application, err := behaviorDialSOCKS5(clientAddress, target.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	payload := behaviorPayload(256*1024+37, 71)
	if err := behaviorWriteAll(application, payload); err != nil {
		t.Fatal(err)
	}
	if err := behaviorCloseWrite(application); err != nil {
		t.Fatal(err)
	}
	echo, err := io.ReadAll(application)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(echo, payload) {
		t.Fatalf("recovered SOCKS5 echo length/content = %d/%v, want %d/exact", len(echo), bytes.Equal(echo, payload), len(payload))
	}
	select {
	case result := <-target.results:
		if result.err != nil || !bytes.Equal(result.payload, payload) {
			t.Fatalf("recovered destination transfer = %d bytes/error %v, want %d bytes/nil", len(result.payload), result.err, len(payload))
		}
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("recovered destination did not observe EOF")
	}
	assertTCPDialCount(t, &attemptsMu, &attemptCount, 3)
}

func waitTCPDialAttempt(t *testing.T, attempts <-chan int, want int) {
	t.Helper()
	select {
	case got := <-attempts:
		if got != want {
			t.Fatalf("dial attempt = %d, want %d", got, want)
		}
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatalf("dial attempt %d did not start", want)
	}
}

func assertTCPDialCount(t *testing.T, mu *sync.Mutex, count *int, want int) {
	t.Helper()
	mu.Lock()
	got := *count
	mu.Unlock()
	if got != want {
		t.Fatalf("physical dials = %d, want %d", got, want)
	}
}

func TestTCPSessionDisconnectReconnectsAndCarriesSOCKS5Stream(t *testing.T) {
	retryTimers := installTCPManualSessionRetryTimer(t)
	target := startBehaviorEchoServer(t, 0, 0, nil, nil)
	serverAddress := freeTCPAddress(t)
	clientAddress := freeTCPAddress(t)
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.DialTimeoutMillis = 500
	transfer.TCP.OpenTimeoutMillis = 2_000

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
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "127.0.0.1" }

	previousDial := dialTCPOnInterface
	firstLinkReady := make(chan *sessionFaultLink, 1)
	secondDialStarted := make(chan struct{})
	allowSecondDial := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	clientDone := make(chan error, 1)
	serverStarted := false
	clientStarted := false
	var firstLink *sessionFaultLink
	t.Cleanup(func() {
		cancel()
		if clientStarted {
			behaviorWaitRun(t, "client", clientDone)
		}
		if serverStarted {
			behaviorWaitRun(t, "server", serverDone)
		}
		dialTCPOnInterface = previousDial
		if firstLink == nil {
			select {
			case firstLink = <-firstLinkReady:
			default:
			}
		}
		if firstLink != nil {
			firstLink.Close()
			select {
			case <-firstLink.Done():
			case <-time.After(time.Second):
				t.Error("fault link did not stop")
			}
		}
	})
	var dialMu sync.Mutex
	dialCount := 0
	dialTCPOnInterface = func(ctx context.Context, destination, _, _ string, timeout time.Duration) (net.Conn, error) {
		dialMu.Lock()
		dialCount++
		attempt := dialCount
		dialMu.Unlock()
		if attempt == 2 {
			close(secondDialStarted)
			select {
			case <-allowSecondDial:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		dialer := net.Dialer{Timeout: timeout}
		upstream, err := dialer.DialContext(ctx, "tcp4", destination)
		if err != nil {
			return nil, err
		}
		if attempt != 1 {
			return upstream, nil
		}
		endpoint, peer := net.Pipe()
		link := newSessionFaultLink(peer, upstream)
		firstLinkReady <- link
		return endpoint, nil
	}

	go func() { serverDone <- server.Run(ctx) }()
	serverStarted = true
	waitForTCPListener(t, serverAddress)
	go func() { clientDone <- client.Run(ctx) }()
	clientStarted = true
	waitForTCPListener(t, clientAddress)
	stack := &behaviorStack{
		client:        client,
		server:        server,
		clientAddress: clientAddress,
		cancel:        cancel,
		clientDone:    clientDone,
		serverDone:    serverDone,
	}

	behaviorEventually(t, 5*time.Second, "initial session", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		return clientErr == nil && serverErr == nil &&
			clientValue.(control.ClientStatus).Sessions == 1 &&
			serverValue.(control.ServerStatus).Sessions == 1
	})
	select {
	case firstLink = <-firstLinkReady:
	case <-time.After(time.Second):
		t.Fatal("initial session fault link was not created")
	}

	runtime := client.getTCPRuntime()
	runtime.mu.Lock()
	pathSession := runtime.sessions["path-a"]
	path := runtime.paths["path-a"]
	runtime.mu.Unlock()
	if pathSession == nil {
		t.Fatal("client has no path session")
	}
	initialSession, initialGeneration, healthy := pathSession.current(path)
	if !healthy {
		t.Fatal("path-a has no negotiated session")
	}

	firstLink.Close()
	select {
	case <-firstLink.Done():
	case <-time.After(time.Second):
		t.Fatal("peer-side session disconnect did not stop the fault link")
	}
	select {
	case <-initialSession.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not close the old session")
	}
	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("replacement retry delay = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	pathSession.mu.Lock()
	oldSession := pathSession.session
	retrying := pathSession.retrying
	retryCount := pathSession.retryCount
	generationBeforeTick := pathSession.generation
	pathSession.mu.Unlock()
	if oldSession != nil || !retrying || retryCount != 1 || generationBeforeTick != initialGeneration {
		t.Fatalf("pre-retry state = session %v/retrying %v/count %d/generation %d, want nil/true/1/%d", oldSession != nil, retrying, retryCount, generationBeforeTick, initialGeneration)
	}
	retryTimers.assertNoPending(t)
	retryTimer.fire(t)
	select {
	case <-secondDialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not schedule a replacement session")
	}
	pathSession.mu.Lock()
	replacementInFlight := pathSession.inFlight
	replacementGeneration := pathSession.generation
	pathSession.mu.Unlock()
	if !replacementInFlight || replacementGeneration != initialGeneration+1 {
		t.Fatalf("replacement state = in-flight %v/generation %d, want true/%d", replacementInFlight, replacementGeneration, initialGeneration+1)
	}

	close(allowSecondDial)
	behaviorEventually(t, 5*time.Second, "replacement session", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil ||
			clientValue.(control.ClientStatus).Sessions != 1 ||
			serverValue.(control.ServerStatus).Sessions != 1 {
			return false
		}
		current, generation, currentHealthy := pathSession.current(path)
		return currentHealthy && current != initialSession && generation == initialGeneration+1
	})
	dialMu.Lock()
	completedDialCount := dialCount
	dialMu.Unlock()
	if completedDialCount != 2 {
		t.Fatalf("physical dials after replacement = %d, want 2", completedDialCount)
	}
	retryTimers.assertNoPending(t)

	application, err := behaviorDialSOCKS5(clientAddress, target.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	behaviorWaitStackCounts(t, stack, 1, 1, 1, 1)
	payload := behaviorPayload(128*1024+43, 83)
	if err := behaviorWriteAll(application, payload); err != nil {
		t.Fatal(err)
	}
	if err := behaviorCloseWrite(application); err != nil {
		t.Fatal(err)
	}
	echo, err := io.ReadAll(application)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(echo, payload) {
		t.Fatalf("replacement session SOCKS5 echo length/content = %d/%v, want %d/exact", len(echo), bytes.Equal(echo, payload), len(payload))
	}
	select {
	case result := <-target.results:
		if result.err != nil || !bytes.Equal(result.payload, payload) {
			t.Fatalf("replacement session destination transfer = %d bytes/error %v, want %d bytes/nil", len(result.payload), result.err, len(payload))
		}
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("replacement session destination did not observe EOF")
	}
	behaviorWaitStackCounts(t, stack, 0, 0, 0, 0)
	behaviorEventually(t, 5*time.Second, "post-transfer session", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		return clientErr == nil && serverErr == nil &&
			clientValue.(control.ClientStatus).Sessions == 1 &&
			serverValue.(control.ServerStatus).Sessions == 1
	})
}

func TestTCPSOCKS5ShutdownReleasesConcurrentConnections(t *testing.T) {
	gate := make(chan struct{})
	target := startBehaviorEchoServer(t, 0, 0, gate, nil)
	stack, stop := startStoppableFaultStack(t, 2)

	const connectionCount = 8
	connections := make([]net.Conn, 0, connectionCount)
	for range connectionCount {
		conn, err := behaviorDialSOCKS5(stack.clientAddress, target.listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	behaviorWaitStackCounts(t, stack, connectionCount, 2*connectionCount, connectionCount, 2*connectionCount)

	stop()
	for index, conn := range connections {
		if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Errorf("connection %d deadline: %v", index, err)
			continue
		}
		if read, err := conn.Read(make([]byte, 1)); read != 0 || err == nil {
			t.Errorf("connection %d after shutdown = %d/%v, want 0/error", index, read, err)
		}
		_ = conn.Close()
	}
	close(gate)
	target.stop(t)

	clientRuntime := stack.client.getTCPRuntime()
	clientRuntime.mu.Lock()
	closing := clientRuntime.closing
	accepted := len(clientRuntime.accepted)
	flows := len(clientRuntime.flows)
	carriers := len(clientRuntime.carriers)
	groups := len(clientRuntime.groups)
	sessions := len(clientRuntime.sessions)
	clientRuntime.mu.Unlock()
	if !closing || accepted != 0 || flows != 0 || carriers != 0 || groups != 0 || sessions != 0 {
		t.Fatalf("client shutdown state = closing %v/accepted %d/flows %d/carrier maps %d/groups %d/sessions %d", closing, accepted, flows, carriers, groups, sessions)
	}
	target.mu.Lock()
	targetConnections := len(target.connections)
	target.mu.Unlock()
	if targetConnections != 0 {
		t.Fatalf("destination retained %d connections after shutdown", targetConnections)
	}
}

func TestTCPSOCKS5ProductionSoak(t *testing.T) {
	durationText := os.Getenv("ENGARDE_SOAK_DURATION")
	if durationText == "" {
		t.Skip("set ENGARDE_SOAK_DURATION (for example 30s) to run the production soak")
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid ENGARDE_SOAK_DURATION %q", durationText)
	}

	target := startBehaviorEchoServer(t, 0, 25*time.Microsecond, nil, nil)
	faultGate := make(chan struct{})
	faultTarget := startBehaviorEchoServerWithoutTimeout(t, faultGate)
	stack := startBehaviorStack(t, 2, func(transfer *config.Transfer) {
		transfer.TCP.ChunkSize = 4 * 1024
		transfer.TCP.CarrierQueueBytes = 256 * 1024
		transfer.TCP.ReorderWindowBytes = 2 * 1024 * 1024
	})
	sentinel, err := behaviorDialSOCKS5(stack.clientAddress, faultTarget.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sentinel.Close()
	behaviorWaitStackCounts(t, stack, 1, 2, 1, 2)

	started := time.Now()
	deadline := started.Add(duration)
	batches := 0
	streams := 0
	bytesTransferred := int64(0)
	faults := 0
	for time.Now().Before(deadline) {
		batchBytes := runProductionSoakBatch(t, stack, target, batches)
		batches++
		streams += productionSoakConcurrency
		bytesTransferred += batchBytes
		if batches%5 == 0 {
			if err := behaviorCloseCarrier(stack.client, "path-b"); err != nil {
				t.Fatal(err)
			}
			faults++
			behaviorWaitStackCounts(t, stack, 1, 2, 1, 2)
		}
	}

	_ = sentinel.Close()
	close(faultGate)
	select {
	case result := <-faultTarget.results:
		if result.err != nil {
			t.Fatalf("soak sentinel destination: %v", result.err)
		}
	case <-time.After(behaviorSocketTimeout):
		t.Fatal("soak sentinel did not close")
	}
	behaviorWaitStackCounts(t, stack, 0, 0, 0, 0)
	elapsed := time.Since(started)
	mebibytes := float64(bytesTransferred) / (1024 * 1024)
	t.Logf("production soak: duration=%v batches=%d streams=%d carrier_faults=%d round_trip=%.2f MiB throughput=%.2f MiB/s", elapsed, batches, streams, faults, mebibytes, mebibytes/elapsed.Seconds())
}

const productionSoakConcurrency = 8

type sessionFaultLink struct {
	peer      net.Conn
	upstream  net.Conn
	closeOnce sync.Once
	done      chan struct{}
}

func newSessionFaultLink(peer, upstream net.Conn) *sessionFaultLink {
	link := &sessionFaultLink{peer: peer, upstream: upstream, done: make(chan struct{})}
	var pumps sync.WaitGroup
	pumps.Add(2)
	pump := func(destination, source net.Conn) {
		defer pumps.Done()
		_, _ = io.Copy(destination, source)
		link.Close()
	}
	go pump(upstream, peer)
	go pump(peer, upstream)
	go func() {
		pumps.Wait()
		close(link.done)
	}()
	return link
}

func (link *sessionFaultLink) Close() {
	link.closeOnce.Do(func() {
		_ = link.peer.Close()
		_ = link.upstream.Close()
	})
}

func (link *sessionFaultLink) Done() <-chan struct{} {
	return link.done
}

type productionSoakResult struct {
	index int
	err   error
}

func runProductionSoakBatch(t *testing.T, stack *behaviorStack, target *behaviorEchoServer, batch int) int64 {
	t.Helper()
	payloads := make([][]byte, productionSoakConcurrency)
	results := make(chan productionSoakResult, productionSoakConcurrency)
	start := make(chan struct{})
	var total int64
	for index := range productionSoakConcurrency {
		payloads[index] = behaviorPayload(64*1024+index*113, batch*productionSoakConcurrency+index+1)
		total += int64(len(payloads[index]))
		go func(index int) {
			<-start
			conn, err := behaviorDialSOCKS5(stack.clientAddress, target.listener.Addr().String())
			if err == nil {
				err = behaviorWriteAll(conn, payloads[index])
			}
			if err == nil {
				err = behaviorCloseWrite(conn)
			}
			var echo []byte
			if err == nil {
				echo, err = io.ReadAll(conn)
			}
			if conn != nil {
				_ = conn.Close()
			}
			if err == nil && !bytes.Equal(echo, payloads[index]) {
				err = fmt.Errorf("echo length/content = %d/%v, want %d/exact", len(echo), bytes.Equal(echo, payloads[index]), len(payloads[index]))
			}
			results <- productionSoakResult{index: index, err: err}
		}(index)
	}
	close(start)
	var firstErr error
	for range productionSoakConcurrency {
		select {
		case result := <-results:
			if result.err != nil && firstErr == nil {
				firstErr = fmt.Errorf("stream %d: %w", result.index, result.err)
			}
		case <-time.After(behaviorSocketTimeout):
			t.Fatal("soak batch clients did not finish")
		}
	}
	if firstErr != nil {
		t.Fatal(firstErr)
	}

	matched := make([]bool, productionSoakConcurrency)
	for range productionSoakConcurrency {
		select {
		case result := <-target.results:
			if result.err != nil {
				t.Fatalf("soak destination: %v", result.err)
			}
			match := -1
			for index, payload := range payloads {
				if !matched[index] && bytes.Equal(result.payload, payload) {
					match = index
					break
				}
			}
			if match < 0 {
				t.Fatalf("soak destination received unknown or duplicate payload of %d bytes", len(result.payload))
			}
			matched[match] = true
		case <-time.After(behaviorSocketTimeout):
			t.Fatal("soak destination streams did not finish")
		}
	}
	behaviorWaitStackCounts(t, stack, 1, 2, 1, 2)
	return total * 2
}

func startStoppableFaultStack(t *testing.T, pathCount int) (*behaviorStack, func()) {
	t.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
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
		cancel:        cancel,
		clientDone:    clientDone,
		serverDone:    serverDone,
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			behaviorWaitRun(t, "client", clientDone)
			behaviorWaitRun(t, "server", serverDone)
			dialTCPOnInterface = previousDial
		})
	}
	t.Cleanup(stop)
	behaviorEventually(t, 10*time.Second, "path sessions", func() bool {
		clientValue, clientErr := client.Status()
		serverValue, serverErr := server.Status()
		if clientErr != nil || serverErr != nil {
			return false
		}
		return clientValue.(control.ClientStatus).Sessions == pathCount &&
			serverValue.(control.ServerStatus).Sessions == pathCount
	})
	return stack, stop
}
