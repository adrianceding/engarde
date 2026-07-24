package clientrole

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/stats"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

func TestTCPClientStatusReportsLastReceived(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{DstAddr: "198.51.100.1:59501", Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	client.interfaceAddress = func(net.Interface) string { return "192.0.2.10" }
	runtime := &tcpClientRuntime{
		client:   client,
		ctx:      context.Background(),
		flows:    make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:    map[string]tcpClientPath{"path-a": {index: 1, address: "192.0.2.10", destination: client.cfg.DstAddr}},
		carriers: make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:  make(map[string]*stats.Traffic),
	}
	client.setTCPRuntime(runtime)

	status := tcpClientStatus(t, client)
	if status.Interfaces[0].Last != nil {
		t.Fatalf("last received = %v, want nil before any receive", *status.Interfaces[0].Last)
	}

	runtime.lastReceivedForPath("path-a", runtime.paths["path-a"]).Store(time.Now().Unix())
	status = tcpClientStatus(t, client)
	if status.Interfaces[0].Last == nil || *status.Interfaces[0].Last < 0 || *status.Interfaces[0].Last > 1 {
		t.Fatalf("last received = %v, want 0 or 1 second", status.Interfaces[0].Last)
	}

	runtime.lastReceivedForPath("path-a", runtime.paths["path-a"]).Store(time.Now().Unix() + 1)
	status = tcpClientStatus(t, client)
	if status.Interfaces[0].Last == nil || *status.Interfaces[0].Last != 0 {
		t.Fatalf("last received after concurrent update = %v, want 0 seconds", status.Interfaces[0].Last)
	}
}

func TestTCPClientStatusLabelsMissingActiveStandbyQuality(t *testing.T) {
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	client := New(config.Client{
		DstAddr:            "198.51.100.1:59501",
		ExcludedInterfaces: []string{"excluded"},
		Transfer:           transfer,
	}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "connecting"},
			{Index: 2, Name: "tracked-unhealthy"},
			{Index: 3, Name: "unavailable"},
			{Index: 4, Name: "excluded"},
		}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string {
		if iface.Name == "unavailable" {
			return ""
		}
		return "192.0.2.10"
	}
	trackedPath := tcpClientPath{index: 2, address: "192.0.2.10", destination: client.cfg.DstAddr}
	runtime := &tcpClientRuntime{
		client:   client,
		ctx:      context.Background(),
		flows:    make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:    map[string]tcpClientPath{"tracked-unhealthy": trackedPath},
		carriers: make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:  make(map[string]*stats.Traffic),
		sessions: map[string]*tcpPathSession{
			"tracked-unhealthy": {path: trackedPath},
		},
	}

	status, err := runtime.status()
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]string, len(status.Interfaces))
	for _, iface := range status.Interfaces {
		states[iface.Name] = iface.QualityState
	}
	if states["connecting"] != "connecting" || states["unavailable"] != "unavailable" || states["tracked-unhealthy"] != "unhealthy" || states["excluded"] != "" {
		t.Fatalf("quality states = %#v, want connecting/unavailable/unhealthy/empty excluded", states)
	}
}

func TestTCPClientStatusResolvesInterfacesOutsideRuntimeLock(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{DstAddr: "198.51.100.1:59501", Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	resolving := make(chan struct{})
	release := make(chan struct{})
	client.interfaceAddress = func(net.Interface) string {
		close(resolving)
		<-release
		return "192.0.2.10"
	}
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          context.Background(),
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        map[string]tcpClientPath{"path-a": {index: 1, address: "192.0.2.10", destination: client.cfg.DstAddr}},
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
		groups:       make(map[*tcpCarrierGroup]struct{}),
	}

	statusDone := make(chan error, 1)
	go func() {
		_, err := runtime.status()
		statusDone <- err
	}()
	select {
	case <-resolving:
	case <-time.After(time.Second):
		t.Fatal("status did not start resolving interface address")
	}

	lockAcquired := make(chan struct{})
	go func() {
		runtime.mu.Lock()
		runtime.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("interface address resolution held the runtime lock")
	}
	close(release)
	if err := <-statusDone; err != nil {
		t.Fatal(err)
	}
}

func TestTCPRefreshRetainsPathAcrossTransientAddressMisses(t *testing.T) {
	runtime := newTCPSessionManagerTestRuntime(t)
	runtime.client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Index: 1, Name: "path-a"}}, nil
	}
	address := "192.0.2.10"
	runtime.client.interfaceAddress = func(net.Interface) string { return address }
	peers, dialCount := installTCPSessionManagerDialer(t, runtime)

	runtime.refresh()
	firstPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if firstPeer.err != nil {
		t.Fatal(firstPeer.err)
	}
	path := tcpClientPath{index: 1, address: address, destination: runtime.client.paths.Destination("path-a")}
	firstPathSession := runtime.sessions["path-a"]
	waitTCPSessionManagerCondition(t, func() bool {
		_, _, healthy := firstPathSession.current(path)
		return healthy
	})

	address = ""
	for range tcpPathAddressMissGraceRefreshes {
		runtime.refresh()
		current, _, healthy := firstPathSession.current(path)
		if !healthy || current == nil || runtime.sessions["path-a"] != firstPathSession {
			t.Fatal("transient address miss replaced the healthy path Session")
		}
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("physical dials during address grace period = %d, want 1", got)
	}

	runtime.refresh()
	if _, exists := runtime.sessions["path-a"]; exists {
		t.Fatal("path remained after the address miss grace period")
	}
	select {
	case <-firstPeer.session.Done():
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("expired address path did not close its physical Session")
	}

	address = "192.0.2.10"
	runtime.refresh()
	secondPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if secondPeer.err != nil {
		t.Fatal(secondPeer.err)
	}
	if runtime.sessions["path-a"] == firstPathSession {
		t.Fatal("restored address reused the closed path manager")
	}
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("physical dials after address recovery = %d, want 2", got)
	}
}

func TestTCPCarrierObserverRecordsSkippedFrame(t *testing.T) {
	traffic := &stats.Traffic{}
	observer := tcpCarrierObserver(traffic)
	frame := tcpstream.Frame{Type: tcpstream.FrameData, Direction: tcpstream.DirectionServerToClient, Payload: []byte("payload")}

	observer.Skip(frame)
	got := traffic.Data.Snapshot()
	if got.SkippedPackets != 1 || got.SkippedBytes != uint64(len(frame.Payload)) {
		t.Fatalf("data skipped = %d packets/%d bytes, want 1/%d", got.SkippedPackets, got.SkippedBytes, len(frame.Payload))
	}
}

func TestTCPLastReceivedConnUpdatesOnAnyReadBytes(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})
	path := tcpClientPath{index: 1, address: "192.0.2.10", destination: "198.51.100.1:59501"}
	runtime := &tcpClientRuntime{paths: map[string]tcpClientPath{"path-a": path}}
	lastReceived := runtime.lastReceivedForPath("path-a", path)
	observed := &tcpLastReceivedConn{Conn: clientConn, lastReceived: lastReceived}

	writeDone := make(chan error, 1)
	go func() {
		_, err := serverConn.Write([]byte("bytes"))
		writeDone <- err
	}()
	payload := make([]byte, len("bytes"))
	if _, err := io.ReadFull(observed, payload); err != nil {
		t.Fatal(err)
	}
	if got := lastReceived.Load(); got == 0 {
		t.Fatal("received TCP bytes did not update last received")
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	lastReceived.Store(0)
	serverConn.Close()
	if read, err := observed.Read(payload); read != 0 || err == nil {
		t.Fatalf("closed read = %d/%v, want 0/error", read, err)
	}
	if got := lastReceived.Load(); got != 0 {
		t.Fatalf("failed empty read updated last received to %d", got)
	}
}

func TestTCPClientMaxStreamsClosesExcessAcceptedConnection(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 1
	client := New(config.Client{Transfer: transfer}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        make(map[string]tcpClientPath),
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
		groups:       make(map[*tcpCarrierGroup]struct{}),
		accepted:     make(map[*tcpAcceptedConn]struct{}),
	}
	defer runtime.shutdown()

	firstRuntime, firstPeer := net.Pipe()
	defer firstPeer.Close()
	if !runtime.startAccept(firstRuntime) {
		t.Fatal("first connection was rejected below maxStreams")
	}
	writeTestSOCKS5Connect(t, firstPeer)
	waitForTCPRuntimeCondition(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.accepted) == 1 && len(runtime.flows) == 1
	})

	excessRuntime, excessPeer := net.Pipe()
	defer excessPeer.Close()
	if runtime.startAccept(excessRuntime) {
		t.Fatal("connection above maxStreams was accepted")
	}
	_ = excessPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := excessPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("rejected connection remained open")
	}

	runtime.shutdown()
	_ = firstPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := firstPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection remained open after shutdown")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.accepted) != 0 || len(runtime.flows) != 0 || len(runtime.groups) != 0 || len(runtime.sessions) != 0 {
		t.Fatalf("shutdown retained accepted/flows/groups/sessions = %d/%d/%d/%d", len(runtime.accepted), len(runtime.flows), len(runtime.groups), len(runtime.sessions))
	}
}

func TestTCPClientZeroMaxStreamsAcceptsUnlimitedConnections(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 0
	client := New(config.Client{Transfer: transfer}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        make(map[string]tcpClientPath),
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
		groups:       make(map[*tcpCarrierGroup]struct{}),
		accepted:     make(map[*tcpAcceptedConn]struct{}),
	}
	defer runtime.shutdown()

	const connections = 3
	peers := make([]net.Conn, 0, connections)
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	for range connections {
		accepted, peer := net.Pipe()
		peers = append(peers, peer)
		if !runtime.startAccept(accepted) {
			t.Fatal("maxStreams=0 rejected an accepted connection")
		}
		writeTestSOCKS5Connect(t, peer)
	}
	waitForTCPRuntimeCondition(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.accepted) == connections && len(runtime.flows) == connections
	})
}

func TestTCPClientShutdownClosesAcceptedSOCKSHandshake(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{Transfer: transfer}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        make(map[string]tcpClientPath),
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
		groups:       make(map[*tcpCarrierGroup]struct{}),
		accepted:     make(map[*tcpAcceptedConn]struct{}),
	}

	accepted, peer := net.Pipe()
	defer peer.Close()
	if !runtime.startAccept(accepted) {
		t.Fatal("SOCKS5 handshake connection was rejected")
	}
	runtime.shutdown()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("SOCKS5 handshake connection remained open after shutdown")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.accepted) != 0 || len(runtime.flows) != 0 || len(runtime.groups) != 0 {
		t.Fatalf("shutdown retained accepted/flows/groups = %d/%d/%d", len(runtime.accepted), len(runtime.flows), len(runtime.groups))
	}
}

func TestTCPClientUnexpectedAcceptErrorCleansRuntime(t *testing.T) {
	acceptErr := errors.New("accept failed")
	listener := &tcpErrorListener{acceptErr: acceptErr, closed: make(chan struct{})}
	originalListenTCP := listenTCP
	listenTCP = func(string, string) (net.Listener, error) { return listener, nil }
	defer func() { listenTCP = originalListenTCP }()

	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{ListenAddr: "127.0.0.1:0", Transfer: transfer}, "test", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, nil }
	if err := client.runTCP(context.Background()); !errors.Is(err, acceptErr) {
		t.Fatalf("runTCP error = %v, want %v", err, acceptErr)
	}
	select {
	case <-listener.closed:
	default:
		t.Fatal("listener remained open after Accept error")
	}
	client.tcpRuntimeMu.RLock()
	runtime := client.tcpRuntime
	client.tcpRuntimeMu.RUnlock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.closing || len(runtime.accepted) != 0 || len(runtime.flows) != 0 || len(runtime.groups) != 0 || len(runtime.sessions) != 0 {
		t.Fatalf("runtime not cleaned after Accept error: closing=%v accepted=%d flows=%d groups=%d sessions=%d", runtime.closing, len(runtime.accepted), len(runtime.flows), len(runtime.groups), len(runtime.sessions))
	}
}

func TestTCPClientShutdownWaitsForCarrierGroupWorkers(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	client := New(config.Client{Transfer: transfer}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        make(map[string]tcpClientPath),
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
		groups:       make(map[*tcpCarrierGroup]struct{}),
		accepted:     make(map[*tcpAcceptedConn]struct{}),
	}

	originalDial := dialTCPOnInterface
	workerStarted := make(chan struct{})
	workerCanceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	dialTCPOnInterface = func(ctx context.Context, _, _, _ string, _ time.Duration) (net.Conn, error) {
		close(workerStarted)
		<-ctx.Done()
		close(workerCanceled)
		<-releaseWorker
		return nil, ctx.Err()
	}
	defer func() { dialTCPOnInterface = originalDial }()

	path := tcpClientPath{index: 1, address: "192.0.2.10", destination: "198.51.100.1:59501"}
	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("carrier group worker did not start")
	}
	shutdownDone := make(chan struct{})
	go func() {
		runtime.shutdown()
		close(shutdownDone)
	}()
	select {
	case <-workerCanceled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel carrier group worker")
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before carrier group worker exited")
	default:
	}
	close(releaseWorker)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after carrier group worker exited")
	}
}

func TestTCPClientShutdownClosesPhysicalSessionsBeforeBlockedFlows(t *testing.T) {
	runtime := newTCPSessionManagerTestRuntime(t)
	runtime.client.cfg.Transfer.KeepaliveIntervalMillis = 60_000
	runtime.client.cfg.Transfer.KeepaliveTimeoutMillis = 120_000
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	physicalReady := make(chan *tcpShutdownWriteBarrierConn, 1)
	peerReady := make(chan tcpSessionManagerPeer, 1)
	originalDial := dialTCPOnInterface
	dialTCPOnInterface = func(context.Context, string, string, string, time.Duration) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		physical := newTCPShutdownWriteBarrierConn(clientConn)
		physicalReady <- physical
		go func() {
			peer, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), nil)
			peerReady <- tcpSessionManagerPeer{interfaceName: "path-a", session: peer, err: err}
			if err == nil {
				serveTCPSessionManagerPeer(peer)
			}
		}()
		return physical, nil
	}
	defer func() { dialTCPOnInterface = originalDial }()
	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	var physical *tcpShutdownWriteBarrierConn
	select {
	case physical = <-physicalReady:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("physical session dial did not start")
	}
	var peer tcpSessionManagerPeer
	select {
	case peer = <-peerReady:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("physical session handshake did not finish")
	}
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	var clientSession *tcpstream.Session
	waitForTCPRuntimeCondition(t, func() bool {
		var healthy bool
		clientSession, _, healthy = runtime.sessions["path-a"].current(path)
		return healthy
	})

	flowConn, application := net.Pipe()
	defer application.Close()
	orderedEndpoint := &tcpShutdownOrderedEndpoint{
		Conn:          flowConn,
		sessionClosed: clientSession.Done(),
	}
	streamID := tcpstream.StreamID{9}
	flow := tcpstream.NewFlow(streamID, orderedEndpoint, tcpstream.DirectionClientToServer, runtime.flowConfig())
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	runtime.accepted[&tcpAcceptedConn{conn: orderedEndpoint}] = struct{}{}
	runtime.mu.Unlock()
	group, err := runtime.assignCarrierGroup(flow, destination, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-group.startedReady:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("virtual stream did not open before shutdown")
	}

	physical.armed.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := application.Write([]byte("block the physical smux writer"))
		writeDone <- writeErr
	}()
	select {
	case <-physical.started:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("physical write did not block")
	}

	shutdownDone := make(chan struct{})
	go func() {
		runtime.shutdown()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		_ = physical.Close()
		<-shutdownDone
		t.Fatal("shutdown waited on a blocked virtual stream close")
	}
	if orderedEndpoint.closedBeforeSession.Load() {
		t.Fatal("flow endpoint closed before its physical session entered shutdown")
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("blocked application write did not exit after shutdown")
	}
	_ = peer.session.Close()
}

type tcpShutdownWriteBarrierConn struct {
	net.Conn
	armed       atomic.Bool
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	closedOnce  sync.Once
}

func newTCPShutdownWriteBarrierConn(conn net.Conn) *tcpShutdownWriteBarrierConn {
	return &tcpShutdownWriteBarrierConn{
		Conn:    conn,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (conn *tcpShutdownWriteBarrierConn) Write(payload []byte) (int, error) {
	if conn.armed.Load() {
		conn.startedOnce.Do(func() { close(conn.started) })
		<-conn.release
	}
	return conn.Conn.Write(payload)
}

func (conn *tcpShutdownWriteBarrierConn) Close() error {
	var closeErr error
	conn.closedOnce.Do(func() {
		close(conn.release)
		closeErr = conn.Conn.Close()
	})
	return closeErr
}

type tcpShutdownOrderedEndpoint struct {
	net.Conn
	sessionClosed       <-chan struct{}
	closedBeforeSession atomic.Bool
}

func (conn *tcpShutdownOrderedEndpoint) Close() error {
	select {
	case <-conn.sessionClosed:
	default:
		conn.closedBeforeSession.Store(true)
	}
	return conn.Conn.Close()
}

func TestTCPClientPathMetricsEvictWithoutReusingObserverState(t *testing.T) {
	pathA := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	pathB := tcpClientPath{index: 2, address: "192.0.2.2", destination: "198.51.100.1:59501"}
	runtime := &tcpClientRuntime{
		paths:        map[string]tcpClientPath{"path-a": pathA, "path-b": pathB},
		traffic:      make(map[string]*stats.Traffic),
		lastReceived: make(map[string]*atomic.Int64),
	}
	oldTraffic := runtime.trafficForPath("path-a", pathA)
	oldLastReceived := runtime.lastReceivedForPath("path-a", pathA)
	preservedTraffic := runtime.trafficForPath("path-b", pathB)

	runtime.mu.Lock()
	runtime.setPathsLocked(map[string]tcpClientPath{"path-b": pathB})
	_, trafficRetained := runtime.traffic["path-a"]
	_, lastReceivedRetained := runtime.lastReceived["path-a"]
	runtime.mu.Unlock()
	if trafficRetained || lastReceivedRetained {
		t.Fatal("removed path metrics remained in runtime maps")
	}
	if got := runtime.trafficForPath("path-b", pathB); got != preservedTraffic {
		t.Fatal("unchanged path traffic was replaced")
	}

	readdedPathA := tcpClientPath{index: 3, address: "192.0.2.3", destination: pathA.destination}
	runtime.mu.Lock()
	runtime.setPathsLocked(map[string]tcpClientPath{"path-a": readdedPathA, "path-b": pathB})
	runtime.mu.Unlock()
	newTraffic := runtime.trafficForPath("path-a", readdedPathA)
	newLastReceived := runtime.lastReceivedForPath("path-a", readdedPathA)
	if newTraffic == oldTraffic || newLastReceived == oldLastReceived {
		t.Fatal("re-added path reused metrics still referenced by old carriers")
	}
	tcpCarrierObserver(oldTraffic).Read(tcpstream.Frame{Type: tcpstream.FrameData, Payload: []byte("old")})
	oldLastReceived.Store(time.Now().Unix())
	if got := newTraffic.Snapshot().Data.RXPackets; got != 0 {
		t.Fatalf("old carrier traffic was recorded on re-added path: %d packets", got)
	}
	if got := newLastReceived.Load(); got != 0 {
		t.Fatalf("old carrier receive timestamp was recorded on re-added path: %d", got)
	}

	oldPathBLastReceived := runtime.lastReceivedForPath("path-b", pathB)
	changedPathB := tcpClientPath{index: pathB.index, address: pathB.address, destination: "203.0.113.1:59501"}
	runtime.mu.Lock()
	runtime.setPathsLocked(map[string]tcpClientPath{"path-a": readdedPathA, "path-b": changedPathB})
	runtime.mu.Unlock()
	changedTraffic := runtime.trafficForPath("path-b", changedPathB)
	changedLastReceived := runtime.lastReceivedForPath("path-b", changedPathB)
	if changedTraffic == preservedTraffic || changedLastReceived == oldPathBLastReceived {
		t.Fatal("changed path reused metrics still referenced by old carriers")
	}
	tcpCarrierObserver(preservedTraffic).Read(tcpstream.Frame{Type: tcpstream.FrameData, Payload: []byte("stale")})
	oldPathBLastReceived.Store(time.Now().Unix())
	if got := changedTraffic.Snapshot().Data.RXPackets; got != 0 {
		t.Fatalf("stale path traffic was recorded after destination change: %d packets", got)
	}
	if got := changedLastReceived.Load(); got != 0 {
		t.Fatalf("stale path receive timestamp was recorded after destination change: %d", got)
	}
}

type tcpErrorListener struct {
	acceptErr error
	closed    chan struct{}
	closeOnce sync.Once
}

func (listener *tcpErrorListener) Accept() (net.Conn, error) {
	return nil, listener.acceptErr
}

func (listener *tcpErrorListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *tcpErrorListener) Addr() net.Addr {
	return tcpTestAddr("tcp-error-listener")
}

type tcpTestAddr string

func (addr tcpTestAddr) Network() string { return "tcp" }

func (addr tcpTestAddr) String() string { return string(addr) }

func waitForTCPRuntimeCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for TCP runtime condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func writeTestSOCKS5Connect(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if reply[0] != 5 || reply[1] != 0 {
		t.Fatalf("SOCKS5 method reply = %v", reply)
	}
	if _, err := conn.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
		t.Fatal(err)
	}
}

func assertTCPInterfaceStatus(t *testing.T, client *Client, wantStatus string, wantCarriers, wantSessions int) {
	t.Helper()
	status := tcpClientStatus(t, client)
	if len(status.Interfaces) != 1 || status.Interfaces[0].Status != wantStatus {
		t.Fatalf("interfaces = %#v, want one %s interface", status.Interfaces, wantStatus)
	}
	if status.Carriers != wantCarriers || status.Sessions != wantSessions {
		t.Fatalf("carriers/sessions = %d/%d, want %d/%d", status.Carriers, status.Sessions, wantCarriers, wantSessions)
	}
}

func tcpClientStatus(t *testing.T, client *Client) control.ClientStatus {
	t.Helper()
	statusValue, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	status := statusValue.(control.ClientStatus)
	if len(status.Interfaces) != 1 {
		t.Fatalf("interfaces = %#v, want one interface", status.Interfaces)
	}
	return status
}
