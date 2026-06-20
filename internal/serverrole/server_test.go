package serverrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/relay"
	"github.com/adrianceding/engarde/internal/transport"
	"github.com/adrianceding/engarde/internal/udp"
)

func resetServerHooks(t *testing.T) {
	t.Helper()
	originalRunControl := runControl
	originalResolveUDPAddr := resolveUDPAddr
	originalListenUDP := listenUDP
	t.Cleanup(func() {
		runControl = originalRunControl
		resolveUDPAddr = originalResolveUDPAddr
		listenUDP = originalListenUDP
	})
}

type fakeUDPRead struct {
	payload []byte
	addr    *net.UDPAddr
	err     error
}

type fakeUDPSocket struct {
	mu              sync.Mutex
	readItems       chan fakeUDPRead
	closed          chan struct{}
	writeErr        error
	writeBufferSize int
	writtenPayloads [][]byte
}

func newFakeUDPSocket() *fakeUDPSocket {
	return &fakeUDPSocket{readItems: make(chan fakeUDPRead, 4), closed: make(chan struct{})}
}

func (socket *fakeUDPSocket) ReadFromUDP(buffer []byte) (int, *net.UDPAddr, error) {
	select {
	case item := <-socket.readItems:
		if item.payload != nil {
			copy(buffer, item.payload)
		}
		return len(item.payload), item.addr, item.err
	case <-socket.closed:
		return 0, nil, net.ErrClosed
	}
}

func (socket *fakeUDPSocket) SetWriteDeadline(time.Time) error { return nil }

func (socket *fakeUDPSocket) SetWriteBuffer(size int) error {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	socket.writeBufferSize = size
	return nil
}

func (socket *fakeUDPSocket) WriteToUDP(payload []byte, addr *net.UDPAddr) (int, error) {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	if socket.writeErr != nil {
		return 0, socket.writeErr
	}
	socket.writtenPayloads = append(socket.writtenPayloads, append([]byte(nil), payload...))
	return len(payload), nil
}

func (socket *fakeUDPSocket) Close() error {
	select {
	case <-socket.closed:
	default:
		close(socket.closed)
	}
	return nil
}

func (socket *fakeUDPSocket) writtenCount() int {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	return len(socket.writtenPayloads)
}

func (socket *fakeUDPSocket) writtenSnapshot() [][]byte {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	payloads := make([][]byte, 0, len(socket.writtenPayloads))
	for _, payload := range socket.writtenPayloads {
		payloads = append(payloads, append([]byte(nil), payload...))
	}
	return payloads
}

func (socket *fakeUDPSocket) writeBuffer() int {
	socket.mu.Lock()
	defer socket.mu.Unlock()
	return socket.writeBufferSize
}

func TestStatusIncludesLearnedClient(t *testing.T) {
	server := New(config.Server{Description: "server", ListenAddr: "0.0.0.0:59501", DstAddr: "127.0.0.1:59301", ClientTimeout: 30}, "test-version", nil)
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 5000}

	server.learnClient(addr)
	client := server.clients[addr.String()]
	client.traffic.Data.RecordRX(100)
	client.traffic.Data.RecordTX(200)
	client.traffic.Control.RecordTX(36)
	pathNow := transport.NowMillis()
	server.pathStats[addr.String()] = &transport.PathStats{ID: addr.String(), LastSeen: pathNow - 80, LastSuccess: pathNow - 40, SmoothedRTT: 16, RTTVariance: 4, Failures: 2}
	server.selection = transport.PathSelection{FirstPathIDs: []string{addr.String()}}
	statusValue, err := server.Status()
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	status := statusValue.(control.ServerStatus)

	if status.Type != "server" || status.Version != "test-version" || status.Description != "server" {
		t.Fatalf("status metadata = %#v", status)
	}
	if status.ListenAddress != "0.0.0.0:59501" || status.DstAddress != "127.0.0.1:59301" {
		t.Fatalf("status addresses = %#v", status)
	}
	if len(status.Sockets) != 1 {
		t.Fatalf("len(Sockets) = %d, want 1", len(status.Sockets))
	}
	if status.Sockets[0].Address != addr.String() || status.Sockets[0].Last == nil || status.Sockets[0].PathRole != transport.PathRoleFirst {
		t.Fatalf("socket status = %#v", status.Sockets[0])
	}
	if status.Sockets[0].Traffic.Data.RXPackets != 1 || status.Sockets[0].Traffic.Data.RXBytes != 100 || status.Sockets[0].Traffic.Data.TXPackets != 1 || status.Sockets[0].Traffic.Data.TXBytes != 200 {
		t.Fatalf("data traffic = %#v", status.Sockets[0].Traffic.Data)
	}
	if status.Sockets[0].Traffic.Control.TXPackets != 1 || status.Sockets[0].Traffic.Control.TXBytes != 36 {
		t.Fatalf("control traffic = %#v", status.Sockets[0].Traffic.Control)
	}
	if status.Sockets[0].Path == nil || status.Sockets[0].Path.SmoothedRTTMillis != 16 || status.Sockets[0].Path.Failures != 2 || status.Sockets[0].Path.LastSeen == nil || status.Sockets[0].Path.LastSuccess == nil {
		t.Fatalf("path status = %#v", status.Sockets[0].Path)
	}
}

func TestClientTargetsRemoveExpiredClientsOnly(t *testing.T) {
	server := New(config.Server{ClientTimeout: 30}, "", nil)
	activeAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 20), Port: 5001}
	expiredAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 21), Port: 5002}
	active := &connectedClient{addr: activeAddr}
	expired := &connectedClient{addr: expiredAddr}
	active.last.Store(90)
	expired.last.Store(10)
	server.clients[activeAddr.String()] = active
	server.clients[expiredAddr.String()] = expired

	targets := server.clientTargets(100)
	if len(targets) != 1 || targets[0].ID != activeAddr.String() {
		t.Fatalf("targets = %#v, want active client only", targets)
	}
	if _, ok := server.clients[expiredAddr.String()]; ok {
		t.Fatal("expired client was not removed")
	}
	if _, ok := server.clients[activeAddr.String()]; !ok {
		t.Fatal("active client was removed")
	}
}

func TestRemoveExpiredClientKeepsRefreshedClient(t *testing.T) {
	server := New(config.Server{ClientTimeout: 30}, "", nil)
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 30), Port: 5003}
	client := &connectedClient{addr: addr}
	client.last.Store(100)
	server.clients[addr.String()] = client

	server.removeExpiredClient(addr.String(), 70)
	if _, ok := server.clients[addr.String()]; !ok {
		t.Fatal("refreshed client was removed")
	}
}

func TestRemoveExpiredClientMissing(t *testing.T) {
	server := New(config.Server{}, "", nil)
	server.removeExpiredClient("missing", 0)
}

func TestRemoveClient(t *testing.T) {
	server := New(config.Server{}, "", nil)
	server.wgSocket = newFakeUDPSocket()
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 40), Port: 5004}
	server.clients[addr.String()] = &connectedClient{addr: addr}
	server.pathStats[addr.String()] = &transport.PathStats{ID: addr.String()}
	server.removeClient(addr.String())
	if _, ok := server.clients[addr.String()]; ok {
		t.Fatal("client was not removed")
	}
	if _, ok := server.pathStats[addr.String()]; ok {
		t.Fatal("pathStats entry was not removed")
	}
}

func TestRetryDoesNotRecreateRemovedClientPathStats(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10}}, "", nil)
	id := server.tracker.NextID()
	clientID := "192.0.2.40:5004"
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: clientID, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	server.retryAdaptiveData(20)

	if _, ok := server.pathStats[clientID]; ok {
		t.Fatal("removed client pathStats was recreated by retry failure accounting")
	}
}

func TestUpdateWireGuardWriteBufferTracksClients(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	server := New(config.Server{}, "", nil)
	server.wgSocket = wgSocket

	server.updateWireGuardWriteBuffer()
	if got := wgSocket.writeBuffer(); got != relay.DefaultWriteBufferBytes {
		t.Fatalf("initial write buffer = %d, want %d", got, relay.DefaultWriteBufferBytes)
	}

	first := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 41), Port: 5005}
	second := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 42), Port: 5006}
	server.clients[first.String()] = &connectedClient{addr: first}
	server.clients[second.String()] = &connectedClient{addr: second}
	server.updateWireGuardWriteBuffer()
	want := relay.DefaultWriteBufferBytes + relay.DefaultWriteBufferTargetBytes
	if got := wgSocket.writeBuffer(); got != want {
		t.Fatalf("two-client write buffer = %d, want %d", got, want)
	}

	server.removeClient(second.String())
	if got := wgSocket.writeBuffer(); got != relay.DefaultWriteBufferBytes {
		t.Fatalf("one-client write buffer = %d, want %d", got, relay.DefaultWriteBufferBytes)
	}
}

func TestRunRejectsInvalidAddresses(t *testing.T) {
	server := New(config.Server{DstAddr: "bad dst", ListenAddr: "127.0.0.1:0"}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with invalid dst address")
	}
	server = New(config.Server{DstAddr: "127.0.0.1:1", ListenAddr: "bad listen"}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with invalid listen address")
	}
}

func TestRunResolveAndListenErrors(t *testing.T) {
	resetServerHooks(t)
	resolveUDPAddr = func(network, address string) (*net.UDPAddr, error) {
		if address == "0.0.0.0:0" {
			return nil, errors.New("resolve source")
		}
		return net.ResolveUDPAddr(network, address)
	}
	server := New(config.Server{DstAddr: "127.0.0.1:1", ListenAddr: "127.0.0.1:0"}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded after source resolve error")
	}

	resolveUDPAddr = net.ResolveUDPAddr
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		return nil, errors.New("listen")
	}
	server = New(config.Server{DstAddr: "127.0.0.1:1", ListenAddr: "127.0.0.1:0"}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded after wg listen error")
	}

	wgSocket := newFakeUDPSocket()
	listenCalls := 0
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		listenCalls++
		if listenCalls == 1 {
			return wgSocket, nil
		}
		return nil, errors.New("client listen")
	}
	server = New(config.Server{DstAddr: "127.0.0.1:1", ListenAddr: "127.0.0.1:0"}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded after client listen error")
	}
	select {
	case <-wgSocket.closed:
	default:
		t.Fatal("wg socket was not closed after client listen error")
	}
}

func TestRunControlErrorBranch(t *testing.T) {
	resetServerHooks(t)
	controlCalled := make(chan struct{}, 1)
	runControl = func(ctx context.Context, listenAddr, username, password string, webFS fs.FS, status control.StatusProvider, actions control.ClientActions) error {
		controlCalled <- struct{}{}
		return errors.New("control")
	}
	wgSocket := newFakeUDPSocket()
	clientSocket := newFakeUDPSocket()
	listenCalls := 0
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		listenCalls++
		if listenCalls == 1 {
			return wgSocket, nil
		}
		return clientSocket, nil
	}
	server := New(config.Server{DstAddr: "127.0.0.1:1", ListenAddr: "127.0.0.1:0", WebManager: config.WebManager{ListenAddr: "127.0.0.1:0"}}, "", nil)
	done := make(chan error, 1)
	go func() { done <- server.Run(context.Background()) }()
	select {
	case <-controlCalled:
	case <-time.After(time.Second):
		t.Fatal("Run did not call control hook")
	}
	clientSocket.Close()
	wgSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRunListenAddressInUse(t *testing.T) {
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgSocket.Close()
	usedSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer usedSocket.Close()
	server := New(config.Server{DstAddr: wgSocket.LocalAddr().String(), ListenAddr: usedSocket.LocalAddr().String()}, "", nil)
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with listen address in use")
	}
}

func TestReceiveFromClientStopsAndWriteError(t *testing.T) {
	server := New(config.Server{}, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.clientSocket, _ = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	server.clientSocket.Close()
	if err := server.receiveFromClient(ctx); err != nil {
		t.Fatalf("receiveFromClient closed context error = %v", err)
	}

	clientSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	wgAddr := wgSocket.LocalAddr().(*net.UDPAddr)
	wgSocket.Close()
	server = New(config.Server{}, "", nil)
	server.clientSocket = clientSocket
	server.wgSocket = wgSocket
	server.wgAddr = wgAddr
	done := make(chan error, 1)
	go func() { done <- server.receiveFromClient(context.Background()) }()
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("payload"), clientSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	clientSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveFromClient returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiveFromClient did not stop")
	}
}

func TestReceiveFromClientReadErrorThenStop(t *testing.T) {
	clientSocket := newFakeUDPSocket()
	clientSocket.readItems <- fakeUDPRead{err: errors.New("read")}
	server := New(config.Server{}, "", nil)
	server.clientSocket = clientSocket
	server.wgSocket = newFakeUDPSocket()
	server.wgAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	done := make(chan error, 1)
	go func() { done <- server.receiveFromClient(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	clientSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveFromClient returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiveFromClient did not stop")
	}
}

func TestReceiveFromWireGuardStopsAndWriteError(t *testing.T) {
	server := New(config.Server{}, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.wgSocket, _ = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	server.wgSocket.Close()
	server.receiveFromWireGuard(ctx)

	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	clientSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	clientAddr := clientSocket.LocalAddr().(*net.UDPAddr)
	clientSocket.Close()
	server = New(config.Server{ClientTimeout: 30, WriteTimeout: 10}, "", nil)
	server.wgSocket = wgSocket
	server.clientSocket = clientSocket
	client := &connectedClient{addr: clientAddr}
	client.last.Store(time.Now().Unix())
	server.clients[clientAddr.String()] = client
	done := make(chan struct{})
	go func() {
		server.receiveFromWireGuard(context.Background())
		close(done)
	}()
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("payload"), wgSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.clientsMu.RLock()
		_, ok := server.clients[clientAddr.String()]
		server.clientsMu.RUnlock()
		if !ok {
			wgSocket.Close()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client was not removed after write error")
}

func TestReceiveFromWireGuardReadErrorThenStop(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	wgSocket.readItems <- fakeUDPRead{err: errors.New("read")}
	server := New(config.Server{}, "", nil)
	server.wgSocket = wgSocket
	server.clientSocket = newFakeUDPSocket()
	done := make(chan struct{})
	go func() {
		server.receiveFromWireGuard(context.Background())
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	wgSocket.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receiveFromWireGuard did not stop")
	}
}

func TestReceiveFromClientAdaptiveDataAckAndDuplicateSuppression(t *testing.T) {
	clientSocket := newFakeUDPSocket()
	wgSocket := newFakeUDPSocket()
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}}, "", nil)
	server.clientSocket = clientSocket
	server.wgSocket = wgSocket
	server.wgAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}
	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 60), Port: 5060}
	framePayload, err := transport.Encode(transport.Frame{Type: transport.FrameData, ID: transport.PacketID{Session: 2, Sequence: 1}, SentAt: transport.NowMillis(), Payload: []byte("inner")})
	if err != nil {
		t.Fatal(err)
	}
	clientSocket.readItems <- fakeUDPRead{payload: framePayload, addr: clientAddr}
	clientSocket.readItems <- fakeUDPRead{payload: framePayload, addr: clientAddr}
	done := make(chan error, 1)
	go func() { done <- server.receiveFromClient(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	clientSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveFromClient returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiveFromClient did not stop")
	}

	wgPayloads := wgSocket.writtenSnapshot()
	if len(wgPayloads) != 1 || string(wgPayloads[0]) != "inner" {
		t.Fatalf("WireGuard writes = %#v", wgPayloads)
	}
	if clientSocket.writtenCount() < 2 {
		t.Fatalf("ACK writes = %d, want at least 2", clientSocket.writtenCount())
	}
	traffic := server.clients[clientAddr.String()].traffic.Snapshot()
	if traffic.Data.RXPackets != 1 || traffic.Data.RXBytes != uint64(len("inner")) {
		t.Fatalf("adaptive data RX traffic = %#v", traffic.Data)
	}
	if traffic.Control.TXPackets != 2 {
		t.Fatalf("adaptive control TX traffic = %#v", traffic.Control)
	}
}

func TestServerAdaptiveKeepaliveTracksPending(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 61), Port: 5061}
	client := &connectedClient{addr: addr}
	client.last.Store(time.Now().Unix())
	server.clients[addr.String()] = client
	server.sendKeepaliveToClients(100)
	time.Sleep(20 * time.Millisecond)
	if clientSocket.writtenCount() != 1 {
		t.Fatalf("keepalive writes = %d, want 1", clientSocket.writtenCount())
	}
	due := server.tracker.Due(200, 50, 1000, 1)
	if len(due) != 1 || due[0].PathID != addr.String() {
		t.Fatalf("due keepalive = %#v", due)
	}
}

func TestServerAdaptiveKeepaliveAckCompletesPending(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 61), Port: 5061}
	client := &connectedClient{addr: addr}
	client.last.Store(time.Now().Unix())
	server.clients[addr.String()] = client
	server.sendKeepaliveToClients(100)
	time.Sleep(20 * time.Millisecond)
	payloads := clientSocket.writtenSnapshot()
	if len(payloads) != 1 {
		t.Fatalf("keepalive writes = %d, want 1", len(payloads))
	}
	frame, err := transport.Decode(payloads[0])
	if err != nil {
		t.Fatal(err)
	}
	ackPayload, err := transport.Encode(transport.Frame{Type: transport.FrameKeepaliveAck, ID: frame.ID, SentAt: frame.SentAt})
	if err != nil {
		t.Fatal(err)
	}
	writeBatch := make([]udp.Packet, 0, 1)
	server.handleAdaptiveFromClient(udp.Packet{Payload: ackPayload}, addr, time.Now().Unix(), 120, &writeBatch)
	if _, ok := server.tracker.Complete(frame.ID); ok {
		t.Fatal("keepalive pending record was not completed by ACK")
	}
	if due := server.tracker.Due(200, 50, 1000, 1); len(due) != 0 {
		t.Fatalf("ACKed keepalive was retried: %#v", due)
	}
}

func TestServerAdaptiveDataUsesCachedFirstPaths(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 70), Port: 5070}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 71), Port: 5071}
	thirdAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 72), Port: 5072}
	nowUnix := time.Now().Unix()
	server.clients[firstAddr.String()] = &connectedClient{addr: firstAddr}
	server.clients[firstAddr.String()].last.Store(nowUnix)
	server.clients[secondAddr.String()] = &connectedClient{addr: secondAddr}
	server.clients[secondAddr.String()].last.Store(nowUnix)
	server.clients[thirdAddr.String()] = &connectedClient{addr: thirdAddr}
	server.clients[thirdAddr.String()].last.Store(nowUnix)
	now := transport.NowMillis()
	server.pathStats[firstAddr.String()] = &transport.PathStats{ID: firstAddr.String(), LastSuccess: now, SmoothedRTT: 100}
	server.pathStats[secondAddr.String()] = &transport.PathStats{ID: secondAddr.String(), LastSuccess: now, SmoothedRTT: 90}
	server.pathStats[thirdAddr.String()] = &transport.PathStats{ID: thirdAddr.String(), LastSuccess: now, SmoothedRTT: 80}
	server.selection = transport.PathSelection{FirstPathIDs: []string{firstAddr.String(), secondAddr.String()}, FallbackPathIDs: []string{thirdAddr.String()}}

	server.sendAdaptiveData([]byte("inner"))
	time.Sleep(20 * time.Millisecond)

	if clientSocket.writtenCount() != 2 {
		t.Fatalf("client socket writes = %d, want 2", clientSocket.writtenCount())
	}
	payloads := clientSocket.writtenSnapshot()
	for _, payload := range payloads {
		frame, err := transport.Decode(payload)
		if err != nil {
			t.Fatal(err)
		}
		if string(frame.Payload) != "inner" {
			t.Fatalf("payload = %q, want inner", string(frame.Payload))
		}
	}
}

func TestServerRefreshPathSelectionRequiresSignificantImprovement(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}, ClientTimeout: 30}, "", nil)
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 73), Port: 5073}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 74), Port: 5074}
	nowUnix := time.Now().Unix()
	server.clients[firstAddr.String()] = &connectedClient{addr: firstAddr}
	server.clients[firstAddr.String()].last.Store(nowUnix)
	server.clients[secondAddr.String()] = &connectedClient{addr: secondAddr}
	server.clients[secondAddr.String()].last.Store(nowUnix)
	now := transport.NowMillis()
	server.pathStats[firstAddr.String()] = &transport.PathStats{ID: firstAddr.String(), LastSuccess: now, SmoothedRTT: 100}
	server.pathStats[secondAddr.String()] = &transport.PathStats{ID: secondAddr.String(), LastSuccess: now, SmoothedRTT: 90}
	server.selection = transport.PathSelection{FirstPathIDs: []string{firstAddr.String()}}

	server.refreshPathSelection(now)
	if got := server.selection.FirstPathIDs; len(got) != 1 || got[0] != firstAddr.String() {
		t.Fatalf("first paths after small RTT change = %#v, want %q", got, firstAddr.String())
	}

	server.pathStats[secondAddr.String()].SmoothedRTT = 70
	server.refreshPathSelection(now)
	if got := server.selection.FirstPathIDs; len(got) != 1 || got[0] != secondAddr.String() {
		t.Fatalf("first paths after significant RTT change = %#v, want %q", got, secondAddr.String())
	}
}

func TestServerRetryUsesOnlyUnsentFallbackPath(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 75), Port: 5075}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 76), Port: 5076}
	thirdAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 77), Port: 5077}
	nowUnix := time.Now().Unix()
	for _, addr := range []*net.UDPAddr{firstAddr, secondAddr, thirdAddr} {
		server.clients[addr.String()] = &connectedClient{addr: addr}
		server.clients[addr.String()].last.Store(nowUnix)
		server.pathStats[addr.String()] = &transport.PathStats{ID: addr.String(), LastSuccess: 100, SmoothedRTT: 50}
	}
	id := server.tracker.NextID()
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: firstAddr.String(), PathIDs: []string{firstAddr.String(), secondAddr.String()}, AttemptPathIDs: []string{firstAddr.String(), secondAddr.String()}, FallbackPathIDs: []string{thirdAddr.String()}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	server.retryAdaptiveData(100)
	time.Sleep(20 * time.Millisecond)

	if clientSocket.writtenCount() != 1 {
		t.Fatalf("retry writes = %d, want 1", clientSocket.writtenCount())
	}
	record, ok := server.tracker.Get(id)
	if !ok {
		t.Fatal("pending record missing")
	}
	if got, want := record.PathIDs, []string{firstAddr.String(), secondAddr.String(), thirdAddr.String()}; !sameStrings(got, want) {
		t.Fatalf("PathIDs = %#v, want %#v", got, want)
	}
	if got, want := record.AttemptPathIDs, []string{thirdAddr.String()}; !sameStrings(got, want) {
		t.Fatalf("AttemptPathIDs = %#v, want %#v", got, want)
	}
}

func TestServerRetryDropsWhenNoFallbackPath(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 78), Port: 5078}
	server.clients[addr.String()] = &connectedClient{addr: addr}
	server.clients[addr.String()].last.Store(time.Now().Unix())
	server.pathStats[addr.String()] = &transport.PathStats{ID: addr.String(), LastSuccess: 100, SmoothedRTT: 50}
	server.selection = transport.PathSelection{FirstPathIDs: []string{addr.String()}}
	id := server.tracker.NextID()
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: addr.String(), PathIDs: []string{addr.String()}, AttemptPathIDs: []string{addr.String()}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	server.retryAdaptiveData(100)
	time.Sleep(20 * time.Millisecond)

	if clientSocket.writtenCount() != 0 {
		t.Fatalf("retry writes = %d, want 0", clientSocket.writtenCount())
	}
	if _, ok := server.tracker.Get(id); ok {
		t.Fatal("pending record was not dropped")
	}
}

func TestServerRetryUsesCurrentFallbackPath(t *testing.T) {
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}, ClientTimeout: 30}, "", nil)
	clientSocket := newFakeUDPSocket()
	server.clientSocket = clientSocket
	firstAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 79), Port: 5079}
	secondAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 80), Port: 5080}
	nowUnix := time.Now().Unix()
	for _, addr := range []*net.UDPAddr{firstAddr, secondAddr} {
		server.clients[addr.String()] = &connectedClient{addr: addr}
		server.clients[addr.String()].last.Store(nowUnix)
		server.pathStats[addr.String()] = &transport.PathStats{ID: addr.String(), LastSuccess: 100, SmoothedRTT: 50}
	}
	server.selection = transport.PathSelection{FirstPathIDs: []string{firstAddr.String()}, FallbackPathIDs: []string{secondAddr.String()}}
	id := server.tracker.NextID()
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: firstAddr.String(), PathIDs: []string{firstAddr.String()}, AttemptPathIDs: []string{firstAddr.String()}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	server.retryAdaptiveData(100)
	time.Sleep(20 * time.Millisecond)

	if clientSocket.writtenCount() != 1 {
		t.Fatalf("current fallback writes = %d, want 1", clientSocket.writtenCount())
	}
}

func intPtr(value int) *int {
	return &value
}

func sameStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestReceiveFromClientAdaptiveInvalidFrameFallbackDependsOnConfirmation(t *testing.T) {
	invalidFrame := make([]byte, transport.HeaderSize)
	copy(invalidFrame, []byte{0x45, 0x47, 0x41, 0x44})
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 62), Port: 5062}
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	server.wgAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}
	writeBatch := make([]udp.Packet, 0, 1)
	now := transport.NowMillis()
	server.handleAdaptiveFromClient(udp.Packet{Payload: invalidFrame}, addr, time.Now().Unix(), now, &writeBatch)
	if len(writeBatch) != 1 {
		t.Fatalf("unconfirmed invalid frame writes = %d, want 1", len(writeBatch))
	}

	writeBatch = writeBatch[:0]
	server.markPathSuccess(addr.String(), now, 10)
	server.handleAdaptiveFromClient(udp.Packet{Payload: invalidFrame}, addr, time.Now().Unix(), now, &writeBatch)
	if len(writeBatch) != 0 {
		t.Fatalf("confirmed invalid frame writes = %d, want 0", len(writeBatch))
	}
}

func TestReceiveFromClientAdaptiveAllowsOversizedRawOnConfirmedPath(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 63), Port: 5063}
	server := New(config.Server{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	server.wgAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}
	now := transport.NowMillis()
	server.markPathSuccess(addr.String(), now, 10)
	rawPayload := make([]byte, transport.MaxPayloadSize+1)
	writeBatch := make([]udp.Packet, 0, 1)

	server.handleAdaptiveFromClient(udp.Packet{Payload: rawPayload}, addr, time.Now().Unix(), now, &writeBatch)

	if len(writeBatch) != 1 || len(writeBatch[0].Payload) != len(rawPayload) {
		t.Fatalf("writeBatch = %#v, want oversized raw payload", writeBatch)
	}
}

func TestLearnClientUpdatesExisting(t *testing.T) {
	server := New(config.Server{}, "", nil)
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 55), Port: 5055}
	client := &connectedClient{addr: addr}
	client.last.Store(1)
	server.clients[addr.String()] = client
	server.learnClient(addr)
	if client.last.Load() <= 1 {
		t.Fatal("existing client timestamp was not updated")
	}
}

func TestRunLoopbackUDPForwarding(t *testing.T) {
	resetServerHooks(t)
	controlStarted := make(chan struct{}, 1)
	runControl = func(ctx context.Context, listenAddr, username, password string, webFS fs.FS, status control.StatusProvider, actions control.ClientActions) error {
		controlStarted <- struct{}{}
		<-ctx.Done()
		return nil
	}

	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgSocket.Close()

	serverListen := freeUDPAddr(t)
	runWGSockets := make(chan *net.UDPConn, 1)
	runClientSockets := make(chan *net.UDPConn, 1)
	listenCalls := 0
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		socket, err := net.ListenUDP(network, laddr)
		if err != nil {
			return nil, err
		}
		listenCalls++
		if listenCalls == 1 {
			runWGSockets <- socket
		} else {
			runClientSockets <- socket
		}
		return socket, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := New(config.Server{Description: "server", ListenAddr: serverListen.String(), DstAddr: wgSocket.LocalAddr().String(), ClientTimeout: 30, WriteTimeout: 10, WebManager: config.WebManager{ListenAddr: "127.0.0.1:0"}}, "test", fstest.MapFS{"index.html": {Data: []byte("ok")}})
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})

	var runWGSocket *net.UDPConn
	select {
	case runWGSocket = <-runWGSockets:
	case <-time.After(time.Second):
		t.Fatal("server WireGuard socket was not initialized")
	}
	select {
	case <-runClientSockets:
	case <-time.After(time.Second):
		t.Fatal("server client socket was not initialized")
	}
	select {
	case <-controlStarted:
	case <-time.After(time.Second):
		t.Fatal("control server was not started")
	}

	clientSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer clientSocket.Close()

	if _, err := clientSocket.WriteToUDP([]byte("client-to-wg"), serverListen); err != nil {
		t.Fatal(err)
	}
	assertUDPReadEventually(t, wgSocket, "client-to-wg")

	serverWGAddr := runWGSocket.LocalAddr().(*net.UDPAddr)
	serverWGLoopback := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: serverWGAddr.Port}
	if _, err := wgSocket.WriteToUDP([]byte("wg-to-client"), serverWGLoopback); err != nil {
		t.Fatal(err)
	}
	assertUDPRead(t, clientSocket, "wg-to-client")
}

func freeUDPAddr(t *testing.T) *net.UDPAddr {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	addr := socket.LocalAddr().(*net.UDPAddr)
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForServerSockets(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.wgSocket != nil && server.clientSocket != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server sockets were not initialized")
}

func assertUDPRead(t *testing.T, socket *net.UDPConn, want string) {
	t.Helper()
	if err := socket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1500)
	n, _, err := socket.ReadFromUDP(buffer)
	if err != nil {
		t.Fatalf("ReadFromUDP returned error: %v", err)
	}
	if got := string(buffer[:n]); got != want {
		t.Fatalf("UDP payload = %q, want %q", got, want)
	}
}

func assertUDPReadEventually(t *testing.T, socket *net.UDPConn, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	buffer := make([]byte, 1500)
	for time.Now().Before(deadline) {
		if err := socket.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, _, err := socket.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		if got := string(buffer[:n]); got == want {
			return
		}
	}
	t.Fatalf("did not read UDP payload %q", want)
}
