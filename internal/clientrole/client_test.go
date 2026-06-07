package clientrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"testing"
	"testing/fstest"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/relay"
)

func resetClientHooks(t *testing.T) {
	t.Helper()
	originalRunControl := runControl
	originalResolveUDPAddr := resolveUDPAddr
	originalListenUDP := listenUDP
	originalNewRefreshTicker := newRefreshTicker
	t.Cleanup(func() {
		runControl = originalRunControl
		resolveUDPAddr = originalResolveUDPAddr
		listenUDP = originalListenUDP
		newRefreshTicker = originalNewRefreshTicker
	})
}

type fakeUDPRead struct {
	payload []byte
	addr    *net.UDPAddr
	err     error
}

type fakeUDPSocket struct {
	readItems       chan fakeUDPRead
	closed          chan struct{}
	writeErr        error
	writeBufferSize int
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
	socket.writeBufferSize = size
	return nil
}

func (socket *fakeUDPSocket) WriteToUDP(payload []byte, addr *net.UDPAddr) (int, error) {
	if socket.writeErr != nil {
		return 0, socket.writeErr
	}
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

func TestClientActionsPreserveLegacyStatuses(t *testing.T) {
	client := New(config.Client{ExcludedInterfaces: []string{"wg0"}}, "", nil)

	if status := client.Include("wg0"); status != "ok" {
		t.Fatalf("Include excluded interface = %q, want ok", status)
	}
	if status := client.Include("wg0"); status != "already-included" {
		t.Fatalf("Include included interface = %q, want already-included", status)
	}
	if status := client.Exclude("wg0"); status != "ok" {
		t.Fatalf("Exclude included interface = %q, want ok", status)
	}
	if status := client.Exclude("wg0"); status != "already-excluded" {
		t.Fatalf("Exclude excluded interface = %q, want already-excluded", status)
	}
	if status := client.ToggleOverride("eth0"); status != "ok" {
		t.Fatalf("ToggleOverride = %q, want ok", status)
	}
	if status := client.ResetExclusions(); status != "ok" {
		t.Fatalf("ResetExclusions = %q, want ok", status)
	}
}

func TestDefaultListenUDPAndCloseAllRoutes(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	socket, err := listenUDP("udp4", addr)
	if err != nil {
		t.Fatalf("listenUDP returned error: %v", err)
	}
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}

	first := newFakeUDPSocket()
	second := newFakeUDPSocket()
	client := New(config.Client{}, "", nil)
	client.routes["first"] = &sendRoute{ifName: "first", socket: first}
	client.routes["second"] = &sendRoute{ifName: "second", socket: second}
	client.closeAllRoutes()
	if len(client.routes) != 0 {
		t.Fatalf("routes were not cleared: %#v", client.routes)
	}
	for name, socket := range map[string]*fakeUDPSocket{"first": first, "second": second} {
		select {
		case <-socket.closed:
		default:
			t.Fatalf("%s socket was not closed", name)
		}
	}
}

func TestUpdateWireGuardWriteBufferTracksRoutes(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	client := New(config.Client{}, "", nil)
	client.wgSocket = wgSocket

	client.updateWireGuardWriteBuffer()
	if wgSocket.writeBufferSize != relay.DefaultWriteBufferBytes {
		t.Fatalf("initial write buffer = %d, want %d", wgSocket.writeBufferSize, relay.DefaultWriteBufferBytes)
	}

	first := newFakeUDPSocket()
	second := newFakeUDPSocket()
	client.routes["first"] = &sendRoute{ifName: "first", socket: first}
	client.routes["second"] = &sendRoute{ifName: "second", socket: second}
	client.updateWireGuardWriteBuffer()
	want := relay.DefaultWriteBufferBytes + relay.DefaultWriteBufferTargetBytes
	if wgSocket.writeBufferSize != want {
		t.Fatalf("two-route write buffer = %d, want %d", wgSocket.writeBufferSize, want)
	}

	client.removeRoute("second")
	if wgSocket.writeBufferSize != relay.DefaultWriteBufferBytes {
		t.Fatalf("one-route write buffer = %d, want %d", wgSocket.writeBufferSize, relay.DefaultWriteBufferBytes)
	}
}

func TestClientStatusIncludesActiveRoute(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("Interfaces returned error: %v", err)
	}
	if len(interfaces) == 0 {
		t.Skip("no network interfaces available")
	}
	ifName := interfaces[0].Name
	client := New(config.Client{Description: "client", ListenAddr: "127.0.0.1:59401", DstAddr: "198.51.100.1:59501"}, "test-version", nil)
	dstAddr, err := net.ResolveUDPAddr("udp4", "198.51.100.1:59501")
	if err != nil {
		t.Fatal(err)
	}
	route := &sendRoute{ifName: ifName, srcAddr: "192.0.2.10", dstAddr: dstAddr}
	route.lastRec.Store(time.Now().Unix() - 1)
	client.routes[ifName] = route

	statusValue, err := client.Status()
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	status := statusValue.(control.ClientStatus)
	if status.Type != "client" || status.Version != "test-version" || status.Description != "client" {
		t.Fatalf("status metadata = %#v", status)
	}
	if status.ListenAddress != "127.0.0.1:59401" {
		t.Fatalf("ListenAddress = %q, want 127.0.0.1:59401", status.ListenAddress)
	}

	for _, webInterface := range status.Interfaces {
		if webInterface.Name != ifName {
			continue
		}
		if webInterface.Status != "active" || webInterface.DstAddress != "198.51.100.1:59501" || webInterface.Last == nil {
			t.Fatalf("interface status = %#v", webInterface)
		}
		return
	}
	t.Fatalf("interface %q not found in status %#v", ifName, status.Interfaces)
}

func waitForUDP(t *testing.T, addr *net.UDPAddr) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
		if err != nil {
			t.Fatal(err)
		}
		_, err = socket.WriteToUDP([]byte("probe"), addr)
		socket.Close()
		if err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("UDP address %v did not become writable", addr)
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

func TestClientStatusErrorAndIdleExcluded(t *testing.T) {
	client := New(config.Client{ListenAddr: "127.0.0.1:59401", DstAddr: "198.51.100.1:59501", ExcludedInterfaces: []string{"excluded"}}, "", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, errors.New("interfaces") }
	if _, err := client.Status(); err == nil {
		t.Fatal("Status succeeded after interface error")
	}

	client.listInterfaces = func() ([]net.Interface, error) { return []net.Interface{{Name: "excluded"}, {Name: "idle"}}, nil }
	client.interfaceAddress = func(iface net.Interface) string { return "192.0.2.10" }
	statusValue, err := client.Status()
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	status := statusValue.(control.ClientStatus)
	if len(status.Interfaces) != 2 {
		t.Fatalf("len(Interfaces) = %d, want 2", len(status.Interfaces))
	}
	if status.Interfaces[0].Status != "excluded" || status.Interfaces[1].Status != "idle" {
		t.Fatalf("statuses = %#v", status.Interfaces)
	}
}

func TestRefreshInterfacesCreatesAndRemovesRoutes(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1", ExcludedInterfaces: []string{"excluded"}}, "", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, errors.New("interfaces") }
	client.refreshInterfaces()

	created := make(chan struct{}, 1)
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "eth0"}, {Name: "excluded"}, {Name: "noaddr"}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string {
		switch iface.Name {
		case "eth0":
			return "127.0.0.1"
		case "excluded":
			return "127.0.0.2"
		}
		return ""
	}
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		if ifName != "eth0" {
			t.Fatalf("unexpected route creation for %s", ifName)
		}
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
		if err != nil {
			return nil, err
		}
		created <- struct{}{}
		return socket, nil
	}
	client.refreshInterfaces()
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("route was not created")
	}
	if !client.hasRoute("eth0") {
		t.Fatal("eth0 route missing")
	}

	client.listInterfaces = func() ([]net.Interface, error) { return []net.Interface{{Name: "eth0"}}, nil }
	client.interfaceAddress = func(iface net.Interface) string { return "127.0.0.2" }
	client.refreshInterfaces()
	route := client.routeSnapshot()["eth0"]
	if route == nil || route.srcAddr != "127.0.0.2" {
		t.Fatalf("eth0 route was not recreated with new address: %#v", route)
	}
}

func TestRefreshInterfacesRemovesMissingAndExcludedRoutes(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1", ExcludedInterfaces: []string{"excluded"}}, "", nil)
	missingSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	excludedSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	client.routes["missing"] = &sendRoute{ifName: "missing", srcAddr: "127.0.0.1", socket: missingSocket}
	client.routes["excluded"] = &sendRoute{ifName: "excluded", srcAddr: "127.0.0.1", socket: excludedSocket}
	client.listInterfaces = func() ([]net.Interface, error) { return []net.Interface{{Name: "excluded"}}, nil }
	client.interfaceAddress = func(iface net.Interface) string { return "127.0.0.1" }
	client.refreshInterfaces()
	if client.hasRoute("missing") || client.hasRoute("excluded") {
		t.Fatalf("routes after refresh = %#v", client.routeSnapshot())
	}
}

func TestUpdateAvailableInterfacesStopsOnContext(t *testing.T) {
	client := New(config.Client{}, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		client.updateAvailableInterfaces(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("updateAvailableInterfaces did not stop")
	}
}

func TestUpdateAvailableInterfacesTicks(t *testing.T) {
	resetClientHooks(t)
	ticks := make(chan time.Time, 1)
	stopped := make(chan struct{})
	newRefreshTicker = func() (<-chan time.Time, func()) {
		return ticks, func() { close(stopped) }
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := New(config.Client{}, "", nil)
	refreshes := make(chan struct{}, 2)
	client.listInterfaces = func() ([]net.Interface, error) {
		refreshes <- struct{}{}
		return nil, nil
	}
	done := make(chan struct{})
	go func() {
		client.updateAvailableInterfaces(ctx)
		close(done)
	}()
	<-refreshes
	ticks <- time.Now()
	<-refreshes
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("updateAvailableInterfaces did not stop")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("ticker was not stopped")
	}
}

func TestRunDescriptionListenAndControlErrors(t *testing.T) {
	resetClientHooks(t)
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		return nil, errors.New("listen")
	}
	client := New(config.Client{Description: "client", ListenAddr: "127.0.0.1:0", DstAddr: "127.0.0.1:1"}, "", nil)
	if err := client.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded after listen error")
	}

	controlCalled := make(chan struct{}, 1)
	runControl = func(ctx context.Context, listenAddr, username, password string, webFS fs.FS, status control.StatusProvider, actions control.ClientActions) error {
		controlCalled <- struct{}{}
		return errors.New("control")
	}
	if err := runControl(context.Background(), "127.0.0.1:0", "", "", nil, client, client); err == nil {
		t.Fatal("runControl hook succeeded")
	}
	select {
	case <-controlCalled:
	case <-time.After(time.Second):
		t.Fatal("control hook was not called")
	}

	wgSocket := newFakeUDPSocket()
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) { return wgSocket, nil }
	refreshStopped := make(chan struct{})
	newRefreshTicker = func() (<-chan time.Time, func()) {
		return make(chan time.Time), func() { close(refreshStopped) }
	}
	ctx, cancel := context.WithCancel(context.Background())
	client = New(config.Client{ListenAddr: "127.0.0.1:0", DstAddr: "127.0.0.1:1", WebManager: config.WebManager{ListenAddr: "127.0.0.1:0"}}, "", nil)
	client.listInterfaces = func() ([]net.Interface, error) { return nil, nil }
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	select {
	case <-controlCalled:
	case <-time.After(time.Second):
		t.Fatal("Run did not call control hook")
	}
	cancel()
	wgSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
	select {
	case <-refreshStopped:
	case <-time.After(time.Second):
		t.Fatal("interface updater did not stop")
	}
}

func TestCreateSendRouteErrorsAndDuplicate(t *testing.T) {
	client := New(config.Client{DstAddr: "bad dst"}, "", nil)
	client.createSendRoute("eth0", "127.0.0.1")
	if client.hasRoute("eth0") {
		t.Fatal("route created with bad dst")
	}

	client = New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	client.createSendRoute("eth0", "bad source")
	if client.hasRoute("eth0") {
		t.Fatal("route created with bad source")
	}

	client = New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) { return nil, errors.New("open") }
	client.createSendRoute("eth0", "127.0.0.1")
	if client.hasRoute("eth0") {
		t.Fatal("route created after socket open error")
	}

	client = New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	firstSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer firstSocket.Close()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket}
	secondSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) { return secondSocket, nil }
	client.createSendRoute("eth0", "127.0.0.1")
	if !client.hasRoute("eth0") {
		t.Fatal("existing route removed")
	}
	if _, err := secondSocket.WriteToUDP([]byte("closed"), secondSocket.LocalAddr().(*net.UDPAddr)); err == nil {
		t.Fatal("duplicate socket was not closed")
	}
}

func TestWriteBackLoopback(t *testing.T) {
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgSocket.Close()
	wgPeer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgPeer.Close()
	routeSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}

	client := New(config.Client{}, "", nil)
	client.wgSocket = wgSocket
	client.setWireGuardAddr(wgPeer.LocalAddr().(*net.UDPAddr))
	route := &sendRoute{ifName: "lo", socket: routeSocket}
	go client.writeBack(route)

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("route-to-wg"), routeSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	assertUDPRead(t, wgPeer, "route-to-wg")
	route.closing.Store(true)
	routeSocket.Close()
}

func TestWriteBackNilWireGuardAddrAndWriteError(t *testing.T) {
	client := New(config.Client{}, "", nil)
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	client.wgSocket = wgSocket
	wgPeer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgPeer.Close()
	routeSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	route := &sendRoute{ifName: "lo", socket: routeSocket}
	client.routes["lo"] = route
	go client.writeBack(route)
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("ignored"), routeSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	client.setWireGuardAddr(wgPeer.LocalAddr().(*net.UDPAddr))
	wgSocket.Close()
	if _, err := sender.WriteToUDP([]byte("write-error"), routeSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if !client.hasRoute("lo") {
		t.Fatal("route was removed after WireGuard write error")
	}
	routeSocket.Close()
}

func TestWriteBackReadErrorRemovesRoute(t *testing.T) {
	routeSocket := newFakeUDPSocket()
	routeSocket.readItems <- fakeUDPRead{err: errors.New("read")}
	client := New(config.Client{}, "", nil)
	client.wgSocket = newFakeUDPSocket()
	client.routes["lo"] = &sendRoute{ifName: "lo", socket: routeSocket}
	client.writeBack(client.routes["lo"])
	if client.hasRoute("lo") {
		t.Fatal("route was not removed after read error")
	}
}

func TestReceiveFromWireGuardWriteErrorRemovesRoute(t *testing.T) {
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgSocket.Close()
	wgPeer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgPeer.Close()
	routeSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	routeAddr := routeSocket.LocalAddr().(*net.UDPAddr)
	routeSocket.Close()
	client := New(config.Client{WriteTimeout: 10}, "", nil)
	client.wgSocket = wgSocket
	client.routes["lo"] = &sendRoute{ifName: "lo", socket: routeSocket, dstAddr: routeAddr}
	done := make(chan error, 1)
	go func() { done <- client.receiveFromWireGuard(context.Background()) }()
	if _, err := wgPeer.WriteToUDP([]byte("wg-to-route"), wgSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !client.hasRoute("lo") {
			wgSocket.Close()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("route was not removed after write error")
}

func TestReceiveFromWireGuardLoopback(t *testing.T) {
	wgSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgSocket.Close()
	wgPeer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer wgPeer.Close()
	routeSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer routeSocket.Close()
	routePeer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer routePeer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	client := New(config.Client{WriteTimeout: 10}, "", nil)
	client.wgSocket = wgSocket
	client.routes["lo"] = &sendRoute{ifName: "lo", socket: routeSocket, dstAddr: routePeer.LocalAddr().(*net.UDPAddr)}
	done := make(chan error, 1)
	go func() { done <- client.receiveFromWireGuard(ctx) }()
	if _, err := wgPeer.WriteToUDP([]byte("wg-to-route"), wgSocket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	assertUDPRead(t, routePeer, "wg-to-route")
	if client.getWireGuardAddr().String() != wgPeer.LocalAddr().String() {
		t.Fatalf("wireguard addr = %v, want %v", client.getWireGuardAddr(), wgPeer.LocalAddr())
	}
	cancel()
	wgSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveFromWireGuard returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiveFromWireGuard did not stop")
	}
}

func TestReceiveFromWireGuardReadErrorThenStop(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	wgSocket.readItems <- fakeUDPRead{err: errors.New("read")}
	client := New(config.Client{}, "", nil)
	client.wgSocket = wgSocket
	done := make(chan error, 1)
	go func() { done <- client.receiveFromWireGuard(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	wgSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiveFromWireGuard returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiveFromWireGuard did not stop")
	}
}

func TestRunRejectsInvalidAddressAndStartsControl(t *testing.T) {
	client := New(config.Client{ListenAddr: "bad listen", DstAddr: "127.0.0.1:1"}, "", nil)
	if err := client.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with invalid listen address")
	}

	resetClientHooks(t)
	controlStarted := make(chan struct{}, 1)
	controlStopped := make(chan struct{})
	runControl = func(ctx context.Context, listenAddr, username, password string, webFS fs.FS, status control.StatusProvider, actions control.ClientActions) error {
		controlStarted <- struct{}{}
		<-ctx.Done()
		close(controlStopped)
		return nil
	}
	refreshStopped := make(chan struct{})
	newRefreshTicker = func() (<-chan time.Time, func()) {
		return make(chan time.Time), func() { close(refreshStopped) }
	}
	socketReady := make(chan udpSocket, 1)
	listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
		socket, err := net.ListenUDP(network, laddr)
		if err == nil {
			socketReady <- socket
		}
		return socket, err
	}
	listenSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	listenAddr := listenSocket.LocalAddr().String()
	listenSocket.Close()
	ctx, cancel := context.WithCancel(context.Background())
	client = New(config.Client{ListenAddr: listenAddr, DstAddr: "127.0.0.1:1", WebManager: config.WebManager{ListenAddr: "127.0.0.1:0"}}, "", fstest.MapFS{"index.html": {Data: []byte("ok")}})
	client.listInterfaces = func() ([]net.Interface, error) { return nil, nil }
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	var runSocket udpSocket
	select {
	case runSocket = <-socketReady:
	case <-time.After(time.Second):
		t.Fatal("client socket was not initialized")
	}
	select {
	case <-controlStarted:
	case <-time.After(time.Second):
		t.Fatal("control server was not started")
	}
	cancel()
	runSocket.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not stop")
	}
	select {
	case <-controlStopped:
	case <-time.After(time.Second):
		t.Fatal("control server did not stop")
	}
	select {
	case <-refreshStopped:
	case <-time.After(time.Second):
		t.Fatal("interface updater did not stop")
	}
}

func waitForClientSocket(t *testing.T, client *Client) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if client.wgSocket != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client socket was not initialized")
}
