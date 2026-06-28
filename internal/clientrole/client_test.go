package clientrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"reflect"
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

func TestClientUsesConfiguredRelayQueueSize(t *testing.T) {
	client := New(config.Client{RelayQueueSize: 512}, "", nil)

	queueSize := reflect.ValueOf(client.dispatcher).Elem().FieldByName("queueSize").Int()
	if queueSize != 512 {
		t.Fatalf("dispatcher queueSize = %d, want 512", queueSize)
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
	if got := wgSocket.writeBuffer(); got != relay.DefaultWriteBufferBytes {
		t.Fatalf("initial write buffer = %d, want %d", got, relay.DefaultWriteBufferBytes)
	}

	first := newFakeUDPSocket()
	second := newFakeUDPSocket()
	client.routes["first"] = &sendRoute{ifName: "first", socket: first}
	client.routes["second"] = &sendRoute{ifName: "second", socket: second}
	client.updateWireGuardWriteBuffer()
	want := relay.DefaultWriteBufferBytes + relay.DefaultWriteBufferTargetBytes
	if got := wgSocket.writeBuffer(); got != want {
		t.Fatalf("two-route write buffer = %d, want %d", got, want)
	}

	client.removeRoute("second")
	if got := wgSocket.writeBuffer(); got != relay.DefaultWriteBufferBytes {
		t.Fatalf("one-route write buffer = %d, want %d", got, relay.DefaultWriteBufferBytes)
	}
}

func TestRemoveRouteClearsPathStats(t *testing.T) {
	client := New(config.Client{}, "", nil)
	client.wgSocket = newFakeUDPSocket()
	socket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: socket}
	client.pathStats["eth0"] = &transport.PathStats{ID: "eth0"}
	client.removeRoute("eth0")
	if _, ok := client.pathStats["eth0"]; ok {
		t.Fatal("pathStats entry was not removed")
	}
}

func TestDispatcherQueueFullDoesNotRemoveRoute(t *testing.T) {
	client := New(config.Client{}, "", nil)
	client.wgSocket = newFakeUDPSocket()
	socket := newFakeUDPSocket()
	t.Cleanup(client.dispatcher.Close)
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: socket}

	client.handleDispatcherError(relay.Result{ID: "eth0", Err: relay.ErrQueueFull, Packets: 1, Bytes: 64})

	if !client.hasRoute("eth0") {
		t.Fatal("ErrQueueFull removed route; want route retained")
	}
	traffic := client.routes["eth0"].traffic.Snapshot()
	if traffic.Data.DropPackets != 1 || traffic.Data.DropBytes != 64 {
		t.Fatalf("drop traffic = %#v, want 1 packet/64 bytes", traffic.Data)
	}
}

func TestRetryDoesNotRecreateRemovedRoutePathStats(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10}}, "", nil)
	id := client.tracker.NextID()
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: "eth0", SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	client.retryAdaptiveData(20)

	if _, ok := client.pathStats["eth0"]; ok {
		t.Fatal("removed route pathStats was recreated by retry failure accounting")
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
	route.traffic.Data.RecordRX(120)
	route.traffic.Data.RecordTX(240)
	route.traffic.Control.RecordRX(36)
	pathNow := transport.NowMillis()
	client.pathStats[ifName] = &transport.PathStats{ID: ifName, LastSeen: pathNow - 50, LastSuccess: pathNow - 25, SmoothedRTT: 12, RTTVariance: 3, Failures: 1}
	client.selection = transport.PathSelection{FirstPathIDs: []string{ifName}}
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
		if webInterface.Status != "active" || webInterface.DstAddress != "198.51.100.1:59501" || webInterface.Last == nil || webInterface.PathRole != transport.PathRoleFirst {
			t.Fatalf("interface status = %#v", webInterface)
		}
		if webInterface.Traffic.Data.RXPackets != 1 || webInterface.Traffic.Data.RXBytes != 120 || webInterface.Traffic.Data.TXPackets != 1 || webInterface.Traffic.Data.TXBytes != 240 {
			t.Fatalf("data traffic = %#v", webInterface.Traffic.Data)
		}
		if webInterface.Traffic.Control.RXPackets != 1 || webInterface.Traffic.Control.RXBytes != 36 {
			t.Fatalf("control traffic = %#v", webInterface.Traffic.Control)
		}
		if webInterface.Path == nil || webInterface.Path.SmoothedRTTMillis != 12 || webInterface.Path.Failures != 1 || webInterface.Path.LastSeen == nil || webInterface.Path.LastSuccess == nil {
			t.Fatalf("path status = %#v", webInterface.Path)
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

func TestRefreshInterfacesRecreatesRouteWhenIndexChanges(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	oldSocket := newFakeUDPSocket()
	client.routes["tun0"] = &sendRoute{ifName: "tun0", ifIndex: 1, srcAddr: "198.18.0.1", socket: oldSocket}
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 2}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		if ifName != "tun0" {
			t.Fatalf("unexpected route creation for %s", ifName)
		}
		return newFakeUDPSocket(), nil
	}

	client.refreshInterfaces()

	select {
	case <-oldSocket.closed:
	default:
		t.Fatal("old route socket was not closed")
	}
	route := client.routeSnapshot()["tun0"]
	if route == nil || route.ifIndex != 2 || route.srcAddr != "198.18.0.1" {
		t.Fatalf("tun0 route was not recreated with new index: %#v", route)
	}
}

func TestRefreshInterfacesRecreatesStaleRoute(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	oldSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: oldSocket}
	now := time.Now().Unix()
	route.lastSent.Store(now)
	route.staleSince.Store(now - routeReceiveStaleSeconds - 1)
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		return newFakeUDPSocket(), nil
	}

	client.refreshInterfaces()

	select {
	case <-oldSocket.closed:
	default:
		t.Fatal("stale route socket was not closed")
	}
	if newRoute := client.routeSnapshot()["tun0"]; newRoute == nil || newRoute == route {
		t.Fatalf("stale route was not recreated: %#v", newRoute)
	}
}

func TestRefreshInterfacesKeepsRouteWithoutOutboundActivity(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	socket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: socket}
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		t.Fatalf("unexpected route creation for %s", ifName)
		return nil, nil
	}

	client.refreshInterfaces()

	if client.routeSnapshot()["tun0"] != route {
		t.Fatal("route without outbound activity was recreated")
	}
	select {
	case <-socket.closed:
		t.Fatal("route without outbound activity was closed")
	default:
	}
}

func TestRefreshInterfacesKeepsRouteWithCurrentReceive(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	socket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: socket}
	now := time.Now().Unix()
	route.lastSent.Store(now)
	route.lastRec.Store(now)
	route.staleSince.Store(now - routeReceiveStaleSeconds - 1)
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		t.Fatalf("unexpected route creation for %s", ifName)
		return nil, nil
	}

	client.refreshInterfaces()

	if client.routeSnapshot()["tun0"] != route {
		t.Fatal("route with current receive activity was recreated")
	}
	select {
	case <-socket.closed:
		t.Fatal("route with current receive activity was closed")
	default:
	}
}

func TestRefreshInterfacesRecreatesRouteAfterDirectReceiveTimeout(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1", Transfer: config.Transfer{Mode: config.TransferModeDirect, DirectReceiveTimeout: 10}}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	oldSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: oldSocket}
	now := time.Now().Unix()
	route.lastRec.Store(now - 11)
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		if ifName != "tun0" {
			t.Fatalf("unexpected route creation for %s", ifName)
		}
		return newFakeUDPSocket(), nil
	}

	client.refreshInterfaces()

	select {
	case <-oldSocket.closed:
	default:
		t.Fatal("timed-out route socket was not closed")
	}
	if newRoute := client.routeSnapshot()["tun0"]; newRoute == nil || newRoute == route {
		t.Fatalf("route timeout was not recreated: %#v", newRoute)
	}
}

func TestRefreshInterfacesRecreatesOutboundActiveRouteWithoutReceive(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1", Transfer: config.Transfer{Mode: config.TransferModeDirect, DirectReceiveTimeout: 10}}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	oldSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: oldSocket}
	now := time.Now().Unix()
	route.lastSent.Store(now - 11)
	route.staleSince.Store(now - 11)
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		if ifName != "tun0" {
			t.Fatalf("unexpected route creation for %s", ifName)
		}
		return newFakeUDPSocket(), nil
	}

	client.refreshInterfaces()

	select {
	case <-oldSocket.closed:
	default:
		t.Fatal("outbound-active route without receive was not closed")
	}
	if newRoute := client.routeSnapshot()["tun0"]; newRoute == nil || newRoute == route {
		t.Fatalf("route timeout was not recreated: %#v", newRoute)
	}
}

func TestRefreshInterfacesKeepsIdleRouteWithoutReceive(t *testing.T) {
	client := New(config.Client{DstAddr: "127.0.0.1:1", Transfer: config.Transfer{Mode: config.TransferModeDirect, DirectReceiveTimeout: 10}}, "", nil)
	t.Cleanup(client.closeAllRoutes)
	socket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", ifIndex: 3, srcAddr: "198.18.0.1", socket: socket}
	client.routes["tun0"] = route
	client.listInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "tun0", Index: 3}}, nil
	}
	client.interfaceAddress = func(iface net.Interface) string { return "198.18.0.1" }
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
		t.Fatalf("unexpected route creation for %s", ifName)
		return nil, nil
	}

	client.refreshInterfaces()

	if client.routeSnapshot()["tun0"] != route {
		t.Fatal("idle never-received route was recreated")
	}
	select {
	case <-socket.closed:
		t.Fatal("idle never-received route was closed")
	default:
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
	client.createSendRoute("eth0", 1, "127.0.0.1")
	if client.hasRoute("eth0") {
		t.Fatal("route created with bad dst")
	}

	client = New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	client.createSendRoute("eth0", 1, "bad source")
	if client.hasRoute("eth0") {
		t.Fatal("route created with bad source")
	}

	client = New(config.Client{DstAddr: "127.0.0.1:1"}, "", nil)
	client.openUDPOnInterface = func(addr *net.UDPAddr, ifName string) (udpSocket, error) { return nil, errors.New("open") }
	client.createSendRoute("eth0", 1, "127.0.0.1")
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
	client.createSendRoute("eth0", 1, "127.0.0.1")
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
	traffic := route.traffic.Snapshot()
	if traffic.Data.RXPackets != 1 || traffic.Data.RXBytes != uint64(len("route-to-wg")) {
		t.Fatalf("route data RX traffic = %#v", traffic.Data)
	}
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

func TestWriteBackAdaptiveDataAckAndDuplicateSuppression(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}}, "", nil)
	client.wgSocket = wgSocket
	client.setWireGuardAddr(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000})
	routeSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", socket: routeSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["tun0"] = route
	framePayload, err := transport.Encode(transport.Frame{Type: transport.FrameData, ID: transport.PacketID{Session: 1, Sequence: 1}, SentAt: transport.NowMillis(), Payload: []byte("inner")})
	if err != nil {
		t.Fatal(err)
	}
	routeSocket.readItems <- fakeUDPRead{payload: framePayload}
	routeSocket.readItems <- fakeUDPRead{payload: framePayload}
	go client.writeBack(route)
	time.Sleep(20 * time.Millisecond)
	route.closing.Store(true)
	routeSocket.Close()

	wgPayloads := wgSocket.writtenSnapshot()
	if len(wgPayloads) != 1 || string(wgPayloads[0]) != "inner" {
		t.Fatalf("WireGuard writes = %#v", wgPayloads)
	}
	if routeSocket.writtenCount() < 2 {
		t.Fatalf("ACK writes = %d, want at least 2", routeSocket.writtenCount())
	}
	traffic := route.traffic.Snapshot()
	if traffic.Data.RXPackets != 1 || traffic.Data.RXBytes != uint64(len("inner")) {
		t.Fatalf("adaptive data RX traffic = %#v", traffic.Data)
	}
	if traffic.Control.TXPackets != 2 {
		t.Fatalf("adaptive control TX traffic = %#v", traffic.Control)
	}
}

func TestWriteBackAdaptiveAckCompletesPendingWithoutWireGuardWrite(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}}, "", nil)
	client.wgSocket = wgSocket
	routeSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", socket: routeSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	id := client.tracker.NextID()
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: "tun0", SentAt: transport.NowMillis(), Payload: []byte("tracked")})
	ackPayload, err := transport.Encode(transport.Frame{Type: transport.FrameAck, ID: id, SentAt: transport.NowMillis()})
	if err != nil {
		t.Fatal(err)
	}
	routeSocket.readItems <- fakeUDPRead{payload: ackPayload}
	go client.writeBack(route)
	time.Sleep(20 * time.Millisecond)
	route.closing.Store(true)
	routeSocket.Close()

	if wgPayloads := wgSocket.writtenSnapshot(); len(wgPayloads) != 0 {
		t.Fatalf("ACK was written to WireGuard: %#v", wgPayloads)
	}
	if _, ok := client.tracker.Complete(id); ok {
		t.Fatal("pending record was not completed by ACK")
	}
}

func TestClientAdaptiveKeepaliveTracksPending(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}}, "", nil)
	routeSocket := newFakeUDPSocket()
	client.routes["tun0"] = &sendRoute{ifName: "tun0", socket: routeSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.sendKeepaliveToRoutes(100)
	time.Sleep(20 * time.Millisecond)
	if routeSocket.writtenCount() != 1 {
		t.Fatalf("keepalive writes = %d, want 1", routeSocket.writtenCount())
	}
	due := client.tracker.Due(200, 50, 1000, 1)
	if len(due) != 1 || due[0].PathID != "tun0" {
		t.Fatalf("due keepalive = %#v", due)
	}
}

func TestClientAdaptiveKeepaliveAckCompletesPending(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive}}, "", nil)
	routeSocket := newFakeUDPSocket()
	route := &sendRoute{ifName: "tun0", socket: routeSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["tun0"] = route
	client.sendKeepaliveToRoutes(100)
	time.Sleep(20 * time.Millisecond)
	payloads := routeSocket.writtenSnapshot()
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
	client.writeBackAdaptive(route, []udp.Packet{{Payload: ackPayload}}, nil)
	if _, ok := client.tracker.Complete(frame.ID); ok {
		t.Fatal("keepalive pending record was not completed by ACK")
	}
	if due := client.tracker.Due(200, 50, 1000, 1); len(due) != 0 {
		t.Fatalf("ACKed keepalive was retried: %#v", due)
	}
	controlTraffic := route.traffic.Snapshot().Control
	if controlTraffic.TXPackets != 1 || controlTraffic.RXPackets != 1 {
		t.Fatalf("control traffic = %#v", controlTraffic)
	}
}

func TestClientAdaptiveDataUsesCachedFirstPaths(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	firstSocket := newFakeUDPSocket()
	secondSocket := newFakeUDPSocket()
	thirdSocket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["eth1"] = &sendRoute{ifName: "eth1", socket: secondSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9002}}
	client.routes["eth2"] = &sendRoute{ifName: "eth2", socket: thirdSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9003}}
	now := transport.NowMillis()
	client.pathStats["eth0"] = &transport.PathStats{ID: "eth0", LastSuccess: now, SmoothedRTT: 100}
	client.pathStats["eth1"] = &transport.PathStats{ID: "eth1", LastSuccess: now, SmoothedRTT: 90}
	client.pathStats["eth2"] = &transport.PathStats{ID: "eth2", LastSuccess: now, SmoothedRTT: 80}
	client.selection = transport.PathSelection{FirstPathIDs: []string{"eth0", "eth1"}, FallbackPathIDs: []string{"eth2"}}

	client.sendAdaptiveData([]byte("inner"))
	time.Sleep(20 * time.Millisecond)

	if firstSocket.writtenCount() != 1 {
		t.Fatalf("eth0 writes = %d, want 1", firstSocket.writtenCount())
	}
	if secondSocket.writtenCount() != 1 {
		t.Fatalf("eth1 writes = %d, want 1", secondSocket.writtenCount())
	}
	if thirdSocket.writtenCount() != 0 {
		t.Fatalf("eth2 writes = %d, want 0", thirdSocket.writtenCount())
	}
}

func TestClientRefreshPathSelectionRequiresSignificantImprovement(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: newFakeUDPSocket(), dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["eth1"] = &sendRoute{ifName: "eth1", socket: newFakeUDPSocket(), dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9002}}
	now := transport.NowMillis()
	client.pathStats["eth0"] = &transport.PathStats{ID: "eth0", LastSuccess: now, SmoothedRTT: 100}
	client.pathStats["eth1"] = &transport.PathStats{ID: "eth1", LastSuccess: now, SmoothedRTT: 90}
	client.selection = transport.PathSelection{FirstPathIDs: []string{"eth0"}}

	client.refreshPathSelection(now)
	if got := client.selection.FirstPathIDs; len(got) != 1 || got[0] != "eth0" {
		t.Fatalf("first paths after small RTT change = %#v, want eth0", got)
	}

	client.pathStats["eth1"].SmoothedRTT = 70
	client.refreshPathSelection(now)
	if got := client.selection.FirstPathIDs; len(got) != 1 || got[0] != "eth1" {
		t.Fatalf("first paths after significant RTT change = %#v, want eth1", got)
	}
}

func TestClientRetryUsesOnlyUnsentFallbackPath(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}}, "", nil)
	firstSocket := newFakeUDPSocket()
	secondSocket := newFakeUDPSocket()
	thirdSocket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["eth1"] = &sendRoute{ifName: "eth1", socket: secondSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9002}}
	client.routes["eth2"] = &sendRoute{ifName: "eth2", socket: thirdSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9003}}
	now := int64(100)
	for _, id := range []string{"eth0", "eth1", "eth2"} {
		client.pathStats[id] = &transport.PathStats{ID: id, LastSuccess: now, SmoothedRTT: 50}
	}
	id := client.tracker.NextID()
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: "eth0", PathIDs: []string{"eth0", "eth1"}, AttemptPathIDs: []string{"eth0", "eth1"}, FallbackPathIDs: []string{"eth2"}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	client.retryAdaptiveData(now)
	time.Sleep(20 * time.Millisecond)

	if firstSocket.writtenCount() != 0 || secondSocket.writtenCount() != 0 {
		t.Fatalf("first path retry writes = eth0:%d eth1:%d, want 0", firstSocket.writtenCount(), secondSocket.writtenCount())
	}
	if thirdSocket.writtenCount() != 1 {
		t.Fatalf("fallback writes = %d, want 1", thirdSocket.writtenCount())
	}
	record, ok := client.tracker.Get(id)
	if !ok {
		t.Fatal("pending record missing")
	}
	if got, want := record.PathIDs, []string{"eth0", "eth1", "eth2"}; !sameStrings(got, want) {
		t.Fatalf("PathIDs = %#v, want %#v", got, want)
	}
	if got, want := record.AttemptPathIDs, []string{"eth2"}; !sameStrings(got, want) {
		t.Fatalf("AttemptPathIDs = %#v, want %#v", got, want)
	}
}

func TestClientRetryDropsWhenNoFallbackPath(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}}, "", nil)
	firstSocket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.pathStats["eth0"] = &transport.PathStats{ID: "eth0", LastSuccess: 100, SmoothedRTT: 50}
	client.selection = transport.PathSelection{FirstPathIDs: []string{"eth0"}}
	id := client.tracker.NextID()
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: "eth0", PathIDs: []string{"eth0"}, AttemptPathIDs: []string{"eth0"}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	client.retryAdaptiveData(100)
	time.Sleep(20 * time.Millisecond)

	if firstSocket.writtenCount() != 0 {
		t.Fatalf("first path retry writes = %d, want 0", firstSocket.writtenCount())
	}
	if _, ok := client.tracker.Get(id); ok {
		t.Fatal("pending record was not dropped")
	}
}

func TestClientRetryUsesCurrentFallbackPath(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000, MaxRetries: intPtr(2)}}, "", nil)
	firstSocket := newFakeUDPSocket()
	secondSocket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["eth1"] = &sendRoute{ifName: "eth1", socket: secondSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9002}}
	for _, id := range []string{"eth0", "eth1"} {
		client.pathStats[id] = &transport.PathStats{ID: id, LastSuccess: 100, SmoothedRTT: 50}
	}
	client.selection = transport.PathSelection{FirstPathIDs: []string{"eth0"}, FallbackPathIDs: []string{"eth1"}}
	id := client.tracker.NextID()
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: "eth0", PathIDs: []string{"eth0"}, AttemptPathIDs: []string{"eth0"}, SentAt: 0, TimeoutMillis: 10, Payload: []byte("payload")})

	client.retryAdaptiveData(100)
	time.Sleep(20 * time.Millisecond)

	if firstSocket.writtenCount() != 0 {
		t.Fatalf("first path retry writes = %d, want 0", firstSocket.writtenCount())
	}
	if secondSocket.writtenCount() != 1 {
		t.Fatalf("current fallback writes = %d, want 1", secondSocket.writtenCount())
	}
}

func TestClientAdaptiveDataTracksMaxFirstPathRTO(t *testing.T) {
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, AckTimeoutMillis: 10, KeepaliveTimeoutMillis: 1000}}, "", nil)
	firstSocket := newFakeUDPSocket()
	secondSocket := newFakeUDPSocket()
	client.routes["eth0"] = &sendRoute{ifName: "eth0", socket: firstSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9001}}
	client.routes["eth1"] = &sendRoute{ifName: "eth1", socket: secondSocket, dstAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9002}}
	now := transport.NowMillis()
	client.pathStats["eth0"] = &transport.PathStats{ID: "eth0", LastSuccess: now, SmoothedRTT: 20, RTTVariance: 5}
	client.pathStats["eth1"] = &transport.PathStats{ID: "eth1", LastSuccess: now, SmoothedRTT: 120, RTTVariance: 10}
	client.selection = transport.PathSelection{FirstPathIDs: []string{"eth0", "eth1"}}

	client.sendAdaptiveData([]byte("inner"))
	time.Sleep(20 * time.Millisecond)

	payloads := firstSocket.writtenSnapshot()
	if len(payloads) != 1 {
		t.Fatalf("eth0 writes = %d, want 1", len(payloads))
	}
	frame, err := transport.Decode(payloads[0])
	if err != nil {
		t.Fatal(err)
	}
	record, ok := client.tracker.Get(frame.ID)
	if !ok {
		t.Fatal("pending record missing")
	}
	if got, want := record.TimeoutMillis, int64(160); got != want {
		t.Fatalf("TimeoutMillis = %d, want %d", got, want)
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

func TestWriteBackAdaptiveInvalidFrameFallbackDependsOnConfirmation(t *testing.T) {
	invalidFrame := make([]byte, transport.HeaderSize)
	copy(invalidFrame, []byte{0x45, 0x47, 0x41, 0x44})
	wgSocket := newFakeUDPSocket()
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	client.wgSocket = wgSocket
	client.setWireGuardAddr(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000})
	route := &sendRoute{ifName: "tun0"}
	client.writeBackAdaptive(route, []udp.Packet{{Payload: invalidFrame}}, nil)
	if wgSocket.writtenCount() != 1 {
		t.Fatalf("unconfirmed invalid frame writes = %d, want 1", wgSocket.writtenCount())
	}

	client.markPathSuccess("tun0", transport.NowMillis(), 10)
	client.writeBackAdaptive(route, []udp.Packet{{Payload: invalidFrame}}, nil)
	if wgSocket.writtenCount() != 1 {
		t.Fatalf("confirmed invalid frame writes = %d, want still 1", wgSocket.writtenCount())
	}
}

func TestWriteBackAdaptiveAllowsOversizedRawOnConfirmedPath(t *testing.T) {
	wgSocket := newFakeUDPSocket()
	client := New(config.Client{Transfer: config.Transfer{Mode: config.TransferModeAdaptive, KeepaliveTimeoutMillis: 1000}}, "", nil)
	client.wgSocket = wgSocket
	client.setWireGuardAddr(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000})
	client.markPathSuccess("tun0", transport.NowMillis(), 10)
	rawPayload := make([]byte, transport.MaxPayloadSize+1)
	route := &sendRoute{ifName: "tun0"}

	client.writeBackAdaptive(route, []udp.Packet{{Payload: rawPayload}}, nil)

	wgPayloads := wgSocket.writtenSnapshot()
	if len(wgPayloads) != 1 || len(wgPayloads[0]) != len(rawPayload) {
		t.Fatalf("WireGuard writes = %#v, want oversized raw payload", wgPayloads)
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
