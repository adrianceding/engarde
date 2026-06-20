package clientrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	pathmgr "github.com/adrianceding/engarde/internal/path"
	"github.com/adrianceding/engarde/internal/relay"
	"github.com/adrianceding/engarde/internal/stats"
	"github.com/adrianceding/engarde/internal/transport"
	"github.com/adrianceding/engarde/internal/udp"
	log "github.com/sirupsen/logrus"
)

var runControl = control.Run
var resolveUDPAddr = net.ResolveUDPAddr
var listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
	conn, err := net.ListenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	return udp.Wrap(conn), nil
}
var newRefreshTicker = defaultRefreshTicker

const (
	routeReceiveStaleSeconds   int64 = 60
	routeOutboundActiveSeconds int64 = 10
)

type udpSocket interface {
	relay.UDPWriter
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	Close() error
}

func defaultRefreshTicker() (<-chan time.Time, func()) {
	ticker := time.NewTicker(time.Second)
	return ticker.C, ticker.Stop
}

type Client struct {
	cfg     config.Client
	version string
	webFS   fs.FS
	paths   *pathmgr.Manager

	listInterfaces     func() ([]net.Interface, error)
	interfaceAddress   func(net.Interface) string
	openUDPOnInterface func(*net.UDPAddr, string) (udpSocket, error)

	wgSocket udpSocket

	wgAddrMu sync.RWMutex
	wgAddr   *net.UDPAddr

	routesMu sync.RWMutex
	routes   map[string]*sendRoute

	dispatcher *relay.Dispatcher
	tracker    *transport.Tracker

	pathStatsMu   sync.Mutex
	pathStats     map[string]*transport.PathStats
	primaryPathID string
	lastKeepalive atomic.Int64
}

type sendRoute struct {
	ifName     string
	ifIndex    int
	srcAddr    string
	dstAddr    *net.UDPAddr
	socket     udpSocket
	lastRec    atomic.Int64
	lastSent   atomic.Int64
	staleSince atomic.Int64
	closing    atomic.Bool
	traffic    stats.Traffic
}

func (route *sendRoute) markSent(now int64) {
	previousSent := route.lastSent.Swap(now)
	if route.staleSince.Load() == 0 || now-previousSent > routeOutboundActiveSeconds {
		route.staleSince.Store(now)
	}
}

func (route *sendRoute) markReceived(now int64) {
	route.lastRec.Store(now)
	route.staleSince.Store(0)
}

func (route *sendRoute) isReceiveStale(now int64) bool {
	lastSent := route.lastSent.Load()
	if lastSent == 0 || now-lastSent > routeOutboundActiveSeconds {
		return false
	}
	staleSince := route.staleSince.Load()
	if staleSince == 0 || route.lastRec.Load() >= staleSince {
		return false
	}
	return now-staleSince >= routeReceiveStaleSeconds
}

func New(cfg config.Client, version string, webFS fs.FS) *Client {
	cfg.Transfer.ApplyDefaults()
	client := &Client{
		cfg:              cfg,
		version:          version,
		webFS:            webFS,
		paths:            pathmgr.NewManager(cfg),
		routes:           make(map[string]*sendRoute),
		listInterfaces:   net.Interfaces,
		interfaceAddress: pathmgr.AddressByInterface,
		openUDPOnInterface: func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
			conn, err := udp.OpenUDPOnInterface(addr, ifName)
			if err != nil {
				return nil, err
			}
			return udp.Wrap(conn), nil
		},
		tracker:   transport.NewTracker(cfg.Transfer.PendingWindow, cfg.Transfer.DuplicateWindow),
		pathStats: make(map[string]*transport.PathStats),
	}
	client.dispatcher = relay.NewDispatcherWithBatch(cfg.WriteTimeout, relay.DefaultQueueSize, cfg.UDPBatch.IsEnabled(), cfg.UDPBatch.EffectiveWriteSize(), func(result relay.Result) {
		log.WithError(result.Err).Warn("Error writing to '" + result.ID + "', re-creating socket")
		client.removeRoute(result.ID)
	})
	return client
}

func (client *Client) Run(ctx context.Context) error {
	if client.cfg.Description != "" {
		log.Info(client.cfg.Description)
	}

	listenAddr, err := resolveUDPAddr("udp4", client.cfg.ListenAddr)
	if err != nil {
		return err
	}
	wgSocket, err := listenUDP("udp", listenAddr)
	if err != nil {
		return err
	}
	client.wgSocket = wgSocket
	client.updateWireGuardWriteBuffer()
	log.Info("Listening on " + client.cfg.ListenAddr)

	go client.closeOnCancel(ctx)
	if client.cfg.WebManager.ListenAddr != "" {
		go func() {
			if err := runControl(ctx, client.cfg.WebManager.ListenAddr, client.cfg.WebManager.Username, client.cfg.WebManager.Password, client.webFS, client, client); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	go client.updateAvailableInterfaces(ctx)
	if client.adaptiveEnabled() {
		go client.updateAdaptiveTransport(ctx)
	}
	return client.receiveFromWireGuard(ctx)
}

func (client *Client) Status() (any, error) {
	interfaces, err := client.listInterfaces()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	routes := client.routeSnapshot()
	primaryPathID := client.currentPrimaryPathID()
	webInterfaces := make([]control.WebInterface, 0, len(interfaces))
	for _, iface := range interfaces {
		ifName := iface.Name
		address := client.interfaceAddress(iface)
		webInterface := control.WebInterface{
			Name:          ifName,
			Label:         client.paths.Label(ifName),
			SenderAddress: address,
		}
		if client.paths.IsExcluded(ifName) {
			webInterface.Status = "excluded"
			webInterfaces = append(webInterfaces, webInterface)
			continue
		}
		webInterface.DstAddress = client.paths.Destination(ifName)
		if route, ok := routes[ifName]; ok {
			webInterface.Status = "active"
			webInterface.Primary = ifName == primaryPathID
			webInterface.Traffic = route.traffic.Snapshot()
			webInterface.Path = client.pathStatus(ifName, transport.NowMillis())
			lastRec := route.lastRec.Load()
			if lastRec > 0 {
				last := now - lastRec
				webInterface.Last = &last
			}
		} else {
			webInterface.Status = "idle"
		}
		webInterfaces = append(webInterfaces, webInterface)
	}
	return control.ClientStatus{
		Type:          "client",
		Version:       client.version,
		Description:   client.cfg.Description,
		ListenAddress: client.cfg.ListenAddr,
		Interfaces:    webInterfaces,
	}, nil
}

func (client *Client) ToggleOverride(ifName string) string {
	return client.paths.ToggleExclusion(ifName)
}

func (client *Client) Include(ifName string) string {
	return client.paths.Include(ifName)
}

func (client *Client) Exclude(ifName string) string {
	return client.paths.Exclude(ifName)
}

func (client *Client) ResetExclusions() string {
	return client.paths.ResetExclusions()
}

func (client *Client) closeOnCancel(ctx context.Context) {
	<-ctx.Done()
	if client.wgSocket != nil {
		client.wgSocket.Close()
	}
	client.closeAllRoutes()
}

func (client *Client) updateAvailableInterfaces(ctx context.Context) {
	ticks, stopTicker := newRefreshTicker()
	defer stopTicker()
	for {
		client.refreshInterfaces()
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}
	}
}

func (client *Client) refreshInterfaces() {
	interfaces, err := client.listInterfaces()
	if err != nil {
		return
	}
	now := time.Now().Unix()
	known := make(map[string]net.Interface, len(interfaces))
	for _, iface := range interfaces {
		known[iface.Name] = iface
	}

	for ifName, route := range client.routeSnapshot() {
		iface, ok := known[ifName]
		if !ok {
			log.Info("Interface '" + ifName + "' no longer exists, deleting it")
			client.removeRoute(ifName)
			continue
		}
		if client.paths.IsExcluded(ifName) {
			log.Info("Interface '" + ifName + "' is now excluded, deleting it")
			client.removeRoute(ifName)
			continue
		}
		if route.ifIndex != 0 && iface.Index != route.ifIndex {
			log.Info("Interface '" + ifName + "' changed index, re-creating socket")
			client.removeRoute(ifName)
			continue
		}
		if address := client.interfaceAddress(iface); address != route.srcAddr {
			log.Info("Interface '" + ifName + "' changed address, re-creating socket")
			client.removeRoute(ifName)
			continue
		}
		if route.isReceiveStale(now) {
			log.Info("Interface '" + ifName + "' stopped receiving packets, re-creating socket")
			client.removeRoute(ifName)
		}
	}

	for _, iface := range interfaces {
		ifName := iface.Name
		if client.paths.IsExcluded(ifName) || client.hasRoute(ifName) {
			continue
		}
		address := client.interfaceAddress(iface)
		if address == "" {
			continue
		}
		log.Info("New interface '" + ifName + "' with IP '" + address + "', adding it")
		client.createSendRoute(ifName, iface.Index, address)
	}
}

func (client *Client) createSendRoute(ifName string, ifIndex int, sourceAddr string) {
	dst := client.paths.Destination(ifName)
	dstAddr, err := resolveUDPAddr("udp4", dst)
	if err != nil {
		log.WithError(err).Error("Can't resolve destination address '" + dst + "' for interface '" + ifName + "', not using it")
		return
	}
	srcAddr, err := resolveUDPAddr("udp4", sourceAddr+":0")
	if err != nil {
		log.WithError(err).Error("Can't resolve source address '" + sourceAddr + "' for interface '" + ifName + "', not using it")
		return
	}
	socket, err := client.openUDPOnInterface(srcAddr, ifName)
	if err != nil {
		log.WithError(err).Error("Can't create socket for address '" + sourceAddr + "' on interface '" + ifName + "', not using it")
		return
	}

	route := &sendRoute{ifName: ifName, ifIndex: ifIndex, srcAddr: sourceAddr, dstAddr: dstAddr, socket: socket}
	client.routesMu.Lock()
	if _, exists := client.routes[ifName]; exists {
		client.routesMu.Unlock()
		socket.Close()
		return
	}
	client.routes[ifName] = route
	client.routesMu.Unlock()
	client.updateWireGuardWriteBuffer()
	go client.writeBack(route)
}

func (client *Client) writeBack(route *sendRoute) {
	readBatch := udp.NewReadBatch(client.cfg.UDPBatch.EffectiveReadSize())
	writeBatch := make([]udp.Packet, 0, client.cfg.UDPBatch.EffectiveWriteSize())
	for {
		n, err := udp.ReadBatch(route.socket, readBatch, client.cfg.UDPBatch.IsEnabled())
		if route.closing.Load() || errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil && n == 0 {
			log.WithError(err).Warn("Error reading from '" + route.ifName + "', re-creating socket")
			client.removeRoute(route.ifName)
			return
		}

		writeBatch = writeBatch[:0]
		if n > 0 {
			route.markReceived(time.Now().Unix())
		}
		if client.adaptiveEnabled() {
			client.writeBackAdaptive(route, readBatch[:n], writeBatch)
			if err != nil {
				log.WithError(err).Warn("Error reading from '" + route.ifName + "', re-creating socket")
				client.removeRoute(route.ifName)
				return
			}
			continue
		}
		wgAddr := client.getWireGuardAddr()
		if wgAddr != nil {
			for i := 0; i < n; i++ {
				route.traffic.Data.RecordRX(len(readBatch[i].Payload))
				writeBatch = append(writeBatch, udp.Packet{Payload: readBatch[i].Payload, Addr: wgAddr})
			}
		}
		if _, err := udp.WriteBatchChunks(client.wgSocket, writeBatch, client.cfg.UDPBatch.IsEnabled(), client.cfg.UDPBatch.EffectiveWriteSize()); err != nil {
			log.WithError(err).Warn("Error writing to WireGuard")
		}
		if err != nil {
			log.WithError(err).Warn("Error reading from '" + route.ifName + "', re-creating socket")
			client.removeRoute(route.ifName)
			return
		}
	}
}

func (client *Client) receiveFromWireGuard(ctx context.Context) error {
	readBatch := udp.NewReadBatch(client.cfg.UDPBatch.EffectiveReadSize())
	payloads := make([][]byte, 0, client.cfg.UDPBatch.EffectiveReadSize())
	for {
		n, err := udp.ReadBatch(client.wgSocket, readBatch, client.cfg.UDPBatch.IsEnabled())
		if err != nil && n == 0 {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from WireGuard")
			continue
		}
		payloads = payloads[:0]
		var wgAddr *net.UDPAddr
		for i := 0; i < n; i++ {
			if readBatch[i].Addr != nil {
				wgAddr = readBatch[i].Addr
			}
			payloads = append(payloads, readBatch[i].Payload)
		}
		if wgAddr != nil {
			client.setWireGuardAddr(wgAddr)
		}
		if len(payloads) > 0 {
			if client.adaptiveEnabled() {
				client.sendAdaptiveDataBatch(payloads)
			} else {
				targets := client.routeTargets()
				client.recordDataTXBatch(targets, payloads)
				client.dispatcher.FanoutBatch(payloads, targets)
			}
		}
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from WireGuard")
		}
	}
}

func (client *Client) setWireGuardAddr(addr *net.UDPAddr) {
	client.wgAddrMu.Lock()
	client.wgAddr = addr
	client.wgAddrMu.Unlock()
}

func (client *Client) getWireGuardAddr() *net.UDPAddr {
	client.wgAddrMu.RLock()
	defer client.wgAddrMu.RUnlock()
	return client.wgAddr
}

func (client *Client) adaptiveEnabled() bool {
	return client.cfg.Transfer.IsAdaptive() && client.tracker != nil
}

func (client *Client) updateAdaptiveTransport(ctx context.Context) {
	interval := time.Duration(client.minAckTimeoutMillis()/2) * time.Millisecond
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			client.maintainAdaptiveTransport()
		}
	}
}

func (client *Client) maintainAdaptiveTransport() {
	if !client.adaptiveEnabled() {
		return
	}
	now := transport.NowMillis()
	client.refreshPrimaryPath(now)
	client.retryAdaptiveData(now)
	lastKeepalive := client.lastKeepalive.Load()
	if now-lastKeepalive >= client.cfg.Transfer.KeepaliveIntervalMillis && client.lastKeepalive.CompareAndSwap(lastKeepalive, now) {
		client.sendKeepaliveToRoutes(now)
	}
}

func (client *Client) sendAdaptiveDataBatch(payloads [][]byte) {
	for _, payload := range payloads {
		client.sendAdaptiveData(payload)
	}
}

func (client *Client) sendAdaptiveData(payload []byte) {
	if len(payload) > transport.MaxPayloadSize {
		now := transport.NowMillis()
		client.sendKeepaliveToRoutes(now)
		targets := client.routeTargets()
		client.recordDataTX(targets, len(payload))
		client.dispatcher.Fanout(payload, targets)
		return
	}
	now := transport.NowMillis()
	target, ok := client.primaryRouteTarget(now)
	if !ok {
		client.refreshPrimaryPath(now)
		target, ok = client.primaryRouteTarget(now)
	}
	if !ok {
		client.sendKeepaliveToRoutes(now)
		fallbackTargets := client.routeTargets()
		client.recordDataTX(fallbackTargets, len(payload))
		client.dispatcher.Fanout(payload, fallbackTargets)
		return
	}
	id := client.tracker.NextID()
	framePayload, err := transport.Encode(transport.Frame{Type: transport.FrameData, ID: id, SentAt: now, Payload: payload})
	if err != nil {
		return
	}
	client.tracker.Track(transport.PendingRecord{ID: id, PathID: target.ID, SentAt: now, TimeoutMillis: client.pathRTO(target.ID), Payload: framePayload})
	client.markRouteSent(target.ID)
	client.recordDataTX([]relay.Target{target}, len(payload))
	client.dispatcher.Send(framePayload, target)
}

func (client *Client) retryAdaptiveData(now int64) {
	due := client.tracker.Due(now, client.minAckTimeoutMillis(), client.maxAckTimeoutMillis(), client.cfg.Transfer.MaxRetriesValue())
	if len(due) == 0 {
		return
	}
	targets := client.adaptiveRouteTargets(now, true)
	if len(targets) == 0 {
		targets = client.adaptiveRouteTargets(now, false)
	}
	for _, record := range due {
		for _, pathID := range record.PathIDs {
			client.markPathFailure(pathID, now)
		}
		pathIDs := make([]string, 0, len(targets))
		for _, target := range targets {
			pathIDs = append(pathIDs, target.ID)
			client.markRouteSent(target.ID)
			client.recordDataTX([]relay.Target{target}, stats.AdaptiveDataPayloadSize(record.Payload))
			client.dispatcher.Send(record.Payload, target)
		}
		client.tracker.UpdatePaths(record.ID, pathIDs)
	}
}

func (client *Client) sendKeepaliveToRoutes(now int64) {
	for _, target := range client.adaptiveRouteTargets(now, false) {
		id := client.tracker.NextID()
		payload, err := transport.Encode(transport.Frame{Type: transport.FrameKeepalive, ID: id, SentAt: now})
		if err != nil {
			continue
		}
		client.tracker.Track(transport.PendingRecord{ID: id, PathID: target.ID, SentAt: now, TimeoutMillis: client.pathRTO(target.ID), Payload: payload})
		client.recordControlTX([]relay.Target{target}, len(payload))
		client.dispatcher.Send(payload, target)
	}
}

func (client *Client) writeBackAdaptive(route *sendRoute, packets []udp.Packet, writeBatch []udp.Packet) {
	writeBatch = writeBatch[:0]
	now := transport.NowMillis()
	for i := range packets {
		packet := packets[i]
		frame, err := transport.Decode(packet.Payload)
		if err != nil {
			if errors.Is(err, transport.ErrNotFrame) || len(packet.Payload) > transport.MaxPayloadSize || !client.pathConfirmed(route.ifName, now) {
				if wgAddr := client.getWireGuardAddr(); wgAddr != nil {
					route.traffic.Data.RecordRX(len(packet.Payload))
					writeBatch = append(writeBatch, udp.Packet{Payload: packet.Payload, Addr: wgAddr})
				}
			}
			continue
		}
		client.markPathSeen(route.ifName, now)
		switch frame.Type {
		case transport.FrameProbe, transport.FrameKeepalive:
			route.traffic.Control.RecordRX(len(packet.Payload))
			ackType := transport.FrameProbeAck
			if frame.Type == transport.FrameKeepalive {
				ackType = transport.FrameKeepaliveAck
			}
			client.sendControlFrame(route, ackType, frame.ID, frame.SentAt)
		case transport.FrameProbeAck, transport.FrameKeepaliveAck:
			route.traffic.Control.RecordRX(len(packet.Payload))
			if record, ok := client.tracker.Complete(frame.ID); ok {
				client.markPathSuccess(route.ifName, now, now-record.SentAt)
			} else {
				client.markPathSuccess(route.ifName, now, now-frame.SentAt)
			}
		case transport.FrameAck:
			route.traffic.Control.RecordRX(len(packet.Payload))
			if record, ok := client.tracker.Complete(frame.ID); ok {
				client.markPathSuccess(route.ifName, now, now-record.SentAt)
			}
		case transport.FrameData:
			client.sendControlFrame(route, transport.FrameAck, frame.ID, frame.SentAt)
			if client.tracker.SeenOrRecord(frame.ID) {
				continue
			}
			if wgAddr := client.getWireGuardAddr(); wgAddr != nil {
				route.traffic.Data.RecordRX(len(frame.Payload))
				writeBatch = append(writeBatch, udp.Packet{Payload: frame.Payload, Addr: wgAddr})
			}
		}
	}
	if _, err := udp.WriteBatchChunks(client.wgSocket, writeBatch, client.cfg.UDPBatch.IsEnabled(), client.cfg.UDPBatch.EffectiveWriteSize()); err != nil {
		log.WithError(err).Warn("Error writing to WireGuard")
	}
}

func (client *Client) sendControlFrame(route *sendRoute, frameType transport.FrameType, id transport.PacketID, sentAt int64) {
	payload, err := transport.Encode(transport.Frame{Type: frameType, ID: id, SentAt: sentAt})
	if err != nil {
		return
	}
	route.traffic.Control.RecordTX(len(payload))
	client.dispatcher.Send(payload, relay.Target{ID: route.ifName, Conn: route.socket, Addr: route.dstAddr})
}

func (client *Client) adaptiveRouteTargets(now int64, eligibleOnly bool) []relay.Target {
	targets := client.routeTargetsSorted(false)
	if len(targets) == 0 || !eligibleOnly {
		return targets
	}
	timeout := time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis) * time.Millisecond
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	eligible := targets[:0]
	for _, target := range targets {
		stats := client.pathStats[target.ID]
		if stats != nil && stats.Eligible(now, timeout) {
			eligible = append(eligible, target)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left := client.pathStats[eligible[i].ID]
		right := client.pathStats[eligible[j].ID]
		if left == nil || right == nil {
			return eligible[i].ID < eligible[j].ID
		}
		if left.Failures != right.Failures {
			return left.Failures < right.Failures
		}
		if left.SmoothedRTT != right.SmoothedRTT {
			return left.SmoothedRTT < right.SmoothedRTT
		}
		return eligible[i].ID < eligible[j].ID
	})
	return eligible
}

func (client *Client) refreshPrimaryPath(now int64) {
	candidates := client.routeIDsSorted()
	client.pathStatsMu.Lock()
	statsSnapshot := client.pathStatsSnapshotLocked()
	current := client.primaryPathID
	client.primaryPathID = transport.SelectPrimaryPath(current, candidates, statsSnapshot, now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
	client.pathStatsMu.Unlock()
}

func (client *Client) primaryRouteTarget(now int64) (relay.Target, bool) {
	client.pathStatsMu.Lock()
	primaryID := client.primaryPathID
	stats := client.pathStats[primaryID]
	eligible := primaryID != "" && stats != nil && stats.Eligible(now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
	client.pathStatsMu.Unlock()
	if !eligible {
		return relay.Target{}, false
	}
	client.routesMu.RLock()
	route := client.routes[primaryID]
	client.routesMu.RUnlock()
	if route == nil {
		return relay.Target{}, false
	}
	return relay.Target{ID: primaryID, Conn: route.socket, Addr: route.dstAddr}, true
}

func (client *Client) currentPrimaryPathID() string {
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	return client.primaryPathID
}

func (client *Client) routeIDsSorted() []string {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	ids := make([]string, 0, len(client.routes))
	for id := range client.routes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (client *Client) routeTargetsSorted(markSent bool) []relay.Target {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	targets := make([]relay.Target, 0, len(client.routes))
	now := time.Now().Unix()
	for ifName, route := range client.routes {
		if markSent {
			route.markSent(now)
		}
		targets = append(targets, relay.Target{ID: ifName, Conn: route.socket, Addr: route.dstAddr})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	return targets
}

func (client *Client) markRouteSent(ifName string) {
	client.routesMu.RLock()
	route := client.routes[ifName]
	client.routesMu.RUnlock()
	if route != nil {
		route.markSent(time.Now().Unix())
	}
}

func (client *Client) recordDataTXBatch(targets []relay.Target, payloads [][]byte) {
	for _, payload := range payloads {
		client.recordDataTX(targets, len(payload))
	}
}

func (client *Client) recordDataTX(targets []relay.Target, payloadSize int) {
	for _, target := range targets {
		client.routesMu.RLock()
		route := client.routes[target.ID]
		client.routesMu.RUnlock()
		if route != nil {
			route.traffic.Data.RecordTX(payloadSize)
		}
	}
}

func (client *Client) recordControlTX(targets []relay.Target, payloadSize int) {
	for _, target := range targets {
		client.routesMu.RLock()
		route := client.routes[target.ID]
		client.routesMu.RUnlock()
		if route != nil {
			route.traffic.Control.RecordTX(payloadSize)
		}
	}
}

func (client *Client) markPathSeen(ifName string, now int64) {
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		stats = &transport.PathStats{ID: ifName}
		client.pathStats[ifName] = stats
	}
	stats.MarkSeen(now)
}

func (client *Client) markPathSuccess(ifName string, now int64, rtt int64) {
	candidates := client.routeIDsSorted()
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		stats = &transport.PathStats{ID: ifName}
		client.pathStats[ifName] = stats
	}
	stats.MarkSuccess(now, rtt)
	client.primaryPathID = transport.SelectPrimaryPath(client.primaryPathID, candidates, client.pathStatsSnapshotLocked(), now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (client *Client) markPathFailure(ifName string, now int64) {
	if !client.hasRoute(ifName) {
		return
	}
	candidates := client.routeIDsSorted()
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		stats = &transport.PathStats{ID: ifName}
		client.pathStats[ifName] = stats
	}
	stats.MarkFailure(now)
	client.primaryPathID = transport.SelectPrimaryPath(client.primaryPathID, candidates, client.pathStatsSnapshotLocked(), now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (client *Client) pathStatsSnapshotLocked() map[string]transport.PathStats {
	snapshot := make(map[string]transport.PathStats, len(client.pathStats))
	for id, stats := range client.pathStats {
		if stats != nil {
			snapshot[id] = *stats
		}
	}
	return snapshot
}

func (client *Client) pathConfirmed(ifName string, now int64) bool {
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		return false
	}
	return stats.Eligible(now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (client *Client) pathRTO(ifName string) int64 {
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		return client.minAckTimeoutMillis()
	}
	return stats.RTO(client.minAckTimeoutMillis(), client.maxAckTimeoutMillis())
}

func (client *Client) pathStatus(ifName string, now int64) *control.PathStatus {
	client.pathStatsMu.Lock()
	defer client.pathStatsMu.Unlock()
	stats := client.pathStats[ifName]
	if stats == nil {
		return nil
	}
	status := &control.PathStatus{
		SmoothedRTTMillis: stats.SmoothedRTT,
		RTTVarianceMillis: stats.RTTVariance,
		Failures:          stats.Failures,
		Eligible:          stats.Eligible(now, time.Duration(client.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond),
	}
	if stats.LastSeen > 0 {
		lastSeen := now - stats.LastSeen
		status.LastSeen = &lastSeen
	}
	if stats.LastSuccess > 0 {
		lastSuccess := now - stats.LastSuccess
		status.LastSuccess = &lastSuccess
	}
	return status
}

func (client *Client) minAckTimeoutMillis() int64 {
	if client.cfg.Transfer.AckTimeoutMillis > 0 {
		return client.cfg.Transfer.AckTimeoutMillis
	}
	return 1
}

func (client *Client) maxAckTimeoutMillis() int64 {
	if client.cfg.Transfer.KeepaliveTimeoutMillis > client.minAckTimeoutMillis() {
		return client.cfg.Transfer.KeepaliveTimeoutMillis
	}
	return client.minAckTimeoutMillis()
}

func (client *Client) routeTargets() []relay.Target {
	return client.routeTargetsSorted(true)
}

func (client *Client) routeSnapshot() map[string]*sendRoute {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	snapshot := make(map[string]*sendRoute, len(client.routes))
	for ifName, route := range client.routes {
		snapshot[ifName] = route
	}
	return snapshot
}

func (client *Client) hasRoute(ifName string) bool {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	_, ok := client.routes[ifName]
	return ok
}

func (client *Client) removeRoute(ifName string) {
	client.routesMu.Lock()
	route, ok := client.routes[ifName]
	if ok {
		delete(client.routes, ifName)
	}
	client.routesMu.Unlock()
	if ok {
		client.dispatcher.Remove(ifName)
		client.removePathStats(ifName)
		route.closing.Store(true)
		route.socket.Close()
		client.updateWireGuardWriteBuffer()
	}
}

func (client *Client) removePathStats(ifName string) {
	client.pathStatsMu.Lock()
	delete(client.pathStats, ifName)
	if client.primaryPathID == ifName {
		client.primaryPathID = ""
	}
	client.pathStatsMu.Unlock()
}

func (client *Client) closeAllRoutes() {
	client.routesMu.Lock()
	routes := client.routes
	client.routes = make(map[string]*sendRoute)
	client.routesMu.Unlock()
	client.dispatcher.Close()
	for _, route := range routes {
		route.closing.Store(true)
		route.socket.Close()
	}
}

func (client *Client) updateWireGuardWriteBuffer() {
	client.routesMu.RLock()
	targetCount := len(client.routes)
	client.routesMu.RUnlock()
	if err := relay.SetWriteBufferForTargets(client.wgSocket, targetCount); err != nil {
		log.WithError(err).Warn("Error setting WireGuard write buffer")
	}
}
