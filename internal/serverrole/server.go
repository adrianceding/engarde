package serverrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
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

type udpSocket interface {
	relay.UDPWriter
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	Close() error
}

type Server struct {
	cfg     config.Server
	version string
	webFS   fs.FS

	allowedClientIPs  []net.IP
	allowedClientNets []*net.IPNet

	wgSocket     udpSocket
	clientSocket udpSocket
	wgAddr       *net.UDPAddr

	clientsMu             sync.RWMutex
	clients               map[string]*connectedClient
	clientTargetsSnapshot []clientTargetSnapshot

	dispatcher *relay.Dispatcher
	tracker    *transport.Tracker

	pathStatsMu   sync.Mutex
	pathStats     map[string]*transport.PathStats
	selection     transport.PathSelection
	lastKeepalive atomic.Int64
}

type connectedClient struct {
	addr    *net.UDPAddr
	last    atomic.Int64
	traffic stats.Traffic
}

type clientTargetSnapshot struct {
	client *connectedClient
	target relay.Target
}

func New(cfg config.Server, version string, webFS fs.FS) *Server {
	cfg.Transfer.ApplyDefaults()
	cfg.ApplyDefaults()
	allowedClientIPs, allowedClientNets := parseAllowedClients(cfg.AllowedClients)
	server := &Server{
		cfg:               cfg,
		version:           version,
		webFS:             webFS,
		allowedClientIPs:  allowedClientIPs,
		allowedClientNets: allowedClientNets,
		clients:           make(map[string]*connectedClient),
		tracker:           transport.NewTracker(cfg.Transfer.PendingWindow, cfg.Transfer.DuplicateWindow),
		pathStats:         make(map[string]*transport.PathStats),
	}
	server.dispatcher = relay.NewDispatcherWithBatch(cfg.WriteTimeout, cfg.RelayQueueSizeValue(), cfg.UDPBatch.IsEnabled(), cfg.UDPBatch.EffectiveWriteSize(), func(result relay.Result) {
		server.handleDispatcherError(result)
	})
	return server
}

func (server *Server) handleDispatcherError(result relay.Result) {
	server.recordClientDataDrop(result.ID, result.Packets, result.Bytes)
	if errors.Is(result.Err, relay.ErrQueueFull) {
		log.WithError(result.Err).Warn("Queue full for client '" + result.ID + "', dropping packet")
		return
	}
	log.WithError(result.Err).Warn("Error writing to client '" + result.ID + "', terminating it")
	server.removeClient(result.ID)
}

func (server *Server) Run(ctx context.Context) error {
	if server.cfg.Description != "" {
		log.Info(server.cfg.Description)
	}

	wgAddr, err := resolveUDPAddr("udp4", server.cfg.DstAddr)
	if err != nil {
		return err
	}
	wgSource, err := resolveUDPAddr("udp4", "0.0.0.0:0")
	if err != nil {
		return err
	}
	wgSocket, err := listenUDP("udp", wgSource)
	if err != nil {
		return err
	}
	server.wgAddr = wgAddr
	server.wgSocket = wgSocket
	server.updateWireGuardWriteBuffer()

	listenAddr, err := resolveUDPAddr("udp4", server.cfg.ListenAddr)
	if err != nil {
		wgSocket.Close()
		return err
	}
	clientSocket, err := listenUDP("udp", listenAddr)
	if err != nil {
		wgSocket.Close()
		return err
	}
	server.clientSocket = clientSocket
	log.Info("Listening on " + server.cfg.ListenAddr)

	go server.closeOnCancel(ctx)
	if server.cfg.WebManager.ListenAddr != "" {
		go func() {
			if err := runControl(ctx, server.cfg.WebManager.ListenAddr, server.cfg.WebManager.Username, server.cfg.WebManager.Password, server.webFS, server, nil); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	go server.receiveFromWireGuard(ctx)
	if server.adaptiveEnabled() {
		go server.updateAdaptiveTransport(ctx)
	}
	return server.receiveFromClient(ctx)
}

func (server *Server) Status() (any, error) {
	now := time.Now().Unix()
	selection := server.currentPathSelection()
	sockets := make([]control.WebSocket, 0)
	server.clientsMu.RLock()
	for address, client := range server.clients {
		lastValue := client.last.Load()
		webSocket := control.WebSocket{Address: address, PathRole: selection.Role(address), Traffic: client.traffic.Snapshot(), Path: server.pathStatus(address, transport.NowMillis())}
		if lastValue > 0 {
			last := now - lastValue
			webSocket.Last = &last
		}
		sockets = append(sockets, webSocket)
	}
	server.clientsMu.RUnlock()

	return control.ServerStatus{
		Type:          "server",
		Version:       server.version,
		Description:   server.cfg.Description,
		ListenAddress: server.cfg.ListenAddr,
		DstAddress:    server.cfg.DstAddr,
		Sockets:       sockets,
	}, nil
}

func (server *Server) closeOnCancel(ctx context.Context) {
	<-ctx.Done()
	server.dispatcher.Close()
	if server.wgSocket != nil {
		server.wgSocket.Close()
	}
	if server.clientSocket != nil {
		server.clientSocket.Close()
	}
}

func (server *Server) receiveFromClient(ctx context.Context) error {
	readBatch := udp.NewReadBatch(server.cfg.UDPBatch.EffectiveReadSize())
	writeBatch := make([]udp.Packet, 0, server.cfg.UDPBatch.EffectiveWriteSize())
	learnedAddrs := make([]*net.UDPAddr, 0, server.cfg.UDPBatch.EffectiveReadSize())
	for {
		n, err := udp.ReadBatch(server.clientSocket, readBatch, server.cfg.UDPBatch.IsEnabled())
		if err != nil && n == 0 {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from client")
			continue
		}
		writeBatch = writeBatch[:0]
		learnedAddrs = learnedAddrs[:0]
		now := time.Now().Unix()
		nowMillis := transport.NowMillis()
		for i := 0; i < n; i++ {
			addr := readBatch[i].Addr
			if addr == nil {
				continue
			}
			if server.adaptiveEnabled() {
				if !containsUDPAddr(learnedAddrs, addr) {
					server.learnClientAt(addr, now)
					learnedAddrs = append(learnedAddrs, addr)
				}
				server.handleAdaptiveFromClient(readBatch[i], addr, now, nowMillis, &writeBatch)
				continue
			}
			if server.handleDirectFromClient(readBatch[i], addr, now, &writeBatch) && !containsUDPAddr(learnedAddrs, addr) {
				learnedAddrs = append(learnedAddrs, addr)
			}
		}
		if _, err := udp.WriteBatchChunks(server.wgSocket, writeBatch, server.cfg.UDPBatch.IsEnabled(), server.cfg.UDPBatch.EffectiveWriteSize()); err != nil {
			log.WithError(err).Warn("Error writing to WireGuard")
		}
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from client")
		}
	}
}

func parseAllowedClients(values []string) ([]net.IP, []*net.IPNet) {
	ips := make([]net.IP, 0, len(values))
	networks := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ip := net.ParseIP(value); ip != nil {
			ips = append(ips, ip)
			continue
		}
		if _, network, err := net.ParseCIDR(value); err == nil {
			networks = append(networks, network)
		}
	}
	return ips, networks
}

func (server *Server) clientAllowed(addr *net.UDPAddr) bool {
	if addr == nil || addr.IP == nil {
		return false
	}
	if len(server.allowedClientIPs) == 0 && len(server.allowedClientNets) == 0 {
		return true
	}
	for _, ip := range server.allowedClientIPs {
		if ip.Equal(addr.IP) {
			return true
		}
	}
	for _, network := range server.allowedClientNets {
		if network.Contains(addr.IP) {
			return true
		}
	}
	return false
}

func (server *Server) handleDirectFromClient(packet udp.Packet, addr *net.UDPAddr, now int64, writeBatch *[]udp.Packet) bool {
	if !server.clientAllowed(addr) {
		return false
	}
	if !server.learnClientAt(addr, now) {
		return false
	}
	server.recordClientDataRX(addr.String(), len(packet.Payload))
	*writeBatch = append(*writeBatch, udp.Packet{Payload: packet.Payload, Addr: server.wgAddr})
	return true
}

func (server *Server) receiveFromWireGuard(ctx context.Context) {
	readBatch := udp.NewReadBatch(server.cfg.UDPBatch.EffectiveReadSize())
	payloads := make([][]byte, 0, server.cfg.UDPBatch.EffectiveReadSize())
	for {
		n, err := udp.ReadBatch(server.wgSocket, readBatch, server.cfg.UDPBatch.IsEnabled())
		if err != nil && n == 0 {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.WithError(err).Warn("Error reading from WireGuard")
			continue
		}
		payloads = payloads[:0]
		for i := 0; i < n; i++ {
			payloads = append(payloads, readBatch[i].Payload)
		}
		if len(payloads) > 0 {
			if server.adaptiveEnabled() {
				server.sendAdaptiveDataBatch(payloads)
			} else {
				targets := server.clientTargets(time.Now().Unix())
				server.recordDataTXBatch(targets, payloads)
				server.dispatcher.FanoutBatch(payloads, targets)
			}
		}
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.WithError(err).Warn("Error reading from WireGuard")
		}
	}
}

func (server *Server) adaptiveEnabled() bool {
	return server.cfg.Transfer.IsAdaptive() && server.tracker != nil
}

func (server *Server) updateAdaptiveTransport(ctx context.Context) {
	interval := time.Duration(server.minAckTimeoutMillis()/2) * time.Millisecond
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
			server.maintainAdaptiveTransport()
		}
	}
}

func (server *Server) maintainAdaptiveTransport() {
	if !server.adaptiveEnabled() {
		return
	}
	now := transport.NowMillis()
	server.refreshPathSelection(now)
	server.retryAdaptiveData(now)
	lastKeepalive := server.lastKeepalive.Load()
	if now-lastKeepalive >= server.cfg.Transfer.KeepaliveIntervalMillis && server.lastKeepalive.CompareAndSwap(lastKeepalive, now) {
		server.sendKeepaliveToClients(now)
	}
}

func (server *Server) handleAdaptiveFromClient(packet udp.Packet, addr *net.UDPAddr, nowUnix int64, nowMillis int64, writeBatch *[]udp.Packet) {
	frame, err := transport.Decode(packet.Payload)
	if err != nil {
		if errors.Is(err, transport.ErrNotFrame) || len(packet.Payload) > transport.MaxPayloadSize || !server.pathConfirmed(addr.String(), nowMillis) {
			server.recordClientDataRX(addr.String(), len(packet.Payload))
			*writeBatch = append(*writeBatch, udp.Packet{Payload: packet.Payload, Addr: server.wgAddr})
		}
		return
	}
	server.markPathSeen(addr.String(), nowMillis)
	switch frame.Type {
	case transport.FrameProbe, transport.FrameKeepalive:
		server.recordClientControlRX(addr.String(), len(packet.Payload))
		server.learnClientAt(addr, nowUnix)
		ackType := transport.FrameProbeAck
		if frame.Type == transport.FrameKeepalive {
			ackType = transport.FrameKeepaliveAck
		}
		server.sendControlFrame(addr, ackType, frame.ID, frame.SentAt)
	case transport.FrameProbeAck, transport.FrameKeepaliveAck:
		server.recordClientControlRX(addr.String(), len(packet.Payload))
		if record, ok := server.tracker.Complete(frame.ID); ok {
			server.markPathSuccess(addr.String(), nowMillis, nowMillis-record.SentAtForPath(addr.String()))
		} else {
			server.markPathSuccess(addr.String(), nowMillis, nowMillis-frame.SentAt)
		}
	case transport.FrameAck:
		server.recordClientControlRX(addr.String(), len(packet.Payload))
		if record, ok := server.tracker.Complete(frame.ID); ok {
			server.markPathSuccess(addr.String(), nowMillis, nowMillis-record.SentAtForPath(addr.String()))
		} else {
			server.markPathSuccess(addr.String(), nowMillis, nowMillis-frame.SentAt)
		}
	case transport.FrameData:
		server.learnClientAt(addr, nowUnix)
		server.sendControlFrame(addr, transport.FrameAck, frame.ID, frame.SentAt)
		if server.tracker.SeenOrRecord(frame.ID) {
			return
		}
		server.recordClientDataRX(addr.String(), len(frame.Payload))
		*writeBatch = append(*writeBatch, udp.Packet{Payload: frame.Payload, Addr: server.wgAddr})
	}
}

func (server *Server) sendAdaptiveDataBatch(payloads [][]byte) {
	for _, payload := range payloads {
		server.sendAdaptiveData(payload)
	}
}

func (server *Server) sendAdaptiveData(payload []byte) {
	if len(payload) > transport.MaxPayloadSize {
		now := transport.NowMillis()
		server.sendKeepaliveToClients(now)
		targets := server.clientTargets(time.Now().Unix())
		server.recordDataTX(targets, len(payload))
		server.dispatcher.Fanout(payload, targets)
		return
	}
	now := transport.NowMillis()
	selection := server.currentPathSelection()
	targets := server.firstClientTargets(selection, now)
	if len(targets) == 0 {
		server.refreshPathSelection(now)
		selection = server.currentPathSelection()
		targets = server.firstClientTargets(selection, now)
	}
	if len(targets) == 0 {
		server.sendKeepaliveToClients(now)
		fallbackTargets := server.clientTargets(time.Now().Unix())
		server.recordDataTX(fallbackTargets, len(payload))
		server.dispatcher.Fanout(payload, fallbackTargets)
		return
	}
	id := server.tracker.NextID()
	framePayload, err := transport.Encode(transport.Frame{Type: transport.FrameData, ID: id, SentAt: now, Payload: payload})
	if err != nil {
		return
	}
	pathIDs := serverTargetIDs(targets)
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: pathIDs[0], PathIDs: pathIDs, AttemptPathIDs: pathIDs, FallbackPathIDs: selection.FallbackPathIDs, SentAt: now, TimeoutMillis: server.pathsRTO(pathIDs), Payload: framePayload})
	for _, target := range targets {
		server.recordDataTX([]relay.Target{target}, len(payload))
		server.dispatcher.Send(framePayload, target)
	}
}

func (server *Server) retryAdaptiveData(now int64) {
	due := server.tracker.Due(now, server.minAckTimeoutMillis(), server.maxAckTimeoutMillis(), server.cfg.Transfer.MaxRetriesValue())
	if len(due) == 0 {
		return
	}
	for _, record := range due {
		for _, pathID := range record.AttemptPathIDs {
			server.markPathFailure(pathID, now)
		}
		target, ok := server.nextFallbackClientTarget(record.PendingRecord, now)
		if !ok {
			server.tracker.Drop(record.ID)
			continue
		}
		server.recordDataTX([]relay.Target{target}, stats.AdaptiveDataPayloadSize(record.Payload))
		server.dispatcher.Send(record.Payload, target)
		server.tracker.RecordAttemptAt(record.ID, []string{target.ID}, now)
	}
}

func (server *Server) sendKeepaliveToClients(now int64) {
	for _, target := range server.adaptiveClientTargets(now, false) {
		id := server.tracker.NextID()
		payload, err := transport.Encode(transport.Frame{Type: transport.FrameKeepalive, ID: id, SentAt: now})
		if err != nil {
			continue
		}
		server.tracker.Track(transport.PendingRecord{ID: id, PathID: target.ID, SentAt: now, TimeoutMillis: server.pathRTO(target.ID), Payload: payload})
		server.recordControlTX([]relay.Target{target}, len(payload))
		server.dispatcher.Send(payload, target)
	}
}

func (server *Server) sendControlFrame(addr *net.UDPAddr, frameType transport.FrameType, id transport.PacketID, sentAt int64) {
	payload, err := transport.Encode(transport.Frame{Type: frameType, ID: id, SentAt: sentAt})
	if err != nil {
		return
	}
	server.recordClientControlTX(addr.String(), len(payload))
	server.dispatcher.Send(payload, relay.Target{ID: addr.String(), Conn: server.clientSocket, Addr: addr})
}

func (server *Server) adaptiveClientTargets(now int64, eligibleOnly bool) []relay.Target {
	targets := server.clientTargetsSorted(time.Now().Unix())
	if len(targets) == 0 || !eligibleOnly {
		return targets
	}
	timeout := time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis) * time.Millisecond
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	eligible := targets[:0]
	for _, target := range targets {
		stats := server.pathStats[target.ID]
		if stats != nil && stats.Eligible(now, timeout) {
			eligible = append(eligible, target)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left := server.pathStats[eligible[i].ID]
		right := server.pathStats[eligible[j].ID]
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

func (server *Server) refreshPathSelection(now int64) {
	candidates := server.clientIDsSorted(time.Now().Unix())
	server.pathStatsMu.Lock()
	statsSnapshot := server.pathStatsSnapshotLocked()
	server.selection = transport.SelectPathSelection(server.selection, candidates, statsSnapshot, now, time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
	server.pathStatsMu.Unlock()
}

func (server *Server) firstClientTargets(selection transport.PathSelection, now int64) []relay.Target {
	return server.clientTargetsByID(selection.FirstPathIDs, now, true)
}

func (server *Server) nextFallbackClientTarget(record transport.PendingRecord, now int64) (relay.Target, bool) {
	selection := server.currentPathSelection()
	pathIDs := mergeServerStrings(record.FallbackPathIDs, mergeServerStrings(selection.FirstPathIDs, selection.FallbackPathIDs))
	for _, target := range server.clientTargetsByID(pathIDs, now, true) {
		if !containsServerString(record.PathIDs, target.ID) {
			return target, true
		}
	}
	return relay.Target{}, false
}

func (server *Server) clientTargetsByID(pathIDs []string, now int64, requireEligible bool) []relay.Target {
	if len(pathIDs) == 0 {
		return nil
	}
	timeout := time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis) * time.Millisecond
	targets := make([]relay.Target, 0, len(pathIDs))
	server.clientsMu.RLock()
	defer server.clientsMu.RUnlock()
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	for _, pathID := range pathIDs {
		if requireEligible {
			stats := server.pathStats[pathID]
			if stats == nil || !stats.Eligible(now, timeout) {
				continue
			}
		}
		client := server.clients[pathID]
		if client == nil {
			continue
		}
		targets = append(targets, relay.Target{ID: pathID, Conn: server.clientSocket, Addr: client.addr})
	}
	return targets
}

func (server *Server) currentPathSelection() transport.PathSelection {
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	return transport.PathSelection{FirstPathIDs: append([]string(nil), server.selection.FirstPathIDs...), FallbackPathIDs: append([]string(nil), server.selection.FallbackPathIDs...), FirstPathCountChangedAt: server.selection.FirstPathCountChangedAt}
}

func serverTargetIDs(targets []relay.Target) []string {
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.ID)
	}
	return ids
}

func containsServerString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func mergeServerStrings(first []string, second []string) []string {
	merged := make([]string, 0, len(first)+len(second))
	seen := make(map[string]struct{}, len(first)+len(second))
	for _, value := range first {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		merged = append(merged, value)
	}
	for _, value := range second {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		merged = append(merged, value)
	}
	return merged
}

func (server *Server) recordDataTXBatch(targets []relay.Target, payloads [][]byte) {
	for _, payload := range payloads {
		server.recordDataTX(targets, len(payload))
	}
}

func (server *Server) recordDataTX(targets []relay.Target, payloadSize int) {
	for _, target := range targets {
		server.recordClientDataTX(target.ID, payloadSize)
	}
}

func (server *Server) recordControlTX(targets []relay.Target, payloadSize int) {
	for _, target := range targets {
		server.recordClientControlTX(target.ID, payloadSize)
	}
}

func (server *Server) recordClientDataRX(id string, payloadSize int) {
	if client := server.clientByID(id); client != nil {
		client.traffic.Data.RecordRX(payloadSize)
	}
}

func (server *Server) recordClientDataTX(id string, payloadSize int) {
	if client := server.clientByID(id); client != nil {
		client.traffic.Data.RecordTX(payloadSize)
	}
}

func (server *Server) recordClientDataDrop(id string, packets int, bytes int) {
	client := server.clientByID(id)
	if client == nil {
		return
	}
	if packets <= 0 {
		packets = 1
	}
	for i := 0; i < packets; i++ {
		dropBytes := 0
		if i == 0 {
			dropBytes = bytes
		}
		client.traffic.Data.RecordDrop(dropBytes)
	}
}

func (server *Server) recordClientControlRX(id string, payloadSize int) {
	if client := server.clientByID(id); client != nil {
		client.traffic.Control.RecordRX(payloadSize)
	}
}

func (server *Server) recordClientControlTX(id string, payloadSize int) {
	if client := server.clientByID(id); client != nil {
		client.traffic.Control.RecordTX(payloadSize)
	}
}

func (server *Server) clientByID(id string) *connectedClient {
	server.clientsMu.RLock()
	defer server.clientsMu.RUnlock()
	return server.clients[id]
}

func (server *Server) markPathSeen(id string, now int64) {
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		stats = &transport.PathStats{ID: id}
		server.pathStats[id] = stats
	}
	stats.MarkSeen(now)
}

func (server *Server) markPathSuccess(id string, now int64, rtt int64) {
	candidates := server.clientIDsSorted(time.Now().Unix())
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		stats = &transport.PathStats{ID: id}
		server.pathStats[id] = stats
	}
	stats.MarkSuccess(now, rtt)
	server.selection = transport.SelectPathSelection(server.selection, candidates, server.pathStatsSnapshotLocked(), now, time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (server *Server) markPathFailure(id string, now int64) {
	if !server.hasClient(id) {
		return
	}
	candidates := server.clientIDsSorted(time.Now().Unix())
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		stats = &transport.PathStats{ID: id}
		server.pathStats[id] = stats
	}
	stats.MarkFailure(now)
	server.selection = transport.SelectPathSelection(server.selection, candidates, server.pathStatsSnapshotLocked(), now, time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (server *Server) pathStatsSnapshotLocked() map[string]transport.PathStats {
	snapshot := make(map[string]transport.PathStats, len(server.pathStats))
	for id, stats := range server.pathStats {
		if stats != nil {
			snapshot[id] = *stats
		}
	}
	return snapshot
}

func (server *Server) pathConfirmed(id string, now int64) bool {
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		return false
	}
	return stats.Eligible(now, time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond)
}

func (server *Server) pathRTO(id string) int64 {
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		return server.minAckTimeoutMillis()
	}
	return stats.RTO(server.minAckTimeoutMillis(), server.maxAckTimeoutMillis())
}

func (server *Server) pathsRTO(pathIDs []string) int64 {
	timeoutMillis := server.minAckTimeoutMillis()
	for _, pathID := range pathIDs {
		pathTimeoutMillis := server.pathRTO(pathID)
		if pathTimeoutMillis > timeoutMillis {
			timeoutMillis = pathTimeoutMillis
		}
	}
	return timeoutMillis
}

func (server *Server) pathStatus(id string, now int64) *control.PathStatus {
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		return nil
	}
	status := &control.PathStatus{
		SmoothedRTTMillis: stats.SmoothedRTT,
		RTTVarianceMillis: stats.RTTVariance,
		Failures:          stats.Failures,
		Eligible:          stats.Eligible(now, time.Duration(server.cfg.Transfer.KeepaliveTimeoutMillis)*time.Millisecond),
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

func (server *Server) minAckTimeoutMillis() int64 {
	if server.cfg.Transfer.AckTimeoutMillis > 0 {
		return server.cfg.Transfer.AckTimeoutMillis
	}
	return 1
}

func (server *Server) maxAckTimeoutMillis() int64 {
	if server.cfg.Transfer.KeepaliveTimeoutMillis > server.minAckTimeoutMillis() {
		return server.cfg.Transfer.KeepaliveTimeoutMillis
	}
	return server.minAckTimeoutMillis()
}

func (server *Server) learnClient(addr *net.UDPAddr) bool {
	return server.learnClientAt(addr, time.Now().Unix())
}

func (server *Server) learnClientAt(addr *net.UDPAddr, now int64) bool {
	key := addr.String()
	server.clientsMu.RLock()
	client, ok := server.clients[key]
	server.clientsMu.RUnlock()
	if ok {
		client.last.Store(now)
		return true
	}

	server.clientsMu.Lock()
	if client, ok := server.clients[key]; ok {
		server.clientsMu.Unlock()
		client.last.Store(now)
		return true
	}
	maxClients := server.cfg.MaxClientsValue()
	if maxClients > 0 && len(server.clients) >= maxClients {
		server.clientsMu.Unlock()
		return false
	}

	log.Info("New client connected: '" + key + "'")
	client = &connectedClient{addr: addr}
	client.last.Store(now)
	server.clients[key] = client
	server.rebuildClientTargetsSnapshotLocked()
	server.clientsMu.Unlock()
	server.updateWireGuardWriteBuffer()
	return true
}

func containsUDPAddr(addrs []*net.UDPAddr, addr *net.UDPAddr) bool {
	for _, existing := range addrs {
		if sameUDPAddr(existing, addr) {
			return true
		}
	}
	return false
}

func sameUDPAddr(first, second *net.UDPAddr) bool {
	if first == nil || second == nil {
		return first == second
	}
	return first.Port == second.Port && first.Zone == second.Zone && first.IP.Equal(second.IP)
}

func (server *Server) clientTargets(now int64) []relay.Target {
	return server.clientTargetsSorted(now)
}

func (server *Server) hasClient(id string) bool {
	server.clientsMu.RLock()
	defer server.clientsMu.RUnlock()
	_, ok := server.clients[id]
	return ok
}

func (server *Server) clientTargetsSorted(now int64) []relay.Target {
	server.ensureClientTargetsSnapshot()
	targets := make([]relay.Target, 0)
	expired := make([]string, 0)
	cutoff := now - server.cfg.ClientTimeout
	server.clientsMu.RLock()
	for _, snapshot := range server.clientTargetsSnapshot {
		address := snapshot.target.ID
		client := snapshot.client
		if client.last.Load() > cutoff {
			targets = append(targets, snapshot.target)
			continue
		}
		expired = append(expired, address)
	}
	server.clientsMu.RUnlock()

	for _, address := range expired {
		log.Info("Client '" + address + "' timed out")
		server.removeExpiredClient(address, cutoff)
	}
	return targets
}

func (server *Server) ensureClientTargetsSnapshot() {
	server.clientsMu.RLock()
	needsRebuild := len(server.clientTargetsSnapshot) != len(server.clients)
	server.clientsMu.RUnlock()
	if !needsRebuild {
		return
	}
	server.clientsMu.Lock()
	if len(server.clientTargetsSnapshot) != len(server.clients) {
		server.rebuildClientTargetsSnapshotLocked()
	}
	server.clientsMu.Unlock()
}

func (server *Server) clientIDsSorted(now int64) []string {
	ids := make([]string, 0)
	expired := make([]string, 0)
	cutoff := now - server.cfg.ClientTimeout
	server.clientsMu.RLock()
	for address, client := range server.clients {
		if client.last.Load() > cutoff {
			ids = append(ids, address)
			continue
		}
		expired = append(expired, address)
	}
	server.clientsMu.RUnlock()

	for _, address := range expired {
		log.Info("Client '" + address + "' timed out")
		server.removeExpiredClient(address, cutoff)
	}
	sort.Strings(ids)
	return ids
}

func (server *Server) removeExpiredClient(address string, cutoff int64) {
	server.clientsMu.Lock()
	client, ok := server.clients[address]
	if !ok {
		server.clientsMu.Unlock()
		return
	}
	removed := false
	if client.last.Load() <= cutoff {
		delete(server.clients, address)
		server.rebuildClientTargetsSnapshotLocked()
		removed = true
	}
	server.clientsMu.Unlock()
	if removed {
		server.dispatcher.Remove(address)
		server.removePathStats(address)
		server.updateWireGuardWriteBuffer()
	}
}

func (server *Server) removeClient(address string) {
	server.clientsMu.Lock()
	_, removed := server.clients[address]
	if removed {
		delete(server.clients, address)
		server.rebuildClientTargetsSnapshotLocked()
	}
	server.clientsMu.Unlock()
	if removed {
		server.dispatcher.Remove(address)
		server.removePathStats(address)
		server.updateWireGuardWriteBuffer()
	}
}

func (server *Server) rebuildClientTargetsSnapshotLocked() {
	snapshot := make([]clientTargetSnapshot, 0, len(server.clients))
	for address, client := range server.clients {
		snapshot = append(snapshot, clientTargetSnapshot{client: client, target: relay.Target{ID: address, Conn: server.clientSocket, Addr: client.addr}})
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].target.ID < snapshot[j].target.ID })
	server.clientTargetsSnapshot = snapshot
}

func (server *Server) removePathStats(id string) {
	server.pathStatsMu.Lock()
	delete(server.pathStats, id)
	server.selection = server.selection.Without(id)
	server.pathStatsMu.Unlock()
}

func (server *Server) updateWireGuardWriteBuffer() {
	server.clientsMu.RLock()
	targetCount := len(server.clients)
	server.clientsMu.RUnlock()
	if err := relay.SetWriteBufferForTargets(server.wgSocket, targetCount); err != nil {
		log.WithError(err).Warn("Error setting WireGuard write buffer")
	}
}
