package serverrole

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
	"github.com/adrianceding/engarde/internal/relay"
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

	wgSocket     udpSocket
	clientSocket udpSocket
	wgAddr       *net.UDPAddr

	clientsMu sync.RWMutex
	clients   map[string]*connectedClient

	dispatcher *relay.Dispatcher
	tracker    *transport.Tracker

	pathStatsMu   sync.Mutex
	pathStats     map[string]*transport.PathStats
	lastKeepalive atomic.Int64
}

type connectedClient struct {
	addr *net.UDPAddr
	last atomic.Int64
}

func New(cfg config.Server, version string, webFS fs.FS) *Server {
	cfg.Transfer.ApplyDefaults()
	server := &Server{
		cfg:       cfg,
		version:   version,
		webFS:     webFS,
		clients:   make(map[string]*connectedClient),
		tracker:   transport.NewTracker(cfg.Transfer.PendingWindow, cfg.Transfer.DuplicateWindow),
		pathStats: make(map[string]*transport.PathStats),
	}
	server.dispatcher = relay.NewDispatcherWithBatch(cfg.WriteTimeout, relay.DefaultQueueSize, cfg.UDPBatch.IsEnabled(), cfg.UDPBatch.EffectiveWriteSize(), func(result relay.Result) {
		log.WithError(result.Err).Warn("Error writing to client '" + result.ID + "', terminating it")
		server.removeClient(result.ID)
	})
	return server
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
	sockets := make([]control.WebSocket, 0)
	server.clientsMu.RLock()
	for address, client := range server.clients {
		lastValue := client.last.Load()
		webSocket := control.WebSocket{Address: address}
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
			if !containsUDPAddr(learnedAddrs, addr) {
				server.learnClientAt(addr, now)
				learnedAddrs = append(learnedAddrs, addr)
			}
			if server.adaptiveEnabled() {
				server.handleAdaptiveFromClient(readBatch[i], addr, now, nowMillis, &writeBatch)
				continue
			}
			writeBatch = append(writeBatch, udp.Packet{Payload: readBatch[i].Payload, Addr: server.wgAddr})
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
				server.dispatcher.FanoutBatch(payloads, server.clientTargets(time.Now().Unix()))
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
			*writeBatch = append(*writeBatch, udp.Packet{Payload: packet.Payload, Addr: server.wgAddr})
		}
		return
	}
	server.markPathSeen(addr.String(), nowMillis)
	switch frame.Type {
	case transport.FrameProbe, transport.FrameKeepalive:
		server.learnClientAt(addr, nowUnix)
		ackType := transport.FrameProbeAck
		if frame.Type == transport.FrameKeepalive {
			ackType = transport.FrameKeepaliveAck
		}
		server.sendControlFrame(addr, ackType, frame.ID, frame.SentAt)
	case transport.FrameProbeAck, transport.FrameKeepaliveAck:
		server.markPathSuccess(addr.String(), nowMillis, nowMillis-frame.SentAt)
	case transport.FrameAck:
		if record, ok := server.tracker.Complete(frame.ID); ok {
			server.markPathSuccess(addr.String(), nowMillis, nowMillis-record.SentAt)
		}
	case transport.FrameData:
		server.learnClientAt(addr, nowUnix)
		server.sendControlFrame(addr, transport.FrameAck, frame.ID, frame.SentAt)
		if server.tracker.SeenOrRecord(frame.ID) {
			return
		}
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
		server.dispatcher.Fanout(payload, server.clientTargets(time.Now().Unix()))
		return
	}
	now := transport.NowMillis()
	targets := server.adaptiveClientTargets(now, true)
	if len(targets) == 0 {
		server.sendKeepaliveToClients(now)
		server.dispatcher.Fanout(payload, server.clientTargets(time.Now().Unix()))
		return
	}
	id := server.tracker.NextID()
	framePayload, err := transport.Encode(transport.Frame{Type: transport.FrameData, ID: id, SentAt: now, Payload: payload})
	if err != nil {
		return
	}
	server.tracker.Track(transport.PendingRecord{ID: id, PathID: targets[0].ID, SentAt: now, TimeoutMillis: server.pathRTO(targets[0].ID), Payload: framePayload})
	server.dispatcher.Send(framePayload, targets[0])
}

func (server *Server) retryAdaptiveData(now int64) {
	due := server.tracker.Due(now, server.minAckTimeoutMillis(), server.maxAckTimeoutMillis(), server.cfg.Transfer.MaxRetriesValue())
	if len(due) == 0 {
		return
	}
	targets := server.adaptiveClientTargets(now, true)
	if len(targets) == 0 {
		targets = server.adaptiveClientTargets(now, false)
	}
	for _, record := range due {
		for _, pathID := range record.PathIDs {
			server.markPathFailure(pathID, now)
		}
		pathIDs := make([]string, 0, len(targets))
		for _, target := range targets {
			pathIDs = append(pathIDs, target.ID)
			server.dispatcher.Send(record.Payload, target)
		}
		server.tracker.UpdatePaths(record.ID, pathIDs)
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
		server.dispatcher.Send(payload, target)
	}
}

func (server *Server) sendControlFrame(addr *net.UDPAddr, frameType transport.FrameType, id transport.PacketID, sentAt int64) {
	payload, err := transport.Encode(transport.Frame{Type: frameType, ID: id, SentAt: sentAt})
	if err != nil {
		return
	}
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
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		stats = &transport.PathStats{ID: id}
		server.pathStats[id] = stats
	}
	stats.MarkSuccess(now, rtt)
}

func (server *Server) markPathFailure(id string, now int64) {
	if !server.hasClient(id) {
		return
	}
	server.pathStatsMu.Lock()
	defer server.pathStatsMu.Unlock()
	stats := server.pathStats[id]
	if stats == nil {
		stats = &transport.PathStats{ID: id}
		server.pathStats[id] = stats
	}
	stats.MarkFailure(now)
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

func (server *Server) learnClient(addr *net.UDPAddr) {
	server.learnClientAt(addr, time.Now().Unix())
}

func (server *Server) learnClientAt(addr *net.UDPAddr, now int64) {
	key := addr.String()
	server.clientsMu.RLock()
	client, ok := server.clients[key]
	server.clientsMu.RUnlock()
	if ok {
		client.last.Store(now)
		return
	}

	log.Info("New client connected: '" + key + "'")
	client = &connectedClient{addr: addr}
	client.last.Store(now)
	server.clientsMu.Lock()
	server.clients[key] = client
	server.clientsMu.Unlock()
	server.updateWireGuardWriteBuffer()
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
	targets := make([]relay.Target, 0)
	expired := make([]string, 0)
	cutoff := now - server.cfg.ClientTimeout
	server.clientsMu.RLock()
	for address, client := range server.clients {
		if client.last.Load() > cutoff {
			targets = append(targets, relay.Target{ID: address, Conn: server.clientSocket, Addr: client.addr})
			continue
		}
		expired = append(expired, address)
	}
	server.clientsMu.RUnlock()

	for _, address := range expired {
		log.Info("Client '" + address + "' timed out")
		server.removeExpiredClient(address, cutoff)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	return targets
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
	}
	server.clientsMu.Unlock()
	if removed {
		server.dispatcher.Remove(address)
		server.removePathStats(address)
		server.updateWireGuardWriteBuffer()
	}
}

func (server *Server) removePathStats(id string) {
	server.pathStatsMu.Lock()
	delete(server.pathStats, id)
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
