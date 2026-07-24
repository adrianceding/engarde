package serverrole

import (
	"container/list"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/stats"
	"github.com/adrianceding/engarde/internal/tcpstream"
	log "github.com/sirupsen/logrus"
)

var listenTCP = func(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

var errTCPStreamOpenTimeout = errors.New("TCP stream OPEN timed out")

var dialTCPDestination = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return dialer.DialContext(ctx, "tcp", address)
}

var tcpFlowCarrierCount = func(flow *tcpstream.Flow) int { return flow.CarrierCount() }

type tcpServerTimer interface {
	Stop() bool
}

var newTCPServerOpenTimer = func(delay time.Duration, callback func()) tcpServerTimer {
	return time.AfterFunc(delay, callback)
}

const (
	tcpServerClosedTTL = time.Minute
	// Keep disconnected peer counters available across several status polls.
	tcpServerTrafficTTL          = 5 * time.Minute
	tcpServerMaintenanceInterval = 30 * time.Second
	// Unlimited admission settings must not make inactive auxiliary state unbounded.
	tcpServerClosedCacheSafetyLimit  = 64 * 1024
	tcpServerTrafficCacheSafetyLimit = 64 * 1024
)

type tcpServerRuntime struct {
	server     *Server
	ctx        context.Context
	cancel     context.CancelFunc
	listener   net.Listener
	instanceID tcpstream.ServerInstanceID

	mu              sync.Mutex
	closing         bool
	streams         map[tcpstream.StreamID]*tcpServerStream
	closed          map[tcpstream.StreamID]time.Time
	closedOrder     *list.List
	closedItems     map[tcpstream.StreamID]*list.Element
	sessions        map[*tcpstream.Session]struct{}
	probes          map[*tcpstream.Session]struct{}
	connections     map[*tcpServerConn]struct{}
	traffic         map[string]*tcpServerTraffic
	inactiveTraffic *list.List
	pending         chan struct{}
	pendingResumes  chan struct{}
	pendingStreams  int
	pendingOpens    int
	acceptWG        sync.WaitGroup
	streamWG        sync.WaitGroup
	flowWG          sync.WaitGroup
	recoveryWG      sync.WaitGroup
	probeWG         sync.WaitGroup
	backgroundWG    sync.WaitGroup
	recoveryMu      sync.Mutex
	nextStreamOrder uint64
}

type tcpServerTraffic struct {
	stats.Traffic
	lastUsed        atomic.Int64
	active          int
	address         string
	inactiveElement *list.Element
}

func (traffic *tcpServerTraffic) touch(now time.Time) {
	traffic.lastUsed.Store(now.UnixNano())
}

func (traffic *tcpServerTraffic) usedAt() time.Time {
	return time.Unix(0, traffic.lastUsed.Load())
}

type tcpServerConn struct {
	net.Conn
	runtime   *tcpServerRuntime
	closeOnce sync.Once
	closeErr  error
	traffic   *tcpServerTraffic
}

func (conn *tcpServerConn) Close() error {
	conn.closeOnce.Do(func() {
		conn.closeErr = conn.Conn.Close()
		conn.runtime.connectionClosed(conn)
	})
	return conn.closeErr
}

type tcpServerStream struct {
	attachMu    sync.Mutex
	ready       chan struct{}
	version     uint8
	destination string
	principal   string
	flow        *tcpstream.Flow
	openTimer   tcpServerTimer
	started     bool
	recoverable bool
	resumeToken tcpstream.ResumeToken
	generation  uint64
	order       uint64
	err         error
}

func (server *Server) runTCP(ctx context.Context) error {
	var instanceID tcpstream.ServerInstanceID
	if server.cfg.Transfer.TCP.ActiveStandby() {
		var err error
		instanceID, err = tcpstream.NewServerInstanceID()
		if err != nil {
			return err
		}
	}
	listener, err := listenTCP("tcp", server.cfg.ListenAddr)
	if err != nil {
		return err
	}
	runtimeCtx, cancel := context.WithCancel(ctx)
	runtime := &tcpServerRuntime{
		server:          server,
		ctx:             runtimeCtx,
		cancel:          cancel,
		listener:        listener,
		instanceID:      instanceID,
		streams:         make(map[tcpstream.StreamID]*tcpServerStream),
		closed:          make(map[tcpstream.StreamID]time.Time),
		closedOrder:     list.New(),
		closedItems:     make(map[tcpstream.StreamID]*list.Element),
		sessions:        make(map[*tcpstream.Session]struct{}),
		probes:          make(map[*tcpstream.Session]struct{}),
		connections:     make(map[*tcpServerConn]struct{}),
		traffic:         make(map[string]*tcpServerTraffic),
		inactiveTraffic: list.New(),
	}
	if limit := server.cfg.Transfer.TCP.MaxPendingConnections; limit > 0 {
		runtime.pending = make(chan struct{}, limit)
	}
	if server.cfg.Transfer.TCP.ActiveStandby() {
		runtime.pendingResumes = make(chan struct{}, server.cfg.Transfer.TCP.MaxPendingResumes)
	}
	server.setTCPRuntime(runtime)
	log.Info("Listening on " + server.cfg.ListenAddr + " over TCP")
	runtime.startBackground()
	defer runtime.shutdown()
	if server.cfg.WebManager.ListenAddr != "" {
		runtime.backgroundWG.Add(1)
		go func() {
			defer runtime.backgroundWG.Done()
			if err := runControl(runtime.ctx, server.cfg.WebManager.ListenAddr, server.cfg.WebManager.Username, server.cfg.WebManager.Password, server.webFS, server, nil); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			if ctx.Err() != nil || errors.Is(acceptErr, net.ErrClosed) {
				return nil
			}
			return acceptErr
		}
		if runtime.tryAcquirePending() {
			runtime.startAccept(conn)
		} else {
			_ = conn.Close()
		}
	}
}

func (runtime *tcpServerRuntime) startBackground() {
	runtime.backgroundWG.Add(2)
	go func() {
		defer runtime.backgroundWG.Done()
		<-runtime.ctx.Done()
		_ = runtime.listener.Close()
	}()
	go func() {
		defer runtime.backgroundWG.Done()
		ticker := time.NewTicker(tcpServerMaintenanceInterval)
		defer ticker.Stop()
		runtime.maintain(ticker.C)
	}()
}

func (runtime *tcpServerRuntime) maintain(ticks <-chan time.Time) {
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case now := <-ticks:
			runtime.pruneState(now)
		}
	}
}

func (runtime *tcpServerRuntime) startAccept(conn net.Conn) {
	runtime.mu.Lock()
	if runtime.closing || runtime.ctx.Err() != nil {
		runtime.mu.Unlock()
		runtime.releasePending()
		_ = conn.Close()
		return
	}
	tracked := &tcpServerConn{Conn: conn, runtime: runtime}
	runtime.connections[tracked] = struct{}{}
	runtime.acceptWG.Add(1)
	runtime.mu.Unlock()
	go func() {
		defer runtime.acceptWG.Done()
		runtime.acceptWithHandshakeDone(tracked, runtime.releasePending)
	}()
}

func (runtime *tcpServerRuntime) tryAcquirePending() bool {
	if runtime.pending == nil {
		return true
	}
	select {
	case runtime.pending <- struct{}{}:
		return true
	default:
		return false
	}
}

func (runtime *tcpServerRuntime) releasePending() {
	if runtime.pending != nil {
		<-runtime.pending
	}
}

func (runtime *tcpServerRuntime) tryAcquireResume() bool {
	if runtime.pendingResumes == nil {
		return false
	}
	select {
	case runtime.pendingResumes <- struct{}{}:
		return true
	default:
		return false
	}
}

func (runtime *tcpServerRuntime) releaseResume() {
	<-runtime.pendingResumes
}

func (runtime *tcpServerRuntime) connectionClosed(conn *tcpServerConn) {
	runtime.mu.Lock()
	delete(runtime.connections, conn)
	if conn.traffic != nil {
		if conn.traffic.active > 0 {
			conn.traffic.active--
		}
		conn.traffic.touch(time.Now())
		if conn.traffic.active == 0 {
			runtime.markTrafficInactiveLocked(conn.traffic)
			runtime.trimTrafficLocked()
		}
		conn.traffic = nil
	}
	runtime.mu.Unlock()
}

func (runtime *tcpServerRuntime) status() control.ServerStatus {
	runtime.mu.Lock()
	runtime.pruneStateLocked(time.Now())
	streamCount := len(runtime.streams)
	sessionCount := len(runtime.sessions)
	streams := make([]control.TCPStreamStatus, 0, streamCount)
	streamIDs := make([]tcpstream.StreamID, 0, streamCount)
	streamFlows := make([]*tcpstream.Flow, 0, streamCount)
	streamSources := make([]*tcpServerStream, 0, streamCount)
	for streamID, stream := range runtime.streams {
		state := "connecting"
		if stream.flow != nil {
			state = "active"
		} else if stream.err != nil {
			state = "failed"
		}
		streams = append(streams, control.TCPStreamStatus{
			Version:     stream.version,
			Destination: stream.destination,
			State:       state,
			Recoverable: stream.recoverable,
		})
		streamIDs = append(streamIDs, streamID)
		streamFlows = append(streamFlows, stream.flow)
		streamSources = append(streamSources, stream)
	}
	sockets := make([]control.WebSocket, 0, len(runtime.traffic))
	trafficSources := make([]*tcpServerTraffic, 0, len(runtime.traffic))
	for address, traffic := range runtime.traffic {
		sockets = append(sockets, control.WebSocket{Address: address})
		trafficSources = append(trafficSources, traffic)
	}
	runtime.mu.Unlock()

	carrierCount := 0
	recovering := 0
	for index := range streams {
		streams[index].ID = hex.EncodeToString(streamIDs[index][:8])
		streamSources[index].attachMu.Lock()
		streams[index].Generation = streamSources[index].generation
		streamSources[index].attachMu.Unlock()
		if flow := streamFlows[index]; flow != nil {
			streams[index].Carriers = tcpFlowCarrierCount(flow)
			carrierCount += streams[index].Carriers
			flowState := flow.State()
			streams[index].State = string(flowState)
			if flowState == tcpstream.FlowStateRecovering {
				recovering++
			}
		}
	}
	sort.Slice(streams, func(left, right int) bool { return streams[left].ID < streams[right].ID })
	for index, traffic := range trafficSources {
		sockets[index].Traffic = traffic.Snapshot()
	}
	sort.Slice(sockets, func(left, right int) bool { return sockets[left].Address < sockets[right].Address })
	return control.ServerStatus{
		Type:            "server",
		Version:         runtime.server.version,
		Description:     runtime.server.cfg.Description,
		ListenAddress:   runtime.server.cfg.ListenAddr,
		PeerAuthEnabled: runtime.server.cfg.PeerAuthEnabled(),
		Streams:         streamCount,
		Carriers:        carrierCount,
		Sessions:        sessionCount,
		CarrierMode:     string(runtime.server.cfg.Transfer.TCP.CarrierMode),
		Recovering:      recovering,
		Sockets:         sockets,
		TCPStreams:      streams,
	}
}

func (runtime *tcpServerRuntime) accept(conn net.Conn) {
	runtime.acceptWithHandshakeDone(conn, nil)
}

func (runtime *tcpServerRuntime) acceptWithHandshakeDone(conn net.Conn, handshakeDone func()) {
	handshakeFinished := false
	finishHandshake := func() {
		if handshakeFinished {
			return
		}
		handshakeFinished = true
		if handshakeDone != nil {
			handshakeDone()
		}
	}
	defer finishHandshake()

	if runtime.isClosing() {
		_ = conn.Close()
		return
	}
	remoteAddr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok || !runtime.server.clientIPAllowed(remoteAddr.IP) {
		conn.Close()
		return
	}
	var users map[string]string
	if runtime.server.cfg.PeerAuth != nil {
		users = runtime.server.cfg.PeerAuth.Users
	}
	handshakeTimeout := time.Duration(runtime.server.cfg.Transfer.TCP.OpenTimeoutMillis) * time.Millisecond
	session, principal, err := tcpstream.AcceptSession(conn, tcpstream.MaxPayloadSize, handshakeTimeout, runtime.sessionConfig(), users)
	finishHandshake()
	if err != nil {
		return
	}
	defer session.Close()

	traffic := runtime.trafficForConnection(conn, remoteAddr.IP.String())
	runtime.mu.Lock()
	if runtime.closing || runtime.ctx.Err() != nil {
		runtime.mu.Unlock()
		return
	}
	if runtime.sessions == nil {
		runtime.sessions = make(map[*tcpstream.Session]struct{})
	}
	maxSessions := runtime.server.cfg.Transfer.TCP.MaxSessions
	if maxSessions > 0 && len(runtime.sessions) >= maxSessions {
		runtime.mu.Unlock()
		return
	}
	runtime.sessions[session] = struct{}{}
	runtime.mu.Unlock()
	defer func() {
		runtime.mu.Lock()
		delete(runtime.sessions, session)
		runtime.mu.Unlock()
	}()

	for {
		streamConn, maxPayload, err := session.AcceptStream()
		if err != nil {
			return
		}
		if !runtime.startSessionStreamForSession(streamConn, maxPayload, principal, traffic, session.ActiveStandby(), session) {
			_ = streamConn.Close()
			return
		}
	}
}

// startSessionStream reports whether the physical Session should keep accepting.
func (runtime *tcpServerRuntime) startSessionStream(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic) bool {
	return runtime.startSessionStreamWithCapability(conn, maxPayload, principal, traffic, false)
}

func (runtime *tcpServerRuntime) startSessionStreamWithCapability(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, activeStandby bool) bool {
	return runtime.startSessionStreamForSession(conn, maxPayload, principal, traffic, activeStandby, nil)
}

func (runtime *tcpServerRuntime) startSessionStreamForSession(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, activeStandby bool, session *tcpstream.Session) bool {
	runtime.mu.Lock()
	if runtime.closing || runtime.ctx.Err() != nil {
		runtime.mu.Unlock()
		return false
	}
	maxPendingDispatch := runtime.server.cfg.Transfer.TCP.MaxPendingStreams
	if activeStandby && maxPendingDispatch > 0 {
		maxInt := int(^uint(0) >> 1)
		resumeReserve := runtime.server.cfg.Transfer.TCP.MaxPendingResumes
		if resumeReserve > maxInt-maxPendingDispatch {
			maxPendingDispatch = maxInt
		} else {
			maxPendingDispatch += resumeReserve
		}
	}
	if maxPendingDispatch > 0 && runtime.pendingStreams >= maxPendingDispatch {
		runtime.mu.Unlock()
		_ = conn.Close()
		return true
	}
	runtime.pendingStreams++
	runtime.streamWG.Add(1)
	runtime.mu.Unlock()
	go func() {
		defer func() {
			runtime.mu.Lock()
			runtime.pendingStreams--
			runtime.mu.Unlock()
			runtime.streamWG.Done()
		}()
		runtime.acceptStreamForSession(conn, maxPayload, principal, traffic, activeStandby, session)
	}()
	return true
}

func (runtime *tcpServerRuntime) acceptStream(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic) {
	runtime.acceptStreamWithCapability(conn, maxPayload, principal, traffic, false)
}

func (runtime *tcpServerRuntime) acceptStreamWithCapability(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, activeStandby bool) {
	runtime.acceptStreamForSession(conn, maxPayload, principal, traffic, activeStandby, nil)
}

func (runtime *tcpServerRuntime) acceptStreamForSession(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, activeStandby bool, session *tcpstream.Session) {
	attached := false
	defer func() {
		if !attached {
			_ = conn.Close()
		}
	}()
	openTimeout := time.Duration(runtime.server.cfg.Transfer.TCP.OpenTimeoutMillis) * time.Millisecond
	if openTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(openTimeout))
	}
	frame, err := tcpstream.ReadFrame(conn, maxPayload)
	if err != nil {
		return
	}
	traffic.Control.RecordRX(tcpstream.HeaderSize + len(frame.Payload))
	_ = conn.SetDeadline(time.Time{})
	switch frame.Type {
	case tcpstream.FrameOpen:
		if !activeStandby && runtime.tryAcquireOpen() {
			attached = runtime.acceptLegacyOpen(conn, maxPayload, principal, traffic, frame)
			runtime.releaseOpen()
		} else {
			runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultGeneralFailure, traffic)
		}
	case tcpstream.FrameRecoverableOpen:
		if activeStandby {
			if runtime.tryAcquireOpen() {
				attached = runtime.acceptRecoverableOpen(conn, maxPayload, principal, traffic, frame)
				runtime.releaseOpen()
			} else {
				runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultGeneralFailure, traffic)
			}
		}
	case tcpstream.FrameResume:
		if activeStandby {
			attached = runtime.acceptResume(conn, maxPayload, principal, traffic, frame)
		}
	case tcpstream.FramePing:
		if activeStandby && runtime.tryAcquireProbe(session) {
			attached = true
			runtime.probeWG.Add(1)
			go func() {
				defer runtime.probeWG.Done()
				defer runtime.releaseProbe(session)
				defer conn.Close()
				runtime.acceptProbe(conn, maxPayload, traffic, frame)
			}()
		}
	}
}

func (runtime *tcpServerRuntime) tryAcquireProbe(session *tcpstream.Session) bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.probes == nil {
		runtime.probes = make(map[*tcpstream.Session]struct{})
	}
	if _, exists := runtime.probes[session]; exists {
		return false
	}
	runtime.probes[session] = struct{}{}
	return true
}

func (runtime *tcpServerRuntime) releaseProbe(session *tcpstream.Session) {
	runtime.mu.Lock()
	delete(runtime.probes, session)
	runtime.mu.Unlock()
}

func (runtime *tcpServerRuntime) tryAcquireOpen() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	limit := runtime.server.cfg.Transfer.TCP.MaxPendingStreams
	if limit > 0 && runtime.pendingOpens >= limit {
		return false
	}
	runtime.pendingOpens++
	return true
}

func (runtime *tcpServerRuntime) releaseOpen() {
	runtime.mu.Lock()
	if runtime.pendingOpens > 0 {
		runtime.pendingOpens--
	}
	runtime.mu.Unlock()
}

func (runtime *tcpServerRuntime) acceptProbe(conn net.Conn, maxPayload uint32, traffic *tcpServerTraffic, frame tcpstream.Frame) {
	for {
		if frame.Type != tcpstream.FramePing || !runtime.writeServerFrame(conn, tcpstream.Frame{Type: tcpstream.FramePong, Direction: tcpstream.DirectionServerToClient, Offset: frame.Offset}, traffic) {
			return
		}
		var err error
		frame, err = tcpstream.ReadFrame(conn, maxPayload)
		if err != nil {
			return
		}
		traffic.Control.RecordRX(tcpstream.HeaderSize + len(frame.Payload))
	}
}

func (runtime *tcpServerRuntime) acceptLegacyOpen(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, frame tcpstream.Frame) bool {
	destination, err := runtime.destinationForOpen(frame)
	if err != nil {
		runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultPolicyDenied, traffic)
		return false
	}
	stream, err := runtime.getOrCreateMode(frame.StreamID, tcpstream.Version, destination, principal, false, tcpstream.ResumeToken{})
	if err != nil {
		runtime.writeOpenResult(conn, frame.StreamID, openResultForError(err), traffic)
		return false
	}
	stream.attachMu.Lock()
	defer stream.attachMu.Unlock()
	if runtime.isClosing() {
		runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	if tcpFlowTerminal(stream.flow) {
		runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	maxCarriers := runtime.server.cfg.Transfer.TCP.MaxCarriersPerStream
	if maxCarriers > 0 && stream.flow.CarrierCount() >= maxCarriers {
		runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	if !runtime.writeOpenResult(conn, frame.StreamID, tcpstream.OpenResultSuccess, traffic) {
		return false
	}
	if _, err := stream.flow.AttachLimitedObserved(conn, maxPayload, runtime.server.cfg.Transfer.TCP.MaxCarriersPerStream, tcpCarrierObserver(traffic)); err != nil {
		return false
	}
	stream.started = true
	if stream.openTimer != nil {
		stream.openTimer.Stop()
		stream.openTimer = nil
	}
	stream.flow.Start()
	return true
}

func (runtime *tcpServerRuntime) acceptRecoverableOpen(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, frame tcpstream.Frame) bool {
	open, err := tcpstream.DecodeRecoverableOpen(frame)
	if err != nil {
		return false
	}
	requestedRecovery := time.Duration(open.RecoveryTimeoutMillis) * time.Millisecond
	serverRetention := time.Duration(runtime.server.cfg.Transfer.TCP.ServerOrphanRetentionMillis) * time.Millisecond
	if requestedRecovery > serverRetention {
		runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	if !runtime.recoveryCapacityAvailable() {
		runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	destination := open.Destination.String()
	stream, err := runtime.getOrCreateMode(frame.StreamID, tcpstream.Version, destination, principal, true, open.ResumeToken)
	if err != nil {
		runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, openResultForError(err), traffic)
		return false
	}
	stream.attachMu.Lock()
	defer stream.attachMu.Unlock()
	if runtime.isClosing() || stream.started || stream.generation != frame.Offset {
		runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	if tcpFlowTerminal(stream.flow) {
		runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultGeneralFailure, traffic)
		return false
	}
	if !runtime.writeRecoverableOpenResult(conn, frame.StreamID, frame.Offset, tcpstream.OpenResultSuccess, traffic) {
		return false
	}
	carrier, err := stream.flow.AttachLimitedObserved(conn, maxPayload, 1, tcpCarrierObserver(traffic))
	if err != nil {
		return false
	}
	stream.started = true
	if stream.openTimer != nil {
		stream.openTimer.Stop()
		stream.openTimer = nil
	}
	stream.flow.Start()
	runtime.monitorRecoverableCarrier(stream, carrier)
	return true
}

func (runtime *tcpServerRuntime) acceptResume(conn net.Conn, maxPayload uint32, principal string, traffic *tcpServerTraffic, frame tcpstream.Frame) bool {
	if !runtime.tryAcquireResume() {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultBusy, traffic)
		return false
	}
	defer runtime.releaseResume()
	token, err := tcpstream.ResumeTokenFromFrame(frame)
	if err != nil {
		return false
	}
	runtime.mu.Lock()
	stream := runtime.streams[frame.StreamID]
	runtime.mu.Unlock()
	if stream == nil {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultExpired, traffic)
		return false
	}
	select {
	case <-stream.ready:
	case <-runtime.ctx.Done():
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultBusy, traffic)
		return false
	}
	stream.attachMu.Lock()
	defer stream.attachMu.Unlock()
	if runtime.isClosing() {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultBusy, traffic)
		return false
	}
	if stream.err != nil || stream.flow == nil {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultExpired, traffic)
		return false
	}
	if !stream.recoverable || stream.principal != principal || subtle.ConstantTimeCompare(stream.resumeToken[:], token[:]) != 1 || frame.Offset <= stream.generation {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultRejected, traffic)
		return false
	}
	if tcpFlowTerminal(stream.flow) {
		runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultExpired, traffic)
		return false
	}
	if !runtime.writeResumeResult(conn, frame.StreamID, frame.Offset, tcpstream.ResumeResultSuccess, traffic) {
		return false
	}
	carrier, err := stream.flow.ReplaceObserved(conn, maxPayload, frame.Offset, tcpCarrierObserver(traffic))
	if err != nil {
		return false
	}
	stream.generation = frame.Offset
	if !stream.started {
		stream.started = true
		if stream.openTimer != nil {
			stream.openTimer.Stop()
			stream.openTimer = nil
		}
		stream.flow.Start()
	}
	runtime.monitorRecoverableCarrier(stream, carrier)
	return true
}

func (runtime *tcpServerRuntime) destinationForOpen(frame tcpstream.Frame) (string, error) {
	destination, err := tcpstream.DecodeDestination(frame.Payload)
	if err != nil {
		return "", err
	}
	return destination.String(), nil
}

func (runtime *tcpServerRuntime) writeOpenResult(conn net.Conn, streamID tcpstream.StreamID, result tcpstream.OpenResult, traffic *tcpServerTraffic) bool {
	frame := tcpstream.Frame{Type: tcpstream.FrameOpenResult, Direction: tcpstream.DirectionServerToClient, StreamID: streamID, Payload: []byte{byte(result)}}
	return runtime.writeServerFrame(conn, frame, traffic)
}

func (runtime *tcpServerRuntime) writeRecoverableOpenResult(conn net.Conn, streamID tcpstream.StreamID, generation uint64, result tcpstream.OpenResult, traffic *tcpServerTraffic) bool {
	retentionMillis := uint32(0)
	if result == tcpstream.OpenResultSuccess {
		retentionMillis = uint32(runtime.server.cfg.Transfer.TCP.ServerOrphanRetentionMillis)
	}
	frame := (tcpstream.RecoverableOpenResult{Result: result, Generation: generation, ServerOrphanRetentionMillis: retentionMillis}).Frame(streamID)
	return runtime.writeServerFrame(conn, frame, traffic)
}

func (runtime *tcpServerRuntime) writeResumeResult(conn net.Conn, streamID tcpstream.StreamID, generation uint64, result tcpstream.ResumeResult, traffic *tcpServerTraffic) bool {
	return runtime.writeServerFrame(conn, tcpstream.NewResumeResultFrame(streamID, generation, result), traffic)
}

func (runtime *tcpServerRuntime) writeServerFrame(conn net.Conn, frame tcpstream.Frame, traffic *tcpServerTraffic) bool {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(runtime.server.cfg.Transfer.TCP.WriteTimeoutMillis) * time.Millisecond))
	if err := tcpstream.WriteFrame(conn, frame); err != nil {
		traffic.Control.RecordDrop(tcpstream.HeaderSize + len(frame.Payload))
		return false
	}
	traffic.Control.RecordTX(tcpstream.HeaderSize + len(frame.Payload))
	_ = conn.SetWriteDeadline(time.Time{})
	return true
}

func (runtime *tcpServerRuntime) trafficForAddress(address string) *tcpServerTraffic {
	return runtime.trafficForAddressAt(address, time.Now())
}

func (runtime *tcpServerRuntime) trafficForAddressAt(address string, now time.Time) *tcpServerTraffic {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	traffic := runtime.trafficForAddressLocked(address, now)
	runtime.trimTrafficLocked()
	return traffic
}

func (runtime *tcpServerRuntime) trafficForConnection(conn net.Conn, address string) *tcpServerTraffic {
	now := time.Now()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	traffic := runtime.trafficForAddressLocked(address, now)
	if tracked, ok := conn.(*tcpServerConn); ok {
		if _, registered := runtime.connections[tracked]; registered && tracked.traffic == nil {
			tracked.traffic = traffic
			if traffic.active == 0 {
				runtime.removeInactiveTrafficLocked(traffic)
			}
			traffic.active++
		}
	}
	runtime.trimTrafficLocked()
	return traffic
}

func (runtime *tcpServerRuntime) trafficForAddressLocked(address string, now time.Time) *tcpServerTraffic {
	runtime.ensureTrafficTrackingLocked()
	traffic := runtime.traffic[address]
	if traffic == nil {
		traffic = &tcpServerTraffic{address: address}
		runtime.traffic[address] = traffic
		traffic.inactiveElement = runtime.inactiveTraffic.PushBack(traffic)
	} else if traffic.active == 0 && traffic.inactiveElement != nil {
		runtime.inactiveTraffic.MoveToBack(traffic.inactiveElement)
	}
	traffic.touch(now)
	return traffic
}

func (runtime *tcpServerRuntime) trafficLimit() int {
	// Bounded admission retains every address that can own a configured TCP
	// resource. Unlimited admission uses a cache-only safety limit instead.
	streams := runtime.server.cfg.Transfer.TCP.MaxStreams
	carriers := runtime.server.cfg.Transfer.TCP.MaxCarriersPerStream
	pending := runtime.server.cfg.Transfer.TCP.MaxPendingConnections
	if streams == 0 || carriers == 0 || pending == 0 {
		return tcpServerTrafficCacheSafetyLimit
	}
	maxInt := int(^uint(0) >> 1)
	if streams > (maxInt-pending)/carriers {
		return maxInt
	}
	return streams*carriers + pending
}

func (runtime *tcpServerRuntime) ensureTrafficTrackingLocked() {
	if runtime.inactiveTraffic != nil {
		return
	}
	inactive := make([]*tcpServerTraffic, 0, len(runtime.traffic))
	for address, traffic := range runtime.traffic {
		if traffic.address == "" {
			traffic.address = address
		}
		traffic.inactiveElement = nil
		if traffic.active == 0 {
			inactive = append(inactive, traffic)
		}
	}
	sort.Slice(inactive, func(left, right int) bool { return inactive[left].usedAt().Before(inactive[right].usedAt()) })
	runtime.inactiveTraffic = list.New()
	for _, traffic := range inactive {
		traffic.inactiveElement = runtime.inactiveTraffic.PushBack(traffic)
	}
}

func (runtime *tcpServerRuntime) markTrafficInactiveLocked(traffic *tcpServerTraffic) {
	if runtime.traffic[traffic.address] != traffic {
		return
	}
	runtime.ensureTrafficTrackingLocked()
	if traffic.inactiveElement == nil {
		traffic.inactiveElement = runtime.inactiveTraffic.PushBack(traffic)
	} else {
		runtime.inactiveTraffic.MoveToBack(traffic.inactiveElement)
	}
}

func (runtime *tcpServerRuntime) removeInactiveTrafficLocked(traffic *tcpServerTraffic) {
	if traffic.inactiveElement != nil {
		runtime.inactiveTraffic.Remove(traffic.inactiveElement)
		traffic.inactiveElement = nil
	}
}

func (runtime *tcpServerRuntime) removeTrafficLocked(traffic *tcpServerTraffic) {
	if runtime.traffic[traffic.address] != traffic {
		return
	}
	delete(runtime.traffic, traffic.address)
	runtime.removeInactiveTrafficLocked(traffic)
}

func (runtime *tcpServerRuntime) trimTrafficLocked() {
	runtime.ensureTrafficTrackingLocked()
	limit := runtime.trafficLimit()
	for len(runtime.traffic) > limit {
		oldest := runtime.inactiveTraffic.Front()
		if oldest == nil {
			return
		}
		runtime.removeTrafficLocked(oldest.Value.(*tcpServerTraffic))
	}
}

func (runtime *tcpServerRuntime) pruneTrafficLocked(now time.Time) {
	runtime.ensureTrafficTrackingLocked()
	for remaining := runtime.inactiveTraffic.Len(); remaining > 0; remaining-- {
		oldest := runtime.inactiveTraffic.Front()
		if oldest == nil {
			return
		}
		traffic := oldest.Value.(*tcpServerTraffic)
		if runtime.traffic[traffic.address] != traffic {
			runtime.removeInactiveTrafficLocked(traffic)
			continue
		}
		if traffic.active != 0 {
			runtime.inactiveTraffic.MoveToBack(oldest)
			continue
		}
		if traffic.usedAt().Add(tcpServerTrafficTTL).After(now) {
			return
		}
		runtime.removeTrafficLocked(traffic)
	}
}

func (runtime *tcpServerRuntime) pruneClosedLocked(now time.Time) {
	runtime.ensureClosedTrackingLocked()
	for {
		oldest := runtime.closedOrder.Front()
		if oldest == nil {
			return
		}
		streamID := oldest.Value.(tcpstream.StreamID)
		expiresAt, exists := runtime.closed[streamID]
		if !exists {
			runtime.closedOrder.Remove(oldest)
			delete(runtime.closedItems, streamID)
			continue
		}
		if expiresAt.After(now) {
			return
		}
		runtime.removeClosedLocked(streamID)
	}
}

func (runtime *tcpServerRuntime) rememberClosedLocked(streamID tcpstream.StreamID, now time.Time) {
	runtime.ensureClosedTrackingLocked()
	limit := runtime.server.cfg.Transfer.TCP.MaxStreams
	if limit == 0 {
		limit = tcpServerClosedCacheSafetyLimit
	}
	if element := runtime.closedItems[streamID]; element != nil {
		runtime.closedOrder.MoveToBack(element)
	} else {
		for len(runtime.closed) >= limit {
			oldest := runtime.closedOrder.Front()
			if oldest == nil {
				break
			}
			runtime.removeClosedLocked(oldest.Value.(tcpstream.StreamID))
		}
		runtime.closedItems[streamID] = runtime.closedOrder.PushBack(streamID)
	}
	runtime.closed[streamID] = now.Add(tcpServerClosedTTL)
}

func (runtime *tcpServerRuntime) ensureClosedTrackingLocked() {
	if runtime.closedOrder != nil && runtime.closedItems != nil && len(runtime.closedItems) == len(runtime.closed) {
		return
	}
	type closedEntry struct {
		streamID tcpstream.StreamID
		expires  time.Time
	}
	entries := make([]closedEntry, 0, len(runtime.closed))
	for streamID, expires := range runtime.closed {
		entries = append(entries, closedEntry{streamID: streamID, expires: expires})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].expires.Before(entries[right].expires) })
	runtime.closedOrder = list.New()
	runtime.closedItems = make(map[tcpstream.StreamID]*list.Element, len(runtime.closed))
	for _, entry := range entries {
		runtime.closedItems[entry.streamID] = runtime.closedOrder.PushBack(entry.streamID)
	}
}

func (runtime *tcpServerRuntime) removeClosedLocked(streamID tcpstream.StreamID) {
	delete(runtime.closed, streamID)
	if element := runtime.closedItems[streamID]; element != nil {
		runtime.closedOrder.Remove(element)
		delete(runtime.closedItems, streamID)
	}
}

func (runtime *tcpServerRuntime) pruneState(now time.Time) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.pruneStateLocked(now)
}

func (runtime *tcpServerRuntime) pruneStateLocked(now time.Time) {
	runtime.pruneClosedLocked(now)
	runtime.pruneTrafficLocked(now)
}

func tcpCarrierObserver(traffic *tcpServerTraffic) tcpstream.CarrierObserver {
	return tcpstream.CarrierObserver{
		Read: func(frame tcpstream.Frame) {
			recordTCPFrameRX(&traffic.Traffic, frame)
		},
		Write: func(frame tcpstream.Frame) {
			recordTCPFrameTX(&traffic.Traffic, frame)
		},
		Drop: func(frame tcpstream.Frame) {
			if frame.Type == tcpstream.FrameData {
				traffic.Data.RecordDrop(len(frame.Payload))
			} else {
				traffic.Control.RecordDrop(tcpstream.HeaderSize + len(frame.Payload))
			}
		},
		Skip: func(frame tcpstream.Frame) {
			if frame.Type == tcpstream.FrameData {
				traffic.Data.RecordSkip(len(frame.Payload))
			} else {
				traffic.Control.RecordSkip(tcpstream.HeaderSize + len(frame.Payload))
			}
		},
	}
}

func recordTCPFrameRX(traffic *stats.Traffic, frame tcpstream.Frame) {
	if frame.Type == tcpstream.FrameData {
		traffic.Data.RecordRX(len(frame.Payload))
	} else {
		traffic.Control.RecordRX(tcpstream.HeaderSize + len(frame.Payload))
	}
}

func recordTCPFrameTX(traffic *stats.Traffic, frame tcpstream.Frame) {
	if frame.Type == tcpstream.FrameData {
		traffic.Data.RecordTX(len(frame.Payload))
	} else {
		traffic.Control.RecordTX(tcpstream.HeaderSize + len(frame.Payload))
	}
}

func (runtime *tcpServerRuntime) getOrCreate(streamID tcpstream.StreamID, version uint8, destination, principal string) (*tcpServerStream, error) {
	return runtime.getOrCreateMode(streamID, version, destination, principal, false, tcpstream.ResumeToken{})
}

func (runtime *tcpServerRuntime) getOrCreateMode(streamID tcpstream.StreamID, version uint8, destination, principal string, recoverable bool, resumeToken tcpstream.ResumeToken) (*tcpServerStream, error) {
	runtime.mu.Lock()
	now := time.Now()
	if runtime.closing || runtime.ctx.Err() != nil {
		err := runtime.contextErrorLocked()
		runtime.mu.Unlock()
		return nil, err
	}
	if expiresAt, closed := runtime.closed[streamID]; closed {
		if expiresAt.After(now) {
			runtime.mu.Unlock()
			return nil, errors.New("TCP stream is closed")
		}
		runtime.removeClosedLocked(streamID)
	}
	if stream, ok := runtime.streams[streamID]; ok {
		if stream.version != version || stream.destination != destination || stream.principal != principal || stream.recoverable != recoverable {
			runtime.mu.Unlock()
			return nil, errors.New("TCP stream destination mismatch")
		}
		if recoverable {
			runtime.mu.Unlock()
			return nil, errors.New("recoverable TCP stream already exists")
		}
		runtime.mu.Unlock()
		select {
		case <-stream.ready:
			return stream, stream.err
		case <-runtime.ctx.Done():
			return nil, runtime.ctx.Err()
		}
	}
	maxStreams := runtime.server.cfg.Transfer.TCP.MaxStreams
	if maxStreams > 0 && len(runtime.streams) >= maxStreams {
		runtime.mu.Unlock()
		return nil, errors.New("maximum TCP streams reached")
	}
	stream := &tcpServerStream{
		ready:       make(chan struct{}),
		version:     version,
		destination: destination,
		principal:   principal,
		recoverable: recoverable,
		resumeToken: resumeToken,
	}
	runtime.nextStreamOrder++
	stream.order = runtime.nextStreamOrder
	if recoverable {
		stream.generation = 1
	}
	runtime.streams[streamID] = stream
	runtime.mu.Unlock()

	timeout := time.Duration(runtime.server.cfg.Transfer.TCP.DialTimeoutMillis) * time.Millisecond
	endpoint, err := dialTCPDestination(runtime.ctx, destination, timeout)
	runtime.mu.Lock()
	if err != nil {
		stream.err = err
		delete(runtime.streams, streamID)
		if !runtime.closing && runtime.ctx.Err() == nil {
			runtime.rememberClosedLocked(streamID, time.Now())
		}
		close(stream.ready)
		runtime.mu.Unlock()
		return nil, err
	}
	if runtime.closing || runtime.ctx.Err() != nil {
		stream.err = runtime.contextErrorLocked()
		delete(runtime.streams, streamID)
		close(stream.ready)
		runtime.mu.Unlock()
		_ = endpoint.Close()
		return nil, stream.err
	}
	flow := tcpstream.NewFlow(streamID, endpoint, tcpstream.DirectionServerToClient, runtime.flowConfigFor(recoverable))
	stream.flow = flow
	openTimeout := time.Duration(runtime.server.cfg.Transfer.TCP.OpenTimeoutMillis) * time.Millisecond
	if openTimeout > 0 {
		stream.attachMu.Lock()
		stream.openTimer = newTCPServerOpenTimer(openTimeout, func() {
			runtime.expireUnstartedStream(stream)
		})
		stream.attachMu.Unlock()
	}
	runtime.flowWG.Add(1)
	close(stream.ready)
	runtime.mu.Unlock()
	go func() {
		defer runtime.flowWG.Done()
		<-flow.Done()
		stream.attachMu.Lock()
		if stream.openTimer != nil {
			stream.openTimer.Stop()
			stream.openTimer = nil
		}
		stream.attachMu.Unlock()
		runtime.mu.Lock()
		if runtime.streams[streamID] == stream {
			delete(runtime.streams, streamID)
		}
		if !runtime.closing && runtime.ctx.Err() == nil {
			runtime.rememberClosedLocked(streamID, time.Now())
		}
		runtime.mu.Unlock()
	}()
	return stream, nil
}

func (runtime *tcpServerRuntime) expireUnstartedStream(stream *tcpServerStream) {
	stream.attachMu.Lock()
	defer stream.attachMu.Unlock()
	stream.openTimer = nil
	if stream.started || stream.flow == nil {
		return
	}
	if tcpFlowTerminal(stream.flow) {
		return
	}
	stream.flow.Reset(errTCPStreamOpenTimeout)
}

func tcpFlowTerminal(flow *tcpstream.Flow) bool {
	if flow == nil {
		return true
	}
	state := flow.State()
	return state == tcpstream.FlowStateCompleted || state == tcpstream.FlowStateFailed
}

func (runtime *tcpServerRuntime) monitorRecoverableCarrier(stream *tcpServerStream, carrier *tcpstream.Carrier) {
	runtime.recoveryWG.Add(1)
	go func() {
		defer runtime.recoveryWG.Done()
		<-carrier.Detached()
		if stream.flow.State() == tcpstream.FlowStateRecovering {
			runtime.enforceRecoveryLimits()
		}
	}()
}

type tcpServerRecoveringStream struct {
	flow         *tcpstream.Flow
	order        uint64
	historyBytes int64
}

func (runtime *tcpServerRuntime) recoveryCapacityAvailable() bool {
	tcpConfig := runtime.server.cfg.Transfer.TCP
	runtime.recoveryMu.Lock()
	defer runtime.recoveryMu.Unlock()
	streams, bytes := runtime.recoveringStreams()
	return len(streams) < tcpConfig.MaxRecoveringStreams && bytes < tcpConfig.MaxRecoveryBytes
}

func (runtime *tcpServerRuntime) enforceRecoveryLimits() {
	tcpConfig := runtime.server.cfg.Transfer.TCP
	runtime.recoveryMu.Lock()
	defer runtime.recoveryMu.Unlock()
	streams, totalBytes := runtime.recoveringStreams()
	if len(streams) <= tcpConfig.MaxRecoveringStreams && totalBytes <= tcpConfig.MaxRecoveryBytes {
		return
	}
	sort.Slice(streams, func(left, right int) bool { return streams[left].order > streams[right].order })
	remaining := len(streams)
	for _, stream := range streams {
		if remaining <= tcpConfig.MaxRecoveringStreams && totalBytes <= tcpConfig.MaxRecoveryBytes {
			break
		}
		stream.flow.Reset(tcpstream.ErrNoCarriers)
		totalBytes -= stream.historyBytes
		remaining--
	}
}

func (runtime *tcpServerRuntime) recoveringStreams() ([]tcpServerRecoveringStream, int64) {
	runtime.mu.Lock()
	streams := make([]*tcpServerStream, 0, len(runtime.streams))
	for _, stream := range runtime.streams {
		streams = append(streams, stream)
	}
	runtime.mu.Unlock()
	recovering := make([]tcpServerRecoveringStream, 0, len(streams))
	var totalBytes int64
	for _, stream := range streams {
		if !stream.recoverable || stream.flow == nil || stream.flow.State() != tcpstream.FlowStateRecovering {
			continue
		}
		historyBytes := int64(stream.flow.HistoryBytes())
		recovering = append(recovering, tcpServerRecoveringStream{flow: stream.flow, order: stream.order, historyBytes: historyBytes})
		totalBytes += historyBytes
	}
	return recovering, totalBytes
}

func openResultForError(err error) tcpstream.OpenResult {
	var netError net.Error
	var dnsError *net.DNSError
	platformResult := platformOpenResult(err)
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netError) && netError.Timeout():
		return tcpstream.OpenResultTimeout
	case errors.As(err, &dnsError) && dnsError.IsNotFound:
		return tcpstream.OpenResultHostUnreachable
	case platformResult != tcpstream.OpenResultGeneralFailure:
		return platformResult
	default:
		return tcpstream.OpenResultGeneralFailure
	}
}

func (runtime *tcpServerRuntime) flowConfig() tcpstream.FlowConfig {
	return runtime.flowConfigFor(runtime.server.cfg.Transfer.TCP.ActiveStandby())
}

func (runtime *tcpServerRuntime) flowConfigFor(activeStandby bool) tcpstream.FlowConfig {
	tcpConfig := runtime.server.cfg.Transfer.TCP
	flowConfig := tcpstream.FlowConfig{
		ChunkSize:          tcpConfig.ChunkSize,
		CarrierQueueBytes:  tcpConfig.CarrierQueueBytes,
		ReorderWindowBytes: tcpConfig.ReorderWindowBytes,
		WriteTimeout:       time.Duration(tcpConfig.WriteTimeoutMillis) * time.Millisecond,
	}
	if activeStandby {
		flowConfig.RecoveryTimeout = time.Duration(tcpConfig.ServerOrphanRetentionMillis) * time.Millisecond
		flowConfig.SingleCarrier = true
	}
	return flowConfig
}

func (runtime *tcpServerRuntime) sessionConfig() tcpstream.SessionConfig {
	transfer := runtime.server.cfg.Transfer
	return tcpstream.SessionConfig{
		KeepaliveInterval: time.Duration(transfer.KeepaliveIntervalMillis) * time.Millisecond,
		KeepaliveTimeout:  time.Duration(transfer.KeepaliveTimeoutMillis) * time.Millisecond,
		ReceiveBuffer:     transfer.TCP.ReorderWindowBytes,
		StreamBuffer:      transfer.TCP.CarrierQueueBytes,
		ActiveStandby:     transfer.TCP.ActiveStandby(),
		ServerInstanceID:  runtime.instanceID,
		OrphanRetention:   time.Duration(transfer.TCP.ServerOrphanRetentionMillis) * time.Millisecond,
	}
}

func (runtime *tcpServerRuntime) isClosing() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.closing || runtime.ctx.Err() != nil
}

func (runtime *tcpServerRuntime) contextErrorLocked() error {
	if err := runtime.ctx.Err(); err != nil {
		return err
	}
	return net.ErrClosed
}

func (runtime *tcpServerRuntime) shutdown() {
	runtime.mu.Lock()
	runtime.closing = true
	sessions := make([]*tcpstream.Session, 0, len(runtime.sessions))
	for session := range runtime.sessions {
		sessions = append(sessions, session)
	}
	connections := make([]*tcpServerConn, 0, len(runtime.connections))
	for conn := range runtime.connections {
		connections = append(connections, conn)
	}
	flows := make([]*tcpstream.Flow, 0, len(runtime.streams))
	for _, stream := range runtime.streams {
		if stream.flow != nil {
			flows = append(flows, stream.flow)
		}
	}
	runtime.mu.Unlock()
	if runtime.cancel != nil {
		runtime.cancel()
	}
	if runtime.listener != nil {
		_ = runtime.listener.Close()
	}
	for _, session := range sessions {
		_ = session.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
	for _, flow := range flows {
		_ = flow.Close()
	}
	runtime.acceptWG.Wait()
	runtime.streamWG.Wait()
	runtime.probeWG.Wait()
	runtime.recoveryWG.Wait()
	runtime.flowWG.Wait()
	runtime.backgroundWG.Wait()
	runtime.mu.Lock()
	runtime.streams = make(map[tcpstream.StreamID]*tcpServerStream)
	runtime.closed = make(map[tcpstream.StreamID]time.Time)
	runtime.closedOrder = list.New()
	runtime.closedItems = make(map[tcpstream.StreamID]*list.Element)
	runtime.sessions = make(map[*tcpstream.Session]struct{})
	runtime.probes = make(map[*tcpstream.Session]struct{})
	runtime.pendingStreams = 0
	runtime.pendingOpens = 0
	runtime.connections = make(map[*tcpServerConn]struct{})
	runtime.traffic = make(map[string]*tcpServerTraffic)
	runtime.inactiveTraffic = list.New()
	runtime.mu.Unlock()
}
