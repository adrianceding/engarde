package clientrole

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/adrianceding/engarde/internal/tcpstream"
	log "github.com/sirupsen/logrus"
)

const (
	tcpSessionRetryInitialDelay    = 100 * time.Millisecond
	tcpSessionRetryMaxDelay        = 5 * time.Second
	tcpFlowRetryStablePeriod       = tcpSessionRetryMaxDelay
	tcpSessionProbeActiveInterval  = 250 * time.Millisecond
	tcpSessionProbeStandbyInterval = time.Second
	tcpSessionProbeTimeout         = 400 * time.Millisecond
	tcpSessionProbeFailureLimit    = 2
)

var newTCPSessionRetryTimer = func(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

var newTCPFlowRetryTimer = func(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

var newTCPFlowOpenTimer = func(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

var newTCPSessionProbeTimer = func(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

var tcpRetryNow = time.Now

var jitterTCPSessionRetryDelay = func(delay time.Duration) time.Duration {
	spread := delay / 5
	return applyTCPSessionRetryJitter(delay, time.Duration(rand.Int64N(int64(2*spread)+1)))
}

func applyTCPSessionRetryJitter(delay, offset time.Duration) time.Duration {
	spread := delay / 5
	jittered := delay - spread + offset
	if jittered > tcpSessionRetryMaxDelay {
		return tcpSessionRetryMaxDelay
	}
	return jittered
}

// tcpPathSession owns the one long-lived physical TCP session for a path.
// Logical Flow carriers are opened inside it and never own its lifetime.
type tcpPathSession struct {
	runtime       *tcpClientRuntime
	interfaceName string
	path          tcpClientPath
	ctx           context.Context
	cancel        context.CancelFunc

	mu            sync.Mutex
	session       *tcpstream.Session
	generation    uint64
	inFlight      bool
	retrying      bool
	retryCount    int
	closed        bool
	done          chan struct{}
	probe         *tcpstream.SessionProbe
	rttEWMA       float64
	jitterEWMA    float64
	penalty       float64
	penaltyAt     time.Time
	cooldownUntil time.Time
	lastProbe     time.Time
	probeFailures int
	activeFlows   int
}

// tcpCarrierGroup owns only the virtual carriers for one logical Flow.
type tcpCarrierGroup struct {
	runtime *tcpClientRuntime

	mu                    sync.Mutex
	startMu               sync.Mutex
	slots                 map[string]*tcpFlowSlot
	flow                  *tcpstream.Flow
	destination           tcpstream.Destination
	beforeAttach          func() error
	onOpenFailed          func(error)
	started               bool
	committed             bool
	startedReady          chan struct{}
	openResults           [tcpstream.OpenResultPolicyDenied + 1]bool
	failed                bool
	closed                bool
	done                  chan struct{}
	resumeToken           tcpstream.ResumeToken
	carrierGeneration     uint64
	serverInstanceID      tcpstream.ServerInstanceID
	activeInterface       string
	activeCarrier         *tcpstream.Carrier
	activePathSession     *tcpPathSession
	retiringCarrier       *tcpstream.Carrier
	activeQueued          bool
	activeWork            tcpActiveWorkKind
	activeQueueGeneration uint64
	activeInFlight        bool
	activePending         tcpActiveWorkKind
	activeExecuting       tcpActiveWorkKind
	order                 uint64
	initialUncertain      bool
	degradedSince         time.Time
	lastMigration         time.Time
}

type tcpFlowSlot struct {
	path              tcpClientPath
	session           *tcpstream.Session
	sessionGeneration uint64
	carrier           *tcpstream.Carrier
	inFlight          bool
	retrying          bool
	retryCount        int
	openRejected      bool
}

func newTCPPathSession(runtime *tcpClientRuntime, interfaceName string, path tcpClientPath) *tcpPathSession {
	parent := runtime.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &tcpPathSession{
		runtime:       runtime,
		interfaceName: interfaceName,
		path:          path,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}
}

func newTCPCarrierGroup(runtime *tcpClientRuntime) *tcpCarrierGroup {
	return &tcpCarrierGroup{
		runtime:      runtime,
		slots:        make(map[string]*tcpFlowSlot),
		startedReady: make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// refreshCarrierGroups reconciles physical path sessions and then updates the
// path view of every live Flow group. The name is retained for its call sites.
func (runtime *tcpClientRuntime) refreshCarrierGroups(paths map[string]tcpClientPath) bool {
	runtime.mu.Lock()
	if runtime.closing || (runtime.ctx != nil && runtime.ctx.Err() != nil) {
		runtime.mu.Unlock()
		return false
	}
	if runtime.sessions == nil {
		runtime.sessions = make(map[string]*tcpPathSession)
	}
	stable := equalTCPPaths(runtime.paths, paths) && len(runtime.sessions) == len(paths)
	if stable {
		for interfaceName, path := range paths {
			if pathSession := runtime.sessions[interfaceName]; pathSession == nil || pathSession.path != path {
				stable = false
				break
			}
		}
	}
	if stable {
		runtime.mu.Unlock()
		return true
	}

	runtime.setPathsLocked(paths)
	stale := make([]*tcpPathSession, 0)
	started := make([]*tcpPathSession, 0)
	for interfaceName, pathSession := range runtime.sessions {
		path, exists := paths[interfaceName]
		if exists && path == pathSession.path {
			continue
		}
		delete(runtime.sessions, interfaceName)
		stale = append(stale, pathSession)
	}
	for interfaceName, path := range paths {
		if runtime.sessions[interfaceName] != nil {
			continue
		}
		pathSession := newTCPPathSession(runtime, interfaceName, path)
		runtime.sessions[interfaceName] = pathSession
		started = append(started, pathSession)
	}
	groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
	for group := range runtime.groups {
		groups = append(groups, group)
	}
	runtime.mu.Unlock()

	for _, pathSession := range stale {
		pathSession.close()
	}
	for _, group := range groups {
		group.syncPaths(paths)
	}
	for _, pathSession := range started {
		pathSession.reconcile()
	}
	return true
}

func (runtime *tcpClientRuntime) assignCarrierGroup(flow *tcpstream.Flow, destination tcpstream.Destination, beforeAttach func() error, onOpenFailed func(error)) (*tcpCarrierGroup, error) {
	var resumeToken tcpstream.ResumeToken
	if runtime.client.cfg.Transfer.TCP.ActiveStandby() {
		var err error
		resumeToken, err = tcpstream.NewResumeToken()
		if err != nil {
			return nil, err
		}
	}
	runtime.mu.Lock()
	if runtime.closing || (runtime.ctx != nil && runtime.ctx.Err() != nil) {
		runtime.mu.Unlock()
		return nil, net.ErrClosed
	}
	maxStreams := runtime.client.cfg.Transfer.TCP.MaxStreams
	if maxStreams > 0 && len(runtime.flows) >= maxStreams {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("maximum TCP streams %d reached", maxStreams)
	}
	if runtime.groups == nil {
		runtime.groups = make(map[*tcpCarrierGroup]struct{})
	}
	if runtime.flows == nil {
		runtime.flows = make(map[tcpstream.StreamID]*tcpstream.Flow)
	}
	if runtime.carriers == nil {
		runtime.carriers = make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier)
	}
	paths := cloneTCPPaths(runtime.paths)
	group := newTCPCarrierGroup(runtime)
	group.resumeToken = resumeToken
	runtime.nextFlowOrder++
	group.order = runtime.nextFlowOrder
	runtime.groups[group] = struct{}{}
	runtime.flows[flow.ID()] = flow
	runtime.carriers[flow.ID()] = make(map[string]*tcpstream.Carrier)
	runtime.mu.Unlock()

	group.syncPaths(paths)
	group.assign(flow, destination, beforeAttach, onOpenFailed)
	return group, nil
}

func (runtime *tcpClientRuntime) releaseCarrierGroup(group *tcpCarrierGroup) {
	group.close()
	group.mu.Lock()
	flow := group.flow
	group.mu.Unlock()
	runtime.mu.Lock()
	if flow != nil {
		delete(runtime.flows, flow.ID())
		delete(runtime.carriers, flow.ID())
	}
	delete(runtime.groups, group)
	runtime.mu.Unlock()
}

func (pathSession *tcpPathSession) reconcile() {
	pathSession.mu.Lock()
	if pathSession.closed || pathSession.session != nil || pathSession.inFlight || pathSession.retrying {
		pathSession.mu.Unlock()
		return
	}
	pathSession.inFlight = true
	pathSession.generation++
	generation := pathSession.generation
	pathSession.mu.Unlock()
	if !pathSession.runtime.startGroupWorker(func() { pathSession.dial(generation) }) {
		pathSession.finishAttempt(generation)
	}
}

func (pathSession *tcpPathSession) dial(generation uint64) {
	tcpConfig := pathSession.runtime.client.cfg.Transfer.TCP
	conn, err := dialTCPOnInterface(
		pathSession.ctx,
		pathSession.path.destination,
		pathSession.path.address,
		pathSession.interfaceName,
		time.Duration(tcpConfig.DialTimeoutMillis)*time.Millisecond,
	)
	if err != nil {
		pathSession.recordFailure(tcpRetryNow())
		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithError(err).Debug("Can't create TCP session on interface '" + pathSession.interfaceName + "'")
		}
		pathSession.retry(generation)
		return
	}
	conn = &tcpLastReceivedConn{
		Conn:         conn,
		lastReceived: pathSession.runtime.lastReceivedForPath(pathSession.interfaceName, pathSession.path),
	}
	var credentials *tcpstream.PeerCredentials
	if configured := pathSession.runtime.client.cfg.PeerAuth; configured != nil {
		credentials = &tcpstream.PeerCredentials{Username: configured.Username, Password: configured.Password}
	}
	session, err := tcpstream.DialSessionWithAuth(
		conn,
		tcpstream.MaxPayloadSize,
		time.Duration(tcpConfig.OpenTimeoutMillis)*time.Millisecond,
		pathSession.runtime.sessionConfig(),
		credentials,
	)
	if err != nil {
		_ = conn.Close()
		pathSession.recordFailure(tcpRetryNow())
		pathSession.retry(generation)
		return
	}
	establishedAt := tcpRetryNow()
	var probe *tcpstream.SessionProbe
	var failedProbe *tcpstream.SessionProbe
	var probeRTT time.Duration
	var probedAt time.Time
	if tcpConfig.ActiveStandby() {
		probe, err = session.OpenProbe(time.Duration(tcpConfig.ResumeOpenTimeoutMillis) * time.Millisecond)
		if err != nil {
			_ = session.Close()
			pathSession.recordFailure(tcpRetryNow())
			pathSession.retry(generation)
			return
		}
		probeRTT, err = probe.Ping(tcpSessionProbeTimeout)
		if err != nil {
			if tcpSessionProbeProtocolError(err) {
				_ = session.Close()
				pathSession.recordFailure(tcpRetryNow())
				pathSession.retry(generation)
				return
			}
			failedProbe = probe
			probe = nil
		} else {
			probedAt = tcpRetryNow()
		}
	}

	pathSession.mu.Lock()
	if pathSession.closed || !pathSession.inFlight || pathSession.generation != generation {
		pathSession.mu.Unlock()
		_ = session.Close()
		pathSession.retireProbe(failedProbe)
		return
	}
	pathSession.session = session
	pathSession.probe = probe
	if probe != nil {
		pathSession.recordProbeRTTLocked(probeRTT, probedAt)
	} else if tcpConfig.ActiveStandby() {
		pathSession.lastProbe = time.Time{}
		pathSession.probeFailures = 1
	}
	pathSession.cooldownUntil = time.Time{}
	pathSession.inFlight = false
	pathSession.retrying = false
	pathSession.mu.Unlock()
	pathSession.retireProbe(failedProbe)
	pathSession.runtime.reconcileGroups(pathSession.interfaceName)

	if tcpConfig.ActiveStandby() {
		pathSession.monitorProbe(session)
	} else {
		select {
		case <-session.Done():
		case <-pathSession.done:
		}
	}
	pathSession.mu.Lock()
	current := pathSession.session == session
	if current {
		pathSession.session = nil
		pathSession.probe = nil
		if tcpSessionWasStable(establishedAt, tcpRetryNow()) {
			pathSession.retryCount = 0
		}
	}
	closed := pathSession.closed
	pathSession.mu.Unlock()
	if current && !closed {
		pathSession.recordFailure(tcpRetryNow())
		pathSession.retry(generation)
	}
}

func (pathSession *tcpPathSession) monitorProbe(session *tcpstream.Session) {
	defer func() { pathSession.retireProbe(pathSession.detachProbe(session)) }()
	for {
		pathSession.mu.Lock()
		if pathSession.closed || pathSession.session != session {
			pathSession.mu.Unlock()
			return
		}
		activeFlows := pathSession.activeFlows
		pathSession.mu.Unlock()
		interval := tcpSessionProbeStandbyInterval
		if activeFlows > 0 {
			interval = tcpSessionProbeActiveInterval
		}
		ticks, stopTimer := newTCPSessionProbeTimer(interval)
		select {
		case <-ticks:
			stopTimer()
		case <-session.Done():
			stopTimer()
			return
		case <-pathSession.done:
			stopTimer()
			return
		}

		pathSession.mu.Lock()
		if pathSession.closed || pathSession.session != session || session.IsClosed() {
			pathSession.mu.Unlock()
			return
		}
		probe := pathSession.probe
		pathSession.mu.Unlock()
		if probe == nil {
			tcpConfig := pathSession.runtime.client.cfg.Transfer.TCP
			opened, err := session.OpenProbe(time.Duration(tcpConfig.ResumeOpenTimeoutMillis) * time.Millisecond)
			if err != nil {
				if pathSession.recordProbeFailure(session) {
					return
				}
				continue
			}
			if !pathSession.installProbe(session, opened) {
				pathSession.retireProbe(opened)
				continue
			}
			probe = opened
		}
		rtt, err := probe.Ping(tcpSessionProbeTimeout)
		if err != nil {
			if tcpSessionProbeProtocolError(err) {
				_ = session.Close()
				return
			}
			if pathSession.clearProbe(session, probe) {
				pathSession.retireProbe(probe)
				if pathSession.recordProbeFailure(session) {
					return
				}
			}
			continue
		}
		if pathSession.recordProbeRTT(session, probe, rtt, tcpRetryNow()) {
			pathSession.runtime.considerActiveMigrations()
		}
	}
}

func tcpSessionProbeProtocolError(err error) bool {
	return errors.Is(err, tcpstream.ErrInvalidFrame) || errors.Is(err, tcpstream.ErrPayloadLength)
}

func (pathSession *tcpPathSession) installProbe(session *tcpstream.Session, probe *tcpstream.SessionProbe) bool {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.closed || pathSession.session != session || pathSession.probe != nil || session.IsClosed() {
		return false
	}
	pathSession.probe = probe
	return true
}

func (pathSession *tcpPathSession) clearProbe(session *tcpstream.Session, probe *tcpstream.SessionProbe) bool {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.session != session || pathSession.probe != probe {
		return false
	}
	pathSession.probe = nil
	return true
}

func (pathSession *tcpPathSession) recordProbeFailure(session *tcpstream.Session) bool {
	pathSession.mu.Lock()
	if pathSession.closed || pathSession.session != session || session.IsClosed() {
		pathSession.mu.Unlock()
		return true
	}
	if pathSession.probeFailures < tcpSessionProbeFailureLimit {
		pathSession.probeFailures++
	}
	hardFailure := pathSession.probeFailures >= tcpSessionProbeFailureLimit
	pathSession.mu.Unlock()
	if hardFailure {
		_ = session.Close()
	}
	return hardFailure
}

func (pathSession *tcpPathSession) detachProbe(session *tcpstream.Session) *tcpstream.SessionProbe {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.session != session {
		return nil
	}
	probe := pathSession.probe
	pathSession.probe = nil
	return probe
}

func (pathSession *tcpPathSession) recordProbeRTT(session *tcpstream.Session, probe *tcpstream.SessionProbe, sample time.Duration, now time.Time) bool {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.closed || pathSession.session != session || pathSession.probe != probe || session.IsClosed() {
		return false
	}
	pathSession.recordProbeRTTLocked(sample, now)
	return true
}

func (pathSession *tcpPathSession) recordProbeRTTLocked(sample time.Duration, now time.Time) {
	pathSession.probeFailures = 0
	if sample > 0 {
		pathSession.recordRTTLocked(sample, now)
		return
	}
	// Coarse clocks can report zero for a successful probe. Refresh liveness
	// without treating that sample as an exceptionally fast path.
	pathSession.lastProbe = now
	pathSession.decayPenaltyLocked(now)
}

func (pathSession *tcpPathSession) retireProbe(probe *tcpstream.SessionProbe) {
	if probe == nil {
		return
	}
	if pathSession.runtime.startGroupWorker(func() { _ = probe.Close() }) {
		return
	}
	_ = probe.Close()
}

func tcpSessionWasStable(establishedAt, endedAt time.Time) bool {
	return !establishedAt.IsZero() && endedAt.Sub(establishedAt) >= tcpSessionRetryMaxDelay
}

func (pathSession *tcpPathSession) finishAttempt(generation uint64) {
	pathSession.mu.Lock()
	if pathSession.generation == generation {
		pathSession.inFlight = false
	}
	pathSession.mu.Unlock()
}

func (pathSession *tcpPathSession) retry(generation uint64) {
	pathSession.mu.Lock()
	if pathSession.closed || pathSession.generation != generation || pathSession.session != nil {
		pathSession.mu.Unlock()
		return
	}
	pathSession.inFlight = false
	if pathSession.retrying {
		pathSession.mu.Unlock()
		return
	}
	baseDelay := tcpSessionRetryDelay(pathSession.retryCount)
	if baseDelay < tcpSessionRetryMaxDelay {
		pathSession.retryCount++
	}
	delay := jitterTCPSessionRetryDelay(baseDelay)
	pathSession.retrying = true
	pathSession.mu.Unlock()

	if pathSession.runtime.startGroupWorker(func() { pathSession.waitToRetry(generation, delay) }) {
		return
	}
	pathSession.mu.Lock()
	if pathSession.generation == generation {
		pathSession.retrying = false
	}
	pathSession.mu.Unlock()
}

func (pathSession *tcpPathSession) waitToRetry(generation uint64, delay time.Duration) {
	ticks, stopTimer := newTCPSessionRetryTimer(delay)
	defer stopTimer()
	select {
	case <-ticks:
	case <-pathSession.done:
		return
	case <-pathSession.ctx.Done():
		return
	}
	pathSession.mu.Lock()
	if pathSession.closed || pathSession.generation != generation || !pathSession.retrying || pathSession.session != nil {
		pathSession.mu.Unlock()
		return
	}
	pathSession.retrying = false
	pathSession.mu.Unlock()
	pathSession.reconcile()
}

func (pathSession *tcpPathSession) current(path tcpClientPath) (*tcpstream.Session, uint64, bool) {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.closed || pathSession.path != path || pathSession.session == nil || pathSession.session.IsClosed() {
		return nil, 0, false
	}
	return pathSession.session, pathSession.generation, true
}

func (pathSession *tcpPathSession) close() {
	pathSession.mu.Lock()
	if pathSession.closed {
		pathSession.mu.Unlock()
		return
	}
	pathSession.closed = true
	close(pathSession.done)
	pathSession.cancel()
	session := pathSession.session
	probe := pathSession.probe
	pathSession.session = nil
	pathSession.probe = nil
	pathSession.mu.Unlock()
	if session != nil {
		_ = session.Close()
	}
	if probe != nil {
		_ = probe.Close()
	}
}

func tcpSessionRetryDelay(retryCount int) time.Duration {
	delay := tcpSessionRetryInitialDelay
	for range retryCount {
		if delay >= tcpSessionRetryMaxDelay/2 {
			return tcpSessionRetryMaxDelay
		}
		delay *= 2
	}
	return delay
}

func (runtime *tcpClientRuntime) sessionConfig() tcpstream.SessionConfig {
	transfer := runtime.client.cfg.Transfer
	return tcpstream.SessionConfig{
		KeepaliveInterval: time.Duration(transfer.KeepaliveIntervalMillis) * time.Millisecond,
		KeepaliveTimeout:  time.Duration(transfer.KeepaliveTimeoutMillis) * time.Millisecond,
		ReceiveBuffer:     transfer.TCP.ReorderWindowBytes,
		StreamBuffer:      transfer.TCP.CarrierQueueBytes,
		ActiveStandby:     transfer.TCP.ActiveStandby(),
	}
}

func (runtime *tcpClientRuntime) currentPathSession(interfaceName string, path tcpClientPath) (*tcpstream.Session, uint64, bool) {
	runtime.mu.Lock()
	pathSession := runtime.sessions[interfaceName]
	currentPath, exists := runtime.paths[interfaceName]
	runtime.mu.Unlock()
	if pathSession == nil || !exists || currentPath != path {
		return nil, 0, false
	}
	return pathSession.current(path)
}

func (runtime *tcpClientRuntime) reconcileGroups(interfaceName string) {
	runtime.mu.Lock()
	groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
	for group := range runtime.groups {
		groups = append(groups, group)
	}
	runtime.mu.Unlock()
	for _, group := range groups {
		group.reconcile(interfaceName)
	}
}

func (group *tcpCarrierGroup) assign(flow *tcpstream.Flow, destination tcpstream.Destination, beforeAttach func() error, onOpenFailed func(error)) {
	group.mu.Lock()
	group.flow = flow
	group.destination = destination
	group.beforeAttach = beforeAttach
	group.onOpenFailed = onOpenFailed
	group.mu.Unlock()
	group.reconcileAll()

	timeout := time.Duration(group.runtime.client.cfg.Transfer.TCP.OpenTimeoutMillis) * time.Millisecond
	if !group.runtime.startGroupWorker(func() { group.watchOpen(flow, timeout) }) {
		flow.Reset(net.ErrClosed)
	}
}

func (group *tcpCarrierGroup) watchOpen(flow *tcpstream.Flow, timeout time.Duration) {
	ticks, stopTimer := newTCPFlowOpenTimer(timeout)
	defer stopTimer()
	var runtimeDone <-chan struct{}
	if group.runtime.ctx != nil {
		runtimeDone = group.runtime.ctx.Done()
	}
	select {
	case <-flow.Done():
	case <-group.startedReady:
	case <-ticks:
		group.failOpen(tcpstream.ErrNoCarriers)
	case <-runtimeDone:
		flow.Reset(group.runtime.ctx.Err())
	}
}

func (group *tcpCarrierGroup) syncPaths(paths map[string]tcpClientPath) {
	stale := make([]*tcpstream.Carrier, 0)
	group.mu.Lock()
	for interfaceName, slot := range group.slots {
		path, exists := paths[interfaceName]
		if exists && path == slot.path {
			continue
		}
		if slot.carrier != nil {
			stale = append(stale, slot.carrier)
		}
		delete(group.slots, interfaceName)
	}
	for interfaceName, path := range paths {
		if group.slots[interfaceName] == nil {
			group.slots[interfaceName] = &tcpFlowSlot{path: path}
		}
	}
	group.mu.Unlock()
	for _, carrier := range stale {
		carrier.Close()
	}
	group.reconcileAll()
}

func (group *tcpCarrierGroup) reconcileAll() {
	if group.activeStandby() {
		group.mu.Lock()
		recoverable := group.started || group.initialUncertain
		active := group.activeInterface != ""
		closed := group.closed || group.failed || group.flow == nil || group.flowDoneLocked()
		group.mu.Unlock()
		if closed || active {
			return
		}
		if recoverable {
			group.scheduleActive(tcpActiveWorkRecovery)
		} else {
			group.scheduleActive(tcpActiveWorkOpen)
		}
		return
	}
	group.mu.Lock()
	names := make([]string, 0, len(group.slots))
	for interfaceName := range group.slots {
		names = append(names, interfaceName)
	}
	group.mu.Unlock()
	for _, interfaceName := range names {
		group.reconcile(interfaceName)
	}
}

func (group *tcpCarrierGroup) reconcile(interfaceName string) {
	if group.activeStandby() {
		group.reconcileAll()
		return
	}
	group.mu.Lock()
	slot := group.slots[interfaceName]
	if group.closed || group.failed || group.flow == nil || slot == nil || group.flowDoneLocked() {
		group.mu.Unlock()
		return
	}
	path := slot.path
	group.mu.Unlock()

	session, generation, healthy := group.runtime.currentPathSession(interfaceName, path)
	if !healthy {
		return
	}
	group.mu.Lock()
	slot = group.slots[interfaceName]
	if group.closed || group.failed || slot == nil || slot.path != path || group.flowDoneLocked() {
		group.mu.Unlock()
		return
	}
	sameSession := slot.session == session && slot.sessionGeneration == generation
	if slot.carrier != nil && sameSession {
		group.mu.Unlock()
		return
	}
	if sameSession && (slot.inFlight || slot.retrying || slot.openRejected) {
		group.mu.Unlock()
		return
	}
	if !sameSession {
		slot.retrying = false
		slot.retryCount = 0
		slot.openRejected = false
	}
	oldCarrier := slot.carrier
	slot.carrier = nil
	slot.session = session
	slot.sessionGeneration = generation
	slot.inFlight = true
	group.mu.Unlock()
	if oldCarrier != nil {
		oldCarrier.Close()
	}
	if !group.runtime.startGroupWorker(func() { group.open(interfaceName, path, session, generation) }) {
		group.finishOpenAttempt(interfaceName, path, session, generation)
	}
}

func (group *tcpCarrierGroup) open(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64) {
	group.mu.Lock()
	if !group.validOpenAttemptLocked(group.slots[interfaceName], path, session, generation) || group.flowDoneLocked() {
		group.mu.Unlock()
		group.finishOpenAttempt(interfaceName, path, session, generation)
		return
	}
	flow := group.flow
	destination := group.destination
	group.mu.Unlock()
	timeout := time.Duration(group.runtime.client.cfg.Transfer.TCP.OpenTimeoutMillis) * time.Millisecond
	conn, maxPayload, err := session.OpenDestination(flow.ID(), destination, timeout)
	if err != nil {
		group.recordOpenError(err)
		var openErr *tcpstream.OpenError
		if errors.As(err, &openErr) {
			group.rejectOpenAttempt(interfaceName, path, session, generation)
			return
		}
		group.retry(interfaceName, path, session, generation, false)
		return
	}
	group.attach(interfaceName, path, session, generation, conn, maxPayload)
}

func (group *tcpCarrierGroup) attach(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64, conn net.Conn, maxPayload uint32) {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.mu.Lock()
	slot := group.slots[interfaceName]
	if !group.validOpenAttemptLocked(slot, path, session, generation) || group.flowDoneLocked() {
		group.mu.Unlock()
		_ = conn.Close()
		return
	}
	flow := group.flow
	first := !group.started
	needsCommit := first && !group.committed
	beforeAttach := group.beforeAttach
	group.mu.Unlock()

	traffic := group.runtime.trafficForPath(interfaceName, path)
	traffic.Control.RecordTX(tcpstream.HeaderSize)
	carrier, err := flow.AttachObserved(conn, maxPayload, tcpCarrierObserver(traffic))
	if err != nil {
		group.retry(interfaceName, path, session, generation, false)
		return
	}
	group.mu.Lock()
	slot = group.slots[interfaceName]
	if !group.validOpenAttemptLocked(slot, path, session, generation) || group.flowDoneLocked() {
		group.mu.Unlock()
		carrier.Close()
		return
	}
	group.mu.Unlock()
	select {
	case <-carrier.Done():
		group.retry(interfaceName, path, session, generation, false)
		return
	default:
	}
	if needsCommit && beforeAttach != nil {
		if err := beforeAttach(); err != nil {
			carrier.Close()
			group.failOpenLocked(err)
			return
		}
	}
	if needsCommit {
		group.mu.Lock()
		group.committed = true
		group.mu.Unlock()
	}
	select {
	case <-carrier.Done():
		group.retry(interfaceName, path, session, generation, false)
		return
	default:
	}

	group.mu.Lock()
	slot = group.slots[interfaceName]
	if !group.validOpenAttemptLocked(slot, path, session, generation) || group.flowDoneLocked() {
		group.mu.Unlock()
		carrier.Close()
		return
	}
	slot.carrier = carrier
	slot.inFlight = false
	slot.openRejected = false
	if first {
		group.started = true
		close(group.startedReady)
	}
	group.mu.Unlock()
	group.runtime.recordGroupCarrier(flow.ID(), interfaceName, carrier)
	if first {
		flow.Start()
	}
	if !group.runtime.startGroupWorker(func() { group.monitorCarrier(interfaceName, path, session, generation, carrier) }) {
		carrier.Close()
		group.removeCarrier(interfaceName, path, session, generation, carrier)
	}
}

func (group *tcpCarrierGroup) monitorCarrier(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64, carrier *tcpstream.Carrier) {
	attachedAt := tcpRetryNow()
	<-carrier.Done()
	if group.removeCarrier(interfaceName, path, session, generation, carrier) {
		group.retry(interfaceName, path, session, generation, tcpRetryNow().Sub(attachedAt) >= tcpFlowRetryStablePeriod)
	}
}

func (group *tcpCarrierGroup) removeCarrier(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64, carrier *tcpstream.Carrier) bool {
	group.mu.Lock()
	slot := group.slots[interfaceName]
	removed := false
	if slot != nil && slot.path == path && slot.session == session && slot.sessionGeneration == generation && slot.carrier == carrier {
		slot.carrier = nil
		removed = true
	}
	flow := group.flow
	group.mu.Unlock()
	if flow != nil {
		group.runtime.removeGroupCarrier(flow.ID(), interfaceName, carrier)
	}
	return removed
}

func (group *tcpCarrierGroup) finishOpenAttempt(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64) {
	group.mu.Lock()
	slot := group.slots[interfaceName]
	if slot != nil && slot.path == path && slot.session == session && slot.sessionGeneration == generation {
		slot.inFlight = false
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) rejectOpenAttempt(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64) {
	group.mu.Lock()
	slot := group.slots[interfaceName]
	if slot != nil && slot.path == path && slot.session == session && slot.sessionGeneration == generation {
		slot.inFlight = false
		slot.openRejected = true
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) retry(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64, resetBackoff bool) {
	group.mu.Lock()
	slot := group.slots[interfaceName]
	if group.closed || group.failed || slot == nil || slot.path != path || slot.session != session || slot.sessionGeneration != generation || slot.carrier != nil || group.flowDoneLocked() || session.IsClosed() {
		group.mu.Unlock()
		return
	}
	slot.inFlight = false
	if resetBackoff {
		slot.retryCount = 0
	}
	if slot.retrying {
		group.mu.Unlock()
		return
	}
	baseDelay := tcpSessionRetryDelay(slot.retryCount)
	if baseDelay < tcpSessionRetryMaxDelay {
		slot.retryCount++
	}
	delay := jitterTCPSessionRetryDelay(baseDelay)
	slot.retrying = true
	group.mu.Unlock()

	if group.runtime.startGroupWorker(func() { group.waitToRetry(interfaceName, path, session, generation, delay) }) {
		return
	}
	group.mu.Lock()
	if current := group.slots[interfaceName]; current == slot && current.path == path && current.session == session && current.sessionGeneration == generation {
		current.retrying = false
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) waitToRetry(interfaceName string, path tcpClientPath, session *tcpstream.Session, generation uint64, delay time.Duration) {
	ticks, stopTimer := newTCPFlowRetryTimer(delay)
	defer stopTimer()
	var runtimeDone <-chan struct{}
	if group.runtime.ctx != nil {
		runtimeDone = group.runtime.ctx.Done()
	}
	select {
	case <-ticks:
	case <-session.Done():
		return
	case <-runtimeDone:
		return
	case <-group.done:
		return
	}

	group.mu.Lock()
	slot := group.slots[interfaceName]
	if group.closed || group.failed || slot == nil || slot.path != path || slot.session != session || slot.sessionGeneration != generation || !slot.retrying || slot.carrier != nil || group.flowDoneLocked() || session.IsClosed() {
		group.mu.Unlock()
		return
	}
	slot.retrying = false
	group.mu.Unlock()
	group.reconcile(interfaceName)
}

func (group *tcpCarrierGroup) validOpenAttemptLocked(slot *tcpFlowSlot, path tcpClientPath, session *tcpstream.Session, generation uint64) bool {
	return !group.closed && !group.failed && slot != nil && slot.path == path && slot.inFlight && slot.session == session && slot.sessionGeneration == generation
}

func (group *tcpCarrierGroup) flowDoneLocked() bool {
	if group.flow == nil {
		return false
	}
	state := group.flow.State()
	return state == tcpstream.FlowStateCompleted || state == tcpstream.FlowStateFailed
}

func (group *tcpCarrierGroup) failOpen(err error) {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.failOpenLocked(err)
}

func (group *tcpCarrierGroup) failOpenLocked(err error) {
	group.mu.Lock()
	if group.started || group.failed || group.closed {
		group.mu.Unlock()
		return
	}
	if errors.Is(err, tcpstream.ErrNoCarriers) {
		err = group.preferredOpenErrorLocked(err)
	}
	group.failed = true
	flow := group.flow
	onOpenFailed := group.onOpenFailed
	committed := group.committed
	group.mu.Unlock()
	if onOpenFailed != nil && !committed {
		onOpenFailed(err)
	}
	if flow != nil {
		flow.Reset(err)
	}
}

func (group *tcpCarrierGroup) recordOpenError(err error) {
	var openErr *tcpstream.OpenError
	if !errors.As(err, &openErr) || openErr.Result > tcpstream.OpenResultPolicyDenied {
		return
	}
	group.mu.Lock()
	if !group.started && !group.failed && !group.closed {
		group.openResults[openErr.Result] = true
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) preferredOpenErrorLocked(fallback error) error {
	for _, result := range [...]tcpstream.OpenResult{
		tcpstream.OpenResultPolicyDenied,
		tcpstream.OpenResultConnectionRefused,
		tcpstream.OpenResultHostUnreachable,
		tcpstream.OpenResultNetworkUnreachable,
		tcpstream.OpenResultTimeout,
		tcpstream.OpenResultGeneralFailure,
	} {
		if group.openResults[result] {
			return &tcpstream.OpenError{Result: result}
		}
	}
	return fallback
}

func (group *tcpCarrierGroup) close() {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.mu.Lock()
	if group.closed {
		group.mu.Unlock()
		return
	}
	group.closed = true
	close(group.done)
	activeCarrier := group.activeCarrier
	activePathSession := group.activePathSession
	group.activeInterface = ""
	group.activeCarrier = nil
	group.activePathSession = nil
	group.retiringCarrier = nil
	carriers := make([]*tcpstream.Carrier, 0, len(group.slots))
	if activeCarrier != nil {
		carriers = append(carriers, activeCarrier)
	}
	for _, slot := range group.slots {
		if slot.carrier != nil && slot.carrier != activeCarrier {
			carriers = append(carriers, slot.carrier)
		}
		slot.carrier = nil
	}
	group.mu.Unlock()
	if activePathSession != nil {
		activePathSession.adjustActiveFlows(-1)
	}
	for _, carrier := range carriers {
		carrier.Close()
	}
}

func (group *tcpCarrierGroup) reset(err error) {
	group.mu.Lock()
	flow := group.flow
	group.mu.Unlock()
	if flow == nil {
		group.close()
		return
	}
	flow.Reset(err)
}

func (runtime *tcpClientRuntime) pathSessionStatus() (map[string]tcpPathQualityStatus, int) {
	runtime.mu.Lock()
	sessions := make(map[string]*tcpPathSession, len(runtime.sessions))
	for interfaceName, pathSession := range runtime.sessions {
		sessions[interfaceName] = pathSession
	}
	paths := cloneTCPPaths(runtime.paths)
	runtime.mu.Unlock()
	status := make(map[string]tcpPathQualityStatus, len(sessions))
	count := 0
	now := tcpRetryNow()
	for interfaceName, pathSession := range sessions {
		quality := pathSession.qualityStatus(paths[interfaceName], runtime.client.cfg.InterfaceHints[interfaceName].Cost, now)
		status[interfaceName] = quality
		if quality.active {
			count++
		}
	}
	return status, count
}

func (runtime *tcpClientRuntime) closeCarrierGroups() {
	runtime.mu.Lock()
	groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
	for group := range runtime.groups {
		groups = append(groups, group)
	}
	runtime.mu.Unlock()
	for _, group := range groups {
		group.close()
	}
}

func (runtime *tcpClientRuntime) recordGroupCarrier(streamID tcpstream.StreamID, interfaceName string, carrier *tcpstream.Carrier) {
	runtime.mu.Lock()
	if carriers := runtime.carriers[streamID]; carriers != nil {
		carriers[interfaceName] = carrier
	}
	runtime.mu.Unlock()
}

func (runtime *tcpClientRuntime) removeGroupCarrier(streamID tcpstream.StreamID, interfaceName string, carrier *tcpstream.Carrier) {
	runtime.mu.Lock()
	if carriers := runtime.carriers[streamID]; carriers != nil && carriers[interfaceName] == carrier {
		delete(carriers, interfaceName)
	}
	runtime.mu.Unlock()
}

func cloneTCPPaths(paths map[string]tcpClientPath) map[string]tcpClientPath {
	clone := make(map[string]tcpClientPath, len(paths))
	for interfaceName, path := range paths {
		clone[interfaceName] = path
	}
	return clone
}

func equalTCPPaths(first, second map[string]tcpClientPath) bool {
	if len(first) != len(second) {
		return false
	}
	for interfaceName, firstPath := range first {
		if secondPath, ok := second[interfaceName]; !ok || secondPath != firstPath {
			return false
		}
	}
	return true
}

func (runtime *tcpClientRuntime) setPathsLocked(paths map[string]tcpClientPath) {
	previous := runtime.paths
	runtime.paths = paths
	for interfaceName := range runtime.traffic {
		current, ok := paths[interfaceName]
		if !ok || previous[interfaceName] != current {
			delete(runtime.traffic, interfaceName)
		}
	}
	for interfaceName := range runtime.lastReceived {
		current, ok := paths[interfaceName]
		if !ok || previous[interfaceName] != current {
			delete(runtime.lastReceived, interfaceName)
		}
	}
}
