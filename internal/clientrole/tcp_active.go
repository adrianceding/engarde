package clientrole

import (
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

const (
	tcpActiveRetryInterval  = 100 * time.Millisecond
	tcpPathFailurePenalty   = 500 * time.Millisecond
	tcpPathLoadPenalty      = 5 * time.Millisecond
	tcpPathPenaltyHalfLife  = 30 * time.Second
	tcpPathDegradeHold      = 5 * time.Second
	tcpPathSwitchMargin     = 50 * time.Millisecond
	tcpPathMigrationCooloff = 30 * time.Second
)

type tcpActiveWorkKind uint8

const (
	tcpActiveWorkNone tcpActiveWorkKind = iota
	tcpActiveWorkMigration
	tcpActiveWorkOpen
	tcpActiveWorkRecovery
)

type tcpActiveCoordinator struct {
	runtime    *tcpClientRuntime
	recoveries chan tcpActiveQueueToken
	opens      chan tcpActiveQueueToken
	migrations chan tcpActiveQueueToken

	retryMu sync.Mutex
	retries map[*tcpCarrierGroup]tcpActiveWorkKind
}

type tcpActiveQueueToken struct {
	group      *tcpCarrierGroup
	generation uint64
}

type tcpActiveCandidate struct {
	interfaceName     string
	path              tcpClientPath
	pathSession       *tcpPathSession
	session           *tcpstream.Session
	sessionGeneration uint64
	serverInstanceID  tcpstream.ServerInstanceID
	score             float64
	degraded          bool
}

type tcpPathQualityStatus struct {
	active               bool
	state                string
	rttMillis            float64
	jitterMillis         float64
	scoreMillis          float64
	failurePenaltyMillis float64
	activeFlows          int
	serverInstanceID     string
}

func (runtime *tcpClientRuntime) startActiveCoordinator() {
	if !runtime.client.cfg.Transfer.TCP.ActiveStandby() || runtime.active != nil {
		return
	}
	tcpConfig := runtime.client.cfg.Transfer.TCP
	coordinator := &tcpActiveCoordinator{
		runtime:    runtime,
		recoveries: make(chan tcpActiveQueueToken, tcpConfig.MaxPendingResumes),
		opens:      make(chan tcpActiveQueueToken, tcpConfig.MaxStreams),
		migrations: make(chan tcpActiveQueueToken, tcpConfig.MaxPendingResumes),
		retries:    make(map[*tcpCarrierGroup]tcpActiveWorkKind),
	}
	runtime.active = coordinator
	for range tcpConfig.MaxConcurrentResumes {
		if !runtime.startGroupWorker(coordinator.run) {
			return
		}
	}
	_ = runtime.startGroupWorker(coordinator.retryLoop)
}

func (coordinator *tcpActiveCoordinator) run() {
	for {
		select {
		case token := <-coordinator.recoveries:
			token.group.runActiveWork(token.generation)
			continue
		default:
		}
		select {
		case token := <-coordinator.opens:
			token.group.runActiveWork(token.generation)
			continue
		default:
		}
		select {
		case token := <-coordinator.recoveries:
			token.group.runActiveWork(token.generation)
		case token := <-coordinator.opens:
			token.group.runActiveWork(token.generation)
		case token := <-coordinator.migrations:
			token.group.runActiveWork(token.generation)
		case <-coordinator.runtime.ctx.Done():
			return
		}
	}
}

func (coordinator *tcpActiveCoordinator) retryLoop() {
	ticker := time.NewTicker(tcpActiveRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			coordinator.retryPending()
		case <-coordinator.runtime.ctx.Done():
			return
		}
	}
}

func (coordinator *tcpActiveCoordinator) retryPending() {
	coordinator.retryMu.Lock()
	pending := coordinator.retries
	coordinator.retries = make(map[*tcpCarrierGroup]tcpActiveWorkKind)
	coordinator.retryMu.Unlock()
	for group, kind := range pending {
		group.scheduleActive(kind)
	}
}

func (coordinator *tcpActiveCoordinator) retry(group *tcpCarrierGroup, kind tcpActiveWorkKind) {
	coordinator.retryMu.Lock()
	if current := coordinator.retries[group]; current < kind {
		coordinator.retries[group] = kind
	}
	coordinator.retryMu.Unlock()
}

func (coordinator *tcpActiveCoordinator) enqueue(group *tcpCarrierGroup, generation uint64, kind tcpActiveWorkKind) bool {
	queue := coordinator.opens
	if kind == tcpActiveWorkRecovery {
		queue = coordinator.recoveries
	} else if kind == tcpActiveWorkMigration {
		queue = coordinator.migrations
	}
	select {
	case queue <- tcpActiveQueueToken{group: group, generation: generation}:
		return true
	default:
		return false
	}
}

func (group *tcpCarrierGroup) activeStandby() bool {
	return group.runtime.client.cfg.Transfer.TCP.ActiveStandby()
}

func (group *tcpCarrierGroup) scheduleActive(kind tcpActiveWorkKind) {
	coordinator := group.runtime.active
	if coordinator == nil {
		switch kind {
		case tcpActiveWorkOpen:
			group.failOpen(tcpstream.ErrNoCarriers)
		case tcpActiveWorkRecovery:
			group.reset(tcpstream.ErrNoCarriers)
		}
		return
	}
	group.mu.Lock()
	if group.closed || group.failed || group.flow == nil || group.flowDoneLocked() {
		group.mu.Unlock()
		return
	}
	if group.activeInFlight {
		if kind > group.activePending {
			group.activePending = kind
		}
		group.mu.Unlock()
		return
	}
	if group.activeQueued {
		if kind > group.activeWork {
			group.activeWork = kind
			generation := group.activeQueueGeneration + 1
			// A successful priority promotion invalidates the old token. If the
			// priority queue is full, the old token remains a valid fallback.
			if coordinator.enqueue(group, generation, kind) {
				group.activeQueueGeneration = generation
			}
		}
		group.mu.Unlock()
		return
	}
	generation := group.activeQueueGeneration + 1
	group.activeQueued = true
	group.activeWork = kind
	group.activeQueueGeneration = generation
	if coordinator.enqueue(group, generation, kind) {
		group.mu.Unlock()
		return
	}
	group.activeQueued = false
	group.activeWork = tcpActiveWorkNone
	closed := group.closed || group.failed || group.flow == nil || group.flowDoneLocked()
	group.mu.Unlock()
	if closed {
		return
	}
	switch kind {
	case tcpActiveWorkOpen:
		group.failOpen(tcpstream.ErrNoCarriers)
	case tcpActiveWorkRecovery:
		group.reset(tcpstream.ErrNoCarriers)
	}
}

func (group *tcpCarrierGroup) runActiveWork(generation uint64) {
	group.mu.Lock()
	if !group.activeQueued || generation != group.activeQueueGeneration || group.activeInFlight || group.closed || group.failed || group.flowDoneLocked() {
		group.mu.Unlock()
		return
	}
	kind := group.activeWork
	group.activeQueued = false
	group.activeWork = tcpActiveWorkNone
	group.activeInFlight = true
	group.activeExecuting = kind
	group.mu.Unlock()
	if kind == tcpActiveWorkRecovery {
		group.runActiveRecovery()
		return
	}
	if kind == tcpActiveWorkMigration {
		group.runActiveMigration()
		return
	}
	group.runActiveOpen()
}

func (group *tcpCarrierGroup) finishActiveWork(next tcpActiveWorkKind, delayed bool) {
	group.mu.Lock()
	wasMigration := group.activeExecuting == tcpActiveWorkMigration
	group.activeInFlight = false
	group.activeExecuting = tcpActiveWorkNone
	if wasMigration && group.started && group.activeInterface == "" && !group.closed && !group.failed && !group.flowDoneLocked() {
		next = tcpActiveWorkRecovery
		delayed = false
	}
	if group.activePending > next {
		next = group.activePending
		delayed = false
	}
	group.activePending = tcpActiveWorkNone
	closed := group.closed || group.failed || group.flowDoneLocked()
	group.mu.Unlock()
	if closed || next == tcpActiveWorkNone {
		return
	}
	if delayed {
		group.runtime.active.retry(group, next)
		return
	}
	group.scheduleActive(next)
}

func (group *tcpCarrierGroup) runActiveOpen() {
	candidate, ok := group.runtime.selectActiveCandidate(tcpstream.ServerInstanceID{})
	if !ok {
		group.finishActiveWork(tcpActiveWorkOpen, true)
		return
	}
	group.mu.Lock()
	flow := group.flow
	destination := group.destination
	token := group.resumeToken
	valid := !group.closed && !group.failed && !group.started && flow != nil && !group.flowDoneLocked()
	group.mu.Unlock()
	if !valid {
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	tcpConfig := group.runtime.client.cfg.Transfer.TCP
	conn, maxPayload, result, err := candidate.session.OpenRecoverableDestination(
		flow.ID(),
		destination,
		token,
		time.Duration(tcpConfig.ClientRecoveryTimeoutMillis)*time.Millisecond,
		time.Duration(tcpConfig.OpenTimeoutMillis)*time.Millisecond,
	)
	if err != nil {
		var openErr *tcpstream.OpenError
		if errors.As(err, &openErr) {
			group.recordOpenError(err)
			group.finishActiveWork(tcpActiveWorkNone, false)
			group.failOpen(err)
			return
		}
		if tcpstream.StreamRequestWasWritten(err) {
			group.markInitialUncertain(candidate)
			group.finishActiveWork(tcpActiveWorkRecovery, false)
			return
		}
		group.finishActiveWork(tcpActiveWorkOpen, false)
		return
	}
	acceptedRetention := time.Duration(result.ServerOrphanRetentionMillis) * time.Millisecond
	if acceptedRetention < time.Duration(tcpConfig.ClientRecoveryTimeoutMillis)*time.Millisecond {
		_ = conn.Close()
		group.finishActiveWork(tcpActiveWorkNone, false)
		group.failOpen(fmt.Errorf("server recovery retention %s is shorter than client recovery timeout", acceptedRetention))
		return
	}
	group.markInitialUncertain(candidate)
	if !group.attachActiveOpen(candidate, conn, maxPayload) {
		group.finishActiveWork(tcpActiveWorkRecovery, false)
		return
	}
	group.finishActiveWork(tcpActiveWorkNone, false)
}

func (group *tcpCarrierGroup) markInitialUncertain(candidate tcpActiveCandidate) {
	group.mu.Lock()
	if !group.closed && !group.failed && !group.started && !group.flowDoneLocked() {
		group.initialUncertain = true
		group.serverInstanceID = candidate.serverInstanceID
		group.carrierGeneration = 1
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) attachActiveOpen(candidate tcpActiveCandidate, conn net.Conn, maxPayload uint32) bool {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.mu.Lock()
	flow := group.flow
	beforeAttach := group.beforeAttach
	valid := !group.closed && !group.failed && !group.started && flow != nil && !group.flowDoneLocked()
	group.mu.Unlock()
	if !valid || !group.runtime.activeCandidateCurrent(candidate) {
		_ = conn.Close()
		return false
	}
	traffic := group.runtime.trafficForPath(candidate.interfaceName, candidate.path)
	carrier, err := flow.AttachObserved(conn, maxPayload, tcpCarrierObserver(traffic))
	if err != nil {
		return false
	}
	select {
	case <-carrier.Done():
		return false
	default:
	}
	if beforeAttach != nil {
		if err := beforeAttach(); err != nil {
			_ = carrier.Close()
			group.failOpenLocked(err)
			return false
		}
	}
	group.mu.Lock()
	slot := group.slots[candidate.interfaceName]
	if group.closed || group.failed || group.started || slot == nil || slot.path != candidate.path || group.flowDoneLocked() {
		group.mu.Unlock()
		_ = carrier.Close()
		return false
	}
	slot.session = candidate.session
	slot.sessionGeneration = candidate.sessionGeneration
	slot.carrier = carrier
	group.activeInterface = candidate.interfaceName
	group.activeCarrier = carrier
	group.activePathSession = candidate.pathSession
	group.serverInstanceID = candidate.serverInstanceID
	group.carrierGeneration = 1
	group.initialUncertain = false
	group.started = true
	group.committed = true
	close(group.startedReady)
	group.mu.Unlock()
	candidate.pathSession.adjustActiveFlows(1)
	group.runtime.recordGroupCarrier(flow.ID(), candidate.interfaceName, carrier)
	flow.Start()
	if !group.runtime.startGroupWorker(func() { group.monitorActiveCarrier(candidate, carrier) }) {
		_ = carrier.Close()
		group.removeActiveCarrier(carrier)
		return false
	}
	return true
}

func (group *tcpCarrierGroup) runActiveRecovery() {
	group.mu.Lock()
	flow := group.flow
	requiredInstance := group.serverInstanceID
	valid := (group.started || group.initialUncertain) && !group.closed && !group.failed && flow != nil && !group.flowDoneLocked() && group.activeInterface == ""
	group.mu.Unlock()
	if !valid {
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	candidate, ok := group.runtime.selectActiveCandidate(requiredInstance)
	if !ok {
		group.finishActiveWork(tcpActiveWorkRecovery, true)
		return
	}
	group.mu.Lock()
	if group.closed || group.failed || group.flowDoneLocked() || group.activeInterface != "" {
		group.mu.Unlock()
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	group.carrierGeneration++
	generation := group.carrierGeneration
	token := group.resumeToken
	group.mu.Unlock()
	timeout := time.Duration(group.runtime.client.cfg.Transfer.TCP.ResumeOpenTimeoutMillis) * time.Millisecond
	conn, maxPayload, err := candidate.session.Resume(flow.ID(), token, generation, timeout)
	if err != nil {
		var resumeErr *tcpstream.ResumeError
		if errors.As(err, &resumeErr) {
			if resumeErr.Result == tcpstream.ResumeResultBusy {
				group.finishActiveWork(tcpActiveWorkRecovery, true)
				return
			}
			group.mu.Lock()
			uncertainOpen := group.initialUncertain
			if uncertainOpen && resumeErr.Result == tcpstream.ResumeResultExpired {
				group.initialUncertain = false
				group.serverInstanceID = tcpstream.ServerInstanceID{}
				group.carrierGeneration = 0
			}
			group.mu.Unlock()
			if uncertainOpen && resumeErr.Result == tcpstream.ResumeResultExpired {
				group.finishActiveWork(tcpActiveWorkOpen, true)
				return
			}
			group.finishActiveWork(tcpActiveWorkNone, false)
			if uncertainOpen {
				group.failOpen(err)
			} else {
				flow.Reset(err)
			}
			return
		}
		group.finishActiveWork(tcpActiveWorkRecovery, false)
		return
	}
	if !group.attachActiveRecovery(candidate, conn, maxPayload, generation) {
		group.finishActiveWork(tcpActiveWorkRecovery, false)
		return
	}
	group.finishActiveWork(tcpActiveWorkNone, false)
}

func (group *tcpCarrierGroup) attachActiveRecovery(candidate tcpActiveCandidate, conn net.Conn, maxPayload uint32, generation uint64) bool {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.mu.Lock()
	flow := group.flow
	initial := group.initialUncertain
	beforeAttach := group.beforeAttach
	valid := !group.closed && !group.failed && (group.started || initial) && flow != nil && !group.flowDoneLocked() && group.activeInterface == "" && group.carrierGeneration == generation
	group.mu.Unlock()
	if !valid || !group.runtime.activeCandidateCurrent(candidate) {
		_ = conn.Close()
		return false
	}
	traffic := group.runtime.trafficForPath(candidate.interfaceName, candidate.path)
	carrier, err := flow.ReplaceObserved(conn, maxPayload, generation, tcpCarrierObserver(traffic))
	if err != nil {
		return false
	}
	if initial && beforeAttach != nil {
		if err := beforeAttach(); err != nil {
			_ = carrier.Close()
			group.failOpenLocked(err)
			return false
		}
	}
	group.mu.Lock()
	slot := group.slots[candidate.interfaceName]
	if group.closed || group.failed || slot == nil || slot.path != candidate.path || group.flowDoneLocked() || group.activeInterface != "" {
		group.mu.Unlock()
		_ = carrier.Close()
		return false
	}
	slot.session = candidate.session
	slot.sessionGeneration = candidate.sessionGeneration
	slot.carrier = carrier
	group.activeInterface = candidate.interfaceName
	group.activeCarrier = carrier
	group.activePathSession = candidate.pathSession
	if initial {
		group.initialUncertain = false
		group.started = true
		group.committed = true
		close(group.startedReady)
	} else {
		group.degradedSince = time.Time{}
		group.lastMigration = tcpRetryNow()
	}
	group.mu.Unlock()
	candidate.pathSession.adjustActiveFlows(1)
	group.runtime.recordGroupCarrier(flow.ID(), candidate.interfaceName, carrier)
	if initial {
		flow.Start()
	}
	if !group.runtime.startGroupWorker(func() { group.monitorActiveCarrier(candidate, carrier) }) {
		_ = carrier.Close()
		group.removeActiveCarrier(carrier)
		return false
	}
	return true
}

func (runtime *tcpClientRuntime) considerActiveMigrations() {
	if !runtime.client.cfg.Transfer.TCP.ActiveStandby() {
		return
	}
	runtime.mu.Lock()
	groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
	for group := range runtime.groups {
		groups = append(groups, group)
	}
	runtime.mu.Unlock()
	for _, group := range groups {
		group.considerActiveMigration()
	}
}

func (group *tcpCarrierGroup) considerActiveMigration() {
	now := tcpRetryNow()
	group.mu.Lock()
	activeInterface := group.activeInterface
	requiredInstance := group.serverInstanceID
	eligible := group.started && activeInterface != "" && !group.closed && !group.failed && !group.activeQueued && !group.activeInFlight && !group.flowDoneLocked() && (group.lastMigration.IsZero() || now.Sub(group.lastMigration) >= tcpPathMigrationCooloff)
	group.mu.Unlock()
	if !eligible {
		return
	}
	current, currentOK := group.runtime.activeCandidateForInterface(activeInterface, requiredInstance)
	best, bestOK := group.runtime.selectActiveCandidateExcept(requiredInstance, activeInterface)
	better := currentOK && bestOK && !best.degraded && best.score+float64(tcpPathSwitchMargin) < current.score
	group.mu.Lock()
	if group.activeInterface != activeInterface || group.closed || group.failed || group.activeInFlight || group.activeQueued {
		group.mu.Unlock()
		return
	}
	if !better {
		group.degradedSince = time.Time{}
		group.mu.Unlock()
		return
	}
	if group.degradedSince.IsZero() {
		group.degradedSince = now
		group.mu.Unlock()
		return
	}
	ready := now.Sub(group.degradedSince) >= tcpPathDegradeHold
	group.mu.Unlock()
	if ready {
		group.scheduleActive(tcpActiveWorkMigration)
	}
}

func (group *tcpCarrierGroup) runActiveMigration() {
	group.mu.Lock()
	flow := group.flow
	fromInterface := group.activeInterface
	requiredInstance := group.serverInstanceID
	valid := group.started && fromInterface != "" && !group.closed && !group.failed && flow != nil && !group.flowDoneLocked()
	group.mu.Unlock()
	if !valid {
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	current, currentOK := group.runtime.activeCandidateForInterface(fromInterface, requiredInstance)
	candidate, candidateOK := group.runtime.selectActiveCandidateExcept(requiredInstance, fromInterface)
	if !currentOK || !candidateOK || candidate.degraded || candidate.score+float64(tcpPathSwitchMargin) >= current.score {
		group.mu.Lock()
		group.degradedSince = time.Time{}
		group.mu.Unlock()
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	group.mu.Lock()
	if group.closed || group.failed || group.flowDoneLocked() || group.activeInterface != fromInterface {
		group.mu.Unlock()
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	group.carrierGeneration++
	generation := group.carrierGeneration
	token := group.resumeToken
	group.mu.Unlock()
	timeout := time.Duration(group.runtime.client.cfg.Transfer.TCP.ResumeOpenTimeoutMillis) * time.Millisecond
	conn, maxPayload, err := candidate.session.Resume(flow.ID(), token, generation, timeout)
	if err != nil {
		group.resetActiveMigrationHold()
		group.finishActiveWork(tcpActiveWorkNone, false)
		return
	}
	if !group.attachActiveMigration(current, candidate, conn, maxPayload, generation) {
		group.finishActiveWork(tcpActiveWorkRecovery, false)
		return
	}
	group.finishActiveWork(tcpActiveWorkNone, false)
}

func (group *tcpCarrierGroup) resetActiveMigrationHold() {
	group.mu.Lock()
	group.degradedSince = time.Time{}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) attachActiveMigration(current, candidate tcpActiveCandidate, conn net.Conn, maxPayload uint32, generation uint64) bool {
	group.startMu.Lock()
	defer group.startMu.Unlock()
	group.mu.Lock()
	flow := group.flow
	fromInterface := current.interfaceName
	activeInterface := group.activeInterface
	valid := !group.closed && !group.failed && flow != nil && !group.flowDoneLocked() && group.carrierGeneration == generation && (activeInterface == fromInterface || activeInterface == "")
	group.mu.Unlock()
	if !valid || !group.runtime.activeCandidateCurrent(candidate) {
		_ = conn.Close()
		return false
	}
	group.mu.Lock()
	activeInterface = group.activeInterface
	oldSlot := group.slots[fromInterface]
	oldCarrier := group.activeCarrier
	oldPathSession := group.activePathSession
	valid = !group.closed && !group.failed && group.flow == flow && !group.flowDoneLocked() && group.carrierGeneration == generation && (activeInterface == fromInterface || activeInterface == "")
	if valid && activeInterface == fromInterface && oldCarrier != nil {
		group.retiringCarrier = oldCarrier
	}
	group.mu.Unlock()
	if !valid {
		_ = conn.Close()
		return false
	}
	traffic := group.runtime.trafficForPath(candidate.interfaceName, candidate.path)
	carrier, err := flow.ReplaceObserved(conn, maxPayload, generation, tcpCarrierObserver(traffic))
	if err != nil {
		group.clearRetiringCarrier(oldCarrier)
		return false
	}
	group.mu.Lock()
	newSlot := group.slots[candidate.interfaceName]
	activeInterface = group.activeInterface
	if group.closed || group.failed || newSlot == nil || newSlot.path != candidate.path || group.flowDoneLocked() || (activeInterface != fromInterface && activeInterface != "") {
		group.mu.Unlock()
		_ = carrier.Close()
		return false
	}
	oldWasActive := activeInterface == fromInterface
	if oldWasActive && oldSlot != nil && oldSlot.carrier == oldCarrier {
		oldSlot.carrier = nil
	}
	newSlot.session = candidate.session
	newSlot.sessionGeneration = candidate.sessionGeneration
	newSlot.carrier = carrier
	group.activeInterface = candidate.interfaceName
	group.activeCarrier = carrier
	group.activePathSession = candidate.pathSession
	if group.retiringCarrier == oldCarrier {
		group.retiringCarrier = nil
	}
	group.degradedSince = time.Time{}
	group.lastMigration = tcpRetryNow()
	group.mu.Unlock()
	if oldWasActive && oldPathSession != nil {
		oldPathSession.adjustActiveFlows(-1)
	}
	group.runtime.removeGroupCarrier(flow.ID(), fromInterface, oldCarrier)
	candidate.pathSession.adjustActiveFlows(1)
	group.runtime.recordGroupCarrier(flow.ID(), candidate.interfaceName, carrier)
	if !group.runtime.startGroupWorker(func() { group.monitorActiveCarrier(candidate, carrier) }) {
		_ = carrier.Close()
		group.removeActiveCarrier(carrier)
		return false
	}
	return true
}

func (runtime *tcpClientRuntime) activeCandidateForInterface(interfaceName string, requiredInstance tcpstream.ServerInstanceID) (tcpActiveCandidate, bool) {
	runtime.mu.Lock()
	path, pathExists := runtime.paths[interfaceName]
	pathSession := runtime.sessions[interfaceName]
	runtime.mu.Unlock()
	if !pathExists || pathSession == nil {
		return tcpActiveCandidate{}, false
	}
	candidate, ok := pathSession.activeCandidate(path, requiredInstance, tcpRetryNow())
	if !ok || !runtime.activeRetentionCompatible(candidate.session) {
		return tcpActiveCandidate{}, false
	}
	candidate.interfaceName = interfaceName
	switch runtime.client.cfg.InterfaceHints[interfaceName].Cost {
	case config.InterfaceCostMetered:
		candidate.score += float64(500 * time.Millisecond)
	case config.InterfaceCostAvoid:
		candidate.score += float64(5 * time.Second)
	}
	return candidate, true
}

func (group *tcpCarrierGroup) monitorActiveCarrier(candidate tcpActiveCandidate, carrier *tcpstream.Carrier) {
	<-carrier.Done()
	<-carrier.Detached()
	removed, plannedRetirement := group.removeActiveCarrier(carrier)
	if removed {
		group.mu.Lock()
		flow := group.flow
		group.mu.Unlock()
		// Normal Flow completion closes its carrier before Flow.Done. The Flow
		// state already reflects that completion, so only penalize an actual loss.
		if flow == nil {
			return
		}
		state := flow.State()
		lost := state == tcpstream.FlowStateRecovering || (state == tcpstream.FlowStateFailed && errors.Is(flow.Err(), tcpstream.ErrNoCarriers))
		if !lost {
			return
		}
		if !plannedRetirement && !candidate.session.IsClosed() {
			candidate.pathSession.recordFailure(tcpRetryNow())
		}
		if state != tcpstream.FlowStateRecovering {
			return
		}
		group.runtime.enforceRecoveryLimits()
		group.scheduleActive(tcpActiveWorkRecovery)
	}
}

func (group *tcpCarrierGroup) clearRetiringCarrier(carrier *tcpstream.Carrier) {
	group.mu.Lock()
	if group.retiringCarrier == carrier {
		group.retiringCarrier = nil
	}
	group.mu.Unlock()
}

func (group *tcpCarrierGroup) removeActiveCarrier(carrier *tcpstream.Carrier) (bool, bool) {
	group.mu.Lock()
	removed := carrier != nil && group.activeCarrier == carrier
	plannedRetirement := carrier != nil && group.retiringCarrier == carrier
	if plannedRetirement {
		group.retiringCarrier = nil
	}
	interfaceName := group.activeInterface
	pathSession := group.activePathSession
	if removed {
		if slot := group.slots[interfaceName]; slot != nil && slot.carrier == carrier {
			slot.carrier = nil
		}
		group.activeInterface = ""
		group.activeCarrier = nil
		group.activePathSession = nil
	}
	flow := group.flow
	group.mu.Unlock()
	if !removed {
		return false, plannedRetirement
	}
	if pathSession != nil {
		pathSession.adjustActiveFlows(-1)
	}
	if flow != nil {
		group.runtime.removeGroupCarrier(flow.ID(), interfaceName, carrier)
	}
	return true, plannedRetirement
}

type tcpRecoveringGroup struct {
	flow         *tcpstream.Flow
	order        uint64
	historyBytes int64
}

func (runtime *tcpClientRuntime) recoveryCapacityAvailable() bool {
	tcpConfig := runtime.client.cfg.Transfer.TCP
	runtime.recoveryMu.Lock()
	defer runtime.recoveryMu.Unlock()
	recovering, bytes := runtime.recoveryUsage()
	return recovering < tcpConfig.MaxRecoveringStreams && bytes < tcpConfig.MaxRecoveryBytes
}

func (runtime *tcpClientRuntime) enforceRecoveryLimits() {
	tcpConfig := runtime.client.cfg.Transfer.TCP
	runtime.recoveryMu.Lock()
	defer runtime.recoveryMu.Unlock()
	entries, totalBytes := runtime.recoveringGroups()
	if len(entries) <= tcpConfig.MaxRecoveringStreams && totalBytes <= tcpConfig.MaxRecoveryBytes {
		return
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].order > entries[right].order })
	remaining := len(entries)
	for _, entry := range entries {
		if remaining <= tcpConfig.MaxRecoveringStreams && totalBytes <= tcpConfig.MaxRecoveryBytes {
			break
		}
		entry.flow.Reset(tcpstream.ErrNoCarriers)
		totalBytes -= entry.historyBytes
		remaining--
	}
}

func (runtime *tcpClientRuntime) recoveryUsage() (int, int64) {
	entries, bytes := runtime.recoveringGroups()
	return len(entries), bytes
}

func (runtime *tcpClientRuntime) recoveringGroups() ([]tcpRecoveringGroup, int64) {
	runtime.mu.Lock()
	groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
	for group := range runtime.groups {
		groups = append(groups, group)
	}
	runtime.mu.Unlock()
	entries := make([]tcpRecoveringGroup, 0, len(groups))
	var totalBytes int64
	for _, group := range groups {
		group.mu.Lock()
		flow := group.flow
		order := group.order
		group.mu.Unlock()
		if flow == nil || flow.State() != tcpstream.FlowStateRecovering {
			continue
		}
		historyBytes := int64(flow.HistoryBytes())
		entries = append(entries, tcpRecoveringGroup{flow: flow, order: order, historyBytes: historyBytes})
		totalBytes += historyBytes
	}
	return entries, totalBytes
}

func (runtime *tcpClientRuntime) selectActiveCandidate(requiredInstance tcpstream.ServerInstanceID) (tcpActiveCandidate, bool) {
	return runtime.selectActiveCandidateExcept(requiredInstance, "")
}

func (runtime *tcpClientRuntime) selectActiveCandidateExcept(requiredInstance tcpstream.ServerInstanceID, excludedInterface string) (tcpActiveCandidate, bool) {
	runtime.mu.Lock()
	paths := cloneTCPPaths(runtime.paths)
	sessions := make(map[string]*tcpPathSession, len(runtime.sessions))
	for interfaceName, pathSession := range runtime.sessions {
		sessions[interfaceName] = pathSession
	}
	runtime.mu.Unlock()
	now := tcpRetryNow()
	candidates := make([]tcpActiveCandidate, 0, len(sessions))
	hasHealthy := false
	for interfaceName, pathSession := range sessions {
		if interfaceName == excludedInterface {
			continue
		}
		path, ok := paths[interfaceName]
		if !ok {
			continue
		}
		candidate, ok := pathSession.activeCandidate(path, requiredInstance, now)
		if !ok || !runtime.activeRetentionCompatible(candidate.session) {
			continue
		}
		candidate.interfaceName = interfaceName
		switch runtime.client.cfg.InterfaceHints[interfaceName].Cost {
		case config.InterfaceCostMetered:
			candidate.score += float64(500 * time.Millisecond)
		case config.InterfaceCostAvoid:
			candidate.score += float64(5 * time.Second)
		}
		candidates = append(candidates, candidate)
		if !candidate.degraded {
			hasHealthy = true
		}
	}
	if len(candidates) == 0 {
		return tcpActiveCandidate{}, false
	}
	if hasHealthy {
		healthy := candidates[:0]
		for _, candidate := range candidates {
			if !candidate.degraded {
				healthy = append(healthy, candidate)
			}
		}
		candidates = healthy
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].score == candidates[right].score {
			return candidates[left].interfaceName < candidates[right].interfaceName
		}
		return candidates[left].score < candidates[right].score
	})
	return candidates[0], true
}

func (runtime *tcpClientRuntime) activeRetentionCompatible(session *tcpstream.Session) bool {
	required := time.Duration(runtime.client.cfg.Transfer.TCP.ClientRecoveryTimeoutMillis) * time.Millisecond
	return session != nil && session.ServerOrphanRetention() >= required
}

func (runtime *tcpClientRuntime) activeCandidateCurrent(candidate tcpActiveCandidate) bool {
	session, generation, healthy := runtime.currentPathSession(candidate.interfaceName, candidate.path)
	return healthy && session == candidate.session && generation == candidate.sessionGeneration
}

func (pathSession *tcpPathSession) activeCandidate(path tcpClientPath, requiredInstance tcpstream.ServerInstanceID, now time.Time) (tcpActiveCandidate, bool) {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	if pathSession.closed || pathSession.path != path || pathSession.session == nil || pathSession.session.IsClosed() || !pathSession.session.ActiveStandby() || now.Before(pathSession.cooldownUntil) {
		return tcpActiveCandidate{}, false
	}
	instanceID := pathSession.session.ServerInstanceID()
	if requiredInstance != (tcpstream.ServerInstanceID{}) && instanceID != requiredInstance {
		return tcpActiveCandidate{}, false
	}
	pathSession.decayPenaltyLocked(now)
	rtt := pathSession.rttEWMA
	if rtt <= 0 {
		rtt = float64(100 * time.Millisecond)
	}
	score := rtt + 2*pathSession.jitterEWMA + pathSession.penalty + float64(pathSession.activeFlows)*float64(tcpPathLoadPenalty)
	score += float64(pathSession.session.StreamCount()) * float64(time.Millisecond)
	degraded := pathSession.lastProbe.IsZero() || now.Sub(pathSession.lastProbe) > 2*tcpSessionProbeStandbyInterval+tcpSessionProbeTimeout
	return tcpActiveCandidate{
		path:              path,
		pathSession:       pathSession,
		session:           pathSession.session,
		sessionGeneration: pathSession.generation,
		serverInstanceID:  instanceID,
		score:             score,
		degraded:          degraded,
	}, true
}

func (pathSession *tcpPathSession) qualityStatus(path tcpClientPath, cost config.InterfaceCost, now time.Time) tcpPathQualityStatus {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	status := tcpPathQualityStatus{state: "unhealthy"}
	if pathSession.closed || pathSession.path != path || pathSession.session == nil || pathSession.session.IsClosed() {
		return status
	}
	status.active = true
	status.state = "healthy"
	if now.Before(pathSession.cooldownUntil) {
		status.state = "cooldown"
	} else if pathSession.session.ActiveStandby() && (pathSession.lastProbe.IsZero() || now.Sub(pathSession.lastProbe) > 2*tcpSessionProbeStandbyInterval+tcpSessionProbeTimeout) {
		status.state = "degraded"
	}
	pathSession.decayPenaltyLocked(now)
	rtt := pathSession.rttEWMA
	if rtt <= 0 {
		rtt = float64(100 * time.Millisecond)
	}
	score := rtt + 2*pathSession.jitterEWMA + pathSession.penalty + float64(pathSession.activeFlows)*float64(tcpPathLoadPenalty)
	score += float64(pathSession.session.StreamCount()) * float64(time.Millisecond)
	switch cost {
	case config.InterfaceCostMetered:
		score += float64(500 * time.Millisecond)
	case config.InterfaceCostAvoid:
		score += float64(5 * time.Second)
	}
	status.rttMillis = rtt / float64(time.Millisecond)
	status.jitterMillis = pathSession.jitterEWMA / float64(time.Millisecond)
	status.scoreMillis = score / float64(time.Millisecond)
	status.failurePenaltyMillis = pathSession.penalty / float64(time.Millisecond)
	status.activeFlows = pathSession.activeFlows
	if pathSession.session.ActiveStandby() {
		instanceID := pathSession.session.ServerInstanceID()
		status.serverInstanceID = fmt.Sprintf("%x", instanceID[:])
	}
	return status
}

func (pathSession *tcpPathSession) recordRTT(sample time.Duration, now time.Time) {
	if sample <= 0 {
		return
	}
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	pathSession.recordRTTLocked(sample, now)
}

func (pathSession *tcpPathSession) recordRTTLocked(sample time.Duration, now time.Time) {
	value := float64(sample)
	if pathSession.rttEWMA == 0 {
		pathSession.rttEWMA = value
		pathSession.jitterEWMA = 0
	} else {
		deviation := math.Abs(value - pathSession.rttEWMA)
		pathSession.rttEWMA = 0.75*pathSession.rttEWMA + 0.25*value
		pathSession.jitterEWMA = 0.75*pathSession.jitterEWMA + 0.25*deviation
	}
	pathSession.lastProbe = now
	pathSession.decayPenaltyLocked(now)
}

func (pathSession *tcpPathSession) recordFailure(now time.Time) {
	pathSession.mu.Lock()
	defer pathSession.mu.Unlock()
	pathSession.decayPenaltyLocked(now)
	pathSession.penalty += float64(tcpPathFailurePenalty)
	pathSession.penaltyAt = now
	pathSession.cooldownUntil = now.Add(tcpActiveRetryInterval)
}

func (pathSession *tcpPathSession) decayPenaltyLocked(now time.Time) {
	if pathSession.penalty == 0 {
		pathSession.penaltyAt = now
		return
	}
	if pathSession.penaltyAt.IsZero() || !now.After(pathSession.penaltyAt) {
		return
	}
	elapsed := now.Sub(pathSession.penaltyAt)
	pathSession.penalty *= math.Exp2(-float64(elapsed) / float64(tcpPathPenaltyHalfLife))
	pathSession.penaltyAt = now
}

func (pathSession *tcpPathSession) adjustActiveFlows(delta int) {
	pathSession.mu.Lock()
	pathSession.activeFlows += delta
	if pathSession.activeFlows < 0 {
		pathSession.activeFlows = 0
	}
	pathSession.mu.Unlock()
}
