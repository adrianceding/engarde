package clientrole

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

func TestAdaptivePathSelectionUsesMeasuredQualityAndStableTies(t *testing.T) {
	instanceID := tcpstream.ServerInstanceID{1}
	fastSession := newActiveSelectionSession(t, instanceID)
	slowSession := newActiveSelectionSession(t, instanceID)
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	client := New(config.Client{Transfer: transfer}, "test", nil)
	runtime := &tcpClientRuntime{
		client: client,
		paths: map[string]tcpClientPath{
			"path-a-slow": {index: 1, address: "192.0.2.1", destination: "server:1"},
			"path-z-fast": {index: 2, address: "192.0.2.2", destination: "server:1"},
		},
		sessions: make(map[string]*tcpPathSession),
	}
	runtime.sessions["path-a-slow"] = &tcpPathSession{
		path:       runtime.paths["path-a-slow"],
		session:    slowSession,
		generation: 1,
		rttEWMA:    float64(200 * time.Millisecond),
	}
	runtime.sessions["path-z-fast"] = &tcpPathSession{
		path:       runtime.paths["path-z-fast"],
		session:    fastSession,
		generation: 1,
		rttEWMA:    float64(40 * time.Millisecond),
	}

	candidate, ok := runtime.selectActiveCandidate(tcpstream.ServerInstanceID{})
	if !ok || candidate.interfaceName != "path-z-fast" {
		t.Fatalf("adaptive candidate = %q/%v, want path-z-fast", candidate.interfaceName, ok)
	}

	client.cfg.InterfaceHints = map[string]config.InterfaceHint{"path-z-fast": {Cost: config.InterfaceCostMetered}}
	candidate, ok = runtime.selectActiveCandidate(tcpstream.ServerInstanceID{})
	if !ok || candidate.interfaceName != "path-a-slow" {
		t.Fatalf("metered candidate = %q/%v, want path-a-slow", candidate.interfaceName, ok)
	}
	client.cfg.InterfaceHints = nil

	fastPath := runtime.sessions["path-z-fast"]
	fastPath.mu.Lock()
	fastPath.activeFlows = 40
	fastPath.mu.Unlock()
	candidate, ok = runtime.selectActiveCandidate(tcpstream.ServerInstanceID{})
	if !ok || candidate.interfaceName != "path-a-slow" {
		t.Fatalf("loaded candidate = %q/%v, want path-a-slow", candidate.interfaceName, ok)
	}

	fastPath.mu.Lock()
	fastPath.activeFlows = 0
	fastPath.rttEWMA = float64(200 * time.Millisecond)
	fastPath.mu.Unlock()
	candidate, ok = runtime.selectActiveCandidate(tcpstream.ServerInstanceID{})
	if !ok || candidate.interfaceName != "path-a-slow" {
		t.Fatalf("tie candidate = %q/%v, want lexical path-a-slow", candidate.interfaceName, ok)
	}
}

func TestAdaptivePathSelectionFiltersServerStateDomain(t *testing.T) {
	firstID := tcpstream.ServerInstanceID{1}
	secondID := tcpstream.ServerInstanceID{2}
	firstSession := newActiveSelectionSession(t, firstID)
	secondSession := newActiveSelectionSession(t, secondID)
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	runtime := &tcpClientRuntime{
		client: New(config.Client{Transfer: transfer}, "test", nil),
		paths: map[string]tcpClientPath{
			"first":  {index: 1},
			"second": {index: 2},
		},
		sessions: map[string]*tcpPathSession{
			"first":  {path: tcpClientPath{index: 1}, session: firstSession, generation: 1, rttEWMA: float64(100 * time.Millisecond)},
			"second": {path: tcpClientPath{index: 2}, session: secondSession, generation: 1, rttEWMA: float64(10 * time.Millisecond)},
		},
	}
	candidate, ok := runtime.selectActiveCandidate(firstID)
	if !ok || candidate.interfaceName != "first" || candidate.serverInstanceID != firstID {
		t.Fatalf("state-domain candidate = %q/%x/%v", candidate.interfaceName, candidate.serverInstanceID, ok)
	}
}

func TestAdaptivePathSelectionRejectsShortServerRetention(t *testing.T) {
	instanceID := tcpstream.ServerInstanceID{1}
	session := newActiveSelectionSessionWithRetention(t, instanceID, 2*time.Second)
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	path := tcpClientPath{index: 1}
	runtime := &tcpClientRuntime{
		client: New(config.Client{Transfer: transfer}, "test", nil),
		paths:  map[string]tcpClientPath{"short": path},
		sessions: map[string]*tcpPathSession{
			"short": {path: path, session: session, generation: 1, rttEWMA: float64(10 * time.Millisecond)},
		},
	}
	if candidate, ok := runtime.selectActiveCandidate(instanceID); ok {
		t.Fatalf("short-retention candidate = %#v, want none", candidate)
	}
}

func TestRemoveActiveCarrierAfterPathSlotWasDeleted(t *testing.T) {
	pathSession := &tcpPathSession{activeFlows: 1}
	runtime := &tcpClientRuntime{}
	group := newTCPCarrierGroup(runtime)
	carrier := new(tcpstream.Carrier)
	group.activeInterface = "path-a"
	group.activeCarrier = carrier
	group.activePathSession = pathSession

	removed, _ := group.removeActiveCarrier(carrier)
	if !removed {
		t.Fatal("carrier was not removed after its path slot disappeared")
	}
	if group.activeInterface != "" {
		t.Fatalf("active interface = %q, want empty", group.activeInterface)
	}
	pathSession.mu.Lock()
	activeFlows := pathSession.activeFlows
	pathSession.mu.Unlock()
	if activeFlows != 0 {
		t.Fatalf("active flows = %d, want 0", activeFlows)
	}
}

func TestMonitorActiveCarrierAfterSameNamePathReplacement(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	carrierConn, carrierPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{5}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		RecoveryTimeout: time.Minute,
		SingleCarrier:   true,
	})
	carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	oldPath := tcpClientPath{index: 1, address: "192.0.2.1", destination: "server:1"}
	replacementPath := tcpClientPath{index: 2, address: "192.0.2.2", destination: "server:1"}
	pathSession := &tcpPathSession{activeFlows: 1}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	group.activeCarrier = carrier
	group.activePathSession = pathSession
	group.slots["path-a"] = &tcpFlowSlot{path: oldPath, carrier: carrier}
	runtime.carriers[flow.ID()] = map[string]*tcpstream.Carrier{"path-a": carrier}
	candidate := tcpActiveCandidate{
		interfaceName: "path-a",
		path:          oldPath,
		pathSession:   pathSession,
		session:       newActiveSelectionSession(t, tcpstream.ServerInstanceID{1}),
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = carrierPeer.Close()
	})

	group.syncPaths(map[string]tcpClientPath{"path-a": replacementPath})
	group.monitorActiveCarrier(candidate, carrier)

	group.mu.Lock()
	activeInterface := group.activeInterface
	queued := group.activeQueued
	work := group.activeWork
	slot := group.slots["path-a"]
	group.mu.Unlock()
	pathSession.mu.Lock()
	activeFlows := pathSession.activeFlows
	pathSession.mu.Unlock()
	runtime.mu.Lock()
	registered := runtime.carriers[flow.ID()]["path-a"]
	runtime.mu.Unlock()
	if activeInterface != "" || activeFlows != 0 || registered != nil {
		t.Fatalf("stale ownership = interface %q, active flows %d, registered carrier %p", activeInterface, activeFlows, registered)
	}
	if slot == nil || slot.path != replacementPath || slot.carrier != nil {
		t.Fatalf("replacement slot = %#v, want replacement path without a carrier", slot)
	}
	if !queued || work != tcpActiveWorkRecovery {
		t.Fatalf("replacement work = queued %v kind %d, want recovery", queued, work)
	}
	select {
	case token := <-runtime.active.recoveries:
		if token.group != group {
			t.Fatal("recovery queue contains a different group")
		}
	default:
		t.Fatal("replacement did not enqueue recovery")
	}
}

func TestMonitorActiveCarrierDoesNotPenalizePlannedRetirement(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	oldConn, oldPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{11}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		RecoveryTimeout: time.Minute,
		SingleCarrier:   true,
	})
	oldCarrier, err := flow.Attach(oldConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	path := tcpClientPath{index: 1}
	pathSession := &tcpPathSession{activeFlows: 1}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	group.activeCarrier = oldCarrier
	group.activePathSession = pathSession
	group.retiringCarrier = oldCarrier
	group.slots["path-a"] = &tcpFlowSlot{path: path, carrier: oldCarrier}
	runtime.carriers[flow.ID()] = map[string]*tcpstream.Carrier{"path-a": oldCarrier}
	candidate := tcpActiveCandidate{
		interfaceName: "path-a",
		path:          path,
		pathSession:   pathSession,
		session:       newActiveSelectionSession(t, tcpstream.ServerInstanceID{1}),
	}
	newConn, newPeer := net.Pipe()
	newCarrier, err := flow.ReplaceObserved(newConn, tcpstream.MaxPayloadSize, 2, tcpstream.CarrierObserver{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = oldPeer.Close()
		_ = newPeer.Close()
	})
	if err := newCarrier.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-newCarrier.Detached():
	case <-time.After(time.Second):
		t.Fatal("replacement carrier did not detach")
	}
	if state := flow.State(); state != tcpstream.FlowStateRecovering {
		t.Fatalf("flow state = %q, want recovering", state)
	}

	group.monitorActiveCarrier(candidate, oldCarrier)

	pathSession.mu.Lock()
	penalty := pathSession.penalty
	cooldownUntil := pathSession.cooldownUntil
	activeFlows := pathSession.activeFlows
	pathSession.mu.Unlock()
	if penalty != 0 || !cooldownUntil.IsZero() {
		t.Fatalf("retired path penalty = %v, cooldown = %v, want zero values", time.Duration(penalty), cooldownUntil)
	}
	if activeFlows != 0 {
		t.Fatalf("retired path active flows = %d, want 0", activeFlows)
	}
	select {
	case token := <-runtime.active.recoveries:
		if token.group != group {
			t.Fatal("recovery queue contains a different group")
		}
	default:
		t.Fatal("recovering Flow was not queued after planned retirement")
	}
}

func TestMonitorActiveCarrierDoesNotPenalizeCompletedFlow(t *testing.T) {
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	runtime := &tcpClientRuntime{
		client:   New(config.Client{Transfer: transfer}, "test", nil),
		carriers: make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
	}
	application, endpoint := net.Pipe()
	carrierConn, carrierPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{3}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		RecoveryTimeout: time.Minute,
		SingleCarrier:   true,
	})
	carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	path := tcpClientPath{index: 1}
	pathSession := &tcpPathSession{activeFlows: 1}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	group.activeCarrier = carrier
	group.activePathSession = pathSession
	group.slots["path-a"] = &tcpFlowSlot{path: path, carrier: carrier}
	runtime.carriers[flow.ID()] = map[string]*tcpstream.Carrier{"path-a": carrier}
	candidate := tcpActiveCandidate{
		interfaceName: "path-a",
		path:          path,
		pathSession:   pathSession,
		session:       newActiveSelectionSession(t, tcpstream.ServerInstanceID{1}),
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = carrierPeer.Close()
	})

	if err := flow.Close(); err != nil {
		t.Fatal(err)
	}
	if flow.State() != tcpstream.FlowStateCompleted {
		t.Fatalf("flow state = %q, want completed", flow.State())
	}
	group.monitorActiveCarrier(candidate, carrier)

	pathSession.mu.Lock()
	penalty := pathSession.penalty
	cooldownUntil := pathSession.cooldownUntil
	activeFlows := pathSession.activeFlows
	pathSession.mu.Unlock()
	if penalty != 0 || !cooldownUntil.IsZero() {
		t.Fatalf("completed Flow path penalty = %v, cooldown = %v, want zero values", time.Duration(penalty), cooldownUntil)
	}
	if activeFlows != 0 || group.activeInterface != "" {
		t.Fatalf("completed Flow ownership = active flows %d/interface %q, want zero values", activeFlows, group.activeInterface)
	}
}

func TestMonitorActiveCarrierPenalizesRecoveringFlow(t *testing.T) {
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	runtime := &tcpClientRuntime{
		client:   New(config.Client{Transfer: transfer}, "test", nil),
		carriers: make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
	}
	application, endpoint := net.Pipe()
	carrierConn, carrierPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{4}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		RecoveryTimeout: time.Minute,
		SingleCarrier:   true,
	})
	carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	path := tcpClientPath{index: 1}
	pathSession := &tcpPathSession{activeFlows: 1}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	group.activeCarrier = carrier
	group.activePathSession = pathSession
	group.slots["path-a"] = &tcpFlowSlot{path: path, carrier: carrier}
	runtime.carriers[flow.ID()] = map[string]*tcpstream.Carrier{"path-a": carrier}
	candidate := tcpActiveCandidate{
		interfaceName: "path-a",
		path:          path,
		pathSession:   pathSession,
		session:       newActiveSelectionSession(t, tcpstream.ServerInstanceID{1}),
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = carrierPeer.Close()
	})

	flow.Start()
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	if flow.State() != tcpstream.FlowStateRecovering {
		t.Fatalf("flow state = %q, want recovering", flow.State())
	}
	group.monitorActiveCarrier(candidate, carrier)

	pathSession.mu.Lock()
	penalty := pathSession.penalty
	cooldownUntil := pathSession.cooldownUntil
	pathSession.mu.Unlock()
	if penalty != float64(tcpPathFailurePenalty) || cooldownUntil.IsZero() {
		t.Fatalf("recovering Flow path penalty = %v, cooldown = %v, want %v and a deadline", time.Duration(penalty), cooldownUntil, tcpPathFailurePenalty)
	}
}

func TestMonitorActiveCarrierPenalizesFailedNoCarriersFlow(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	carrierConn, carrierPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{6}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		RecoveryTimeout: time.Minute,
		SingleCarrier:   true,
	})
	carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	path := tcpClientPath{index: 1}
	pathSession := &tcpPathSession{activeFlows: 1}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	group.activeCarrier = carrier
	group.activePathSession = pathSession
	group.slots["path-a"] = &tcpFlowSlot{path: path, carrier: carrier}
	runtime.carriers[flow.ID()] = map[string]*tcpstream.Carrier{"path-a": carrier}
	candidate := tcpActiveCandidate{
		interfaceName: "path-a",
		path:          path,
		pathSession:   pathSession,
		session:       newActiveSelectionSession(t, tcpstream.ServerInstanceID{1}),
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = carrierPeer.Close()
	})

	flow.Start()
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	flow.Reset(tcpstream.ErrNoCarriers)
	group.monitorActiveCarrier(candidate, carrier)

	pathSession.mu.Lock()
	penalty := pathSession.penalty
	cooldownUntil := pathSession.cooldownUntil
	pathSession.mu.Unlock()
	if penalty != float64(tcpPathFailurePenalty) || cooldownUntil.IsZero() {
		t.Fatalf("failed Flow path penalty = %v, cooldown = %v, want %v and a deadline", time.Duration(penalty), cooldownUntil, tcpPathFailurePenalty)
	}
	select {
	case <-runtime.active.recoveries:
		t.Fatal("terminal Flow enqueued recovery")
	default:
	}
}

func TestInitialUncertainFlowReconcilesWithRecovery(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{7}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{})
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.initialUncertain = true
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
	})

	group.reconcileAll()

	group.mu.Lock()
	queued := group.activeQueued
	work := group.activeWork
	group.mu.Unlock()
	if !queued || work != tcpActiveWorkRecovery {
		t.Fatalf("uncertain Flow work = queued %v kind %d, want recovery", queued, work)
	}
	select {
	case token := <-runtime.active.recoveries:
		if token.group != group {
			t.Fatal("recovery queue contains a different group")
		}
	default:
		t.Fatal("uncertain Flow was not queued for recovery")
	}
	select {
	case <-runtime.active.opens:
		t.Fatal("uncertain Flow was queued for a duplicate open")
	default:
	}
}

func TestQueuedMigrationPromotesToRecoveryPriority(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{8}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{})
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
	})

	group.scheduleActive(tcpActiveWorkMigration)
	var migrationToken tcpActiveQueueToken
	select {
	case migrationToken = <-runtime.active.migrations:
	default:
		t.Fatal("migration has no queue token")
	}
	group.scheduleActive(tcpActiveWorkRecovery)

	group.mu.Lock()
	queued := group.activeQueued
	work := group.activeWork
	group.mu.Unlock()
	if !queued || work != tcpActiveWorkRecovery {
		t.Fatalf("promoted work = queued %v kind %d, want recovery", queued, work)
	}
	select {
	case recoveryToken := <-runtime.active.recoveries:
		if recoveryToken.group != group {
			t.Fatal("recovery queue contains a different group")
		}
		migrationToken.group.runActiveWork(migrationToken.generation)
		group.mu.Lock()
		stillQueued := group.activeQueued
		stillWork := group.activeWork
		group.mu.Unlock()
		if !stillQueued || stillWork != tcpActiveWorkRecovery {
			t.Fatalf("stale migration token consumed promoted work: queued %v kind %d", stillQueued, stillWork)
		}
	default:
		t.Fatal("promoted recovery has no high-priority token")
	}
}

func TestQueuedMigrationKeepsRecoveryWhenPriorityQueueIsFull(t *testing.T) {
	runtime := newActiveCoordinatorTestRuntime()
	application, endpoint := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{9}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{})
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
	})

	group.scheduleActive(tcpActiveWorkMigration)
	runtime.active.recoveries <- tcpActiveQueueToken{group: newTCPCarrierGroup(runtime)}
	group.scheduleActive(tcpActiveWorkRecovery)

	group.mu.Lock()
	queued := group.activeQueued
	work := group.activeWork
	group.mu.Unlock()
	if !queued || work != tcpActiveWorkRecovery {
		t.Fatalf("fallback work = queued %v kind %d, want recovery", queued, work)
	}
	select {
	case <-flow.Done():
		t.Fatalf("priority promotion reset live Flow: %v", flow.Err())
	default:
	}
	select {
	case token := <-runtime.active.migrations:
		if token.group != group {
			t.Fatal("fallback queue contains a different group")
		}
	default:
		t.Fatal("promoted work lost its original queue token")
	}
}

type activeCloseBarrierConn struct {
	net.Conn
	started chan struct{}
	release <-chan struct{}
}

func (conn *activeCloseBarrierConn) Close() error {
	close(conn.started)
	<-conn.release
	return conn.Conn.Close()
}

func TestFlowDoneLockedRecognizesTerminalStateBeforeDoneCloses(t *testing.T) {
	application, endpoint := net.Pipe()
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClose) }) }
	flow := tcpstream.NewFlow(tcpstream.StreamID{10}, &activeCloseBarrierConn{
		Conn:    endpoint,
		started: closeStarted,
		release: releaseClose,
	}, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{})
	group := newTCPCarrierGroup(&tcpClientRuntime{})
	group.flow = flow
	t.Cleanup(func() {
		release()
		_ = flow.Close()
		_ = application.Close()
	})

	go flow.Reset(nil)
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("endpoint close did not reach barrier")
	}
	select {
	case <-flow.Done():
		t.Fatal("Flow.Done closed before endpoint Close returned")
	default:
	}
	group.mu.Lock()
	done := group.flowDoneLocked()
	group.mu.Unlock()
	if !done {
		t.Fatalf("terminal Flow state %q was treated as live", flow.State())
	}
	release()
	select {
	case <-flow.Done():
	case <-time.After(time.Second):
		t.Fatal("Flow.Done did not close after endpoint Close returned")
	}
}

func TestActiveMigrationResumeErrorsResetHoldWithoutRetry(t *testing.T) {
	tests := []struct {
		name   string
		result tcpstream.ResumeResult
	}{
		{name: "busy", result: tcpstream.ResumeResultBusy},
		{name: "rejected", result: tcpstream.ResumeResultRejected},
		{name: "expired", result: tcpstream.ResumeResultExpired},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instanceID := tcpstream.ServerInstanceID{byte(index + 1)}
			currentSession := newActiveSelectionSession(t, instanceID)
			targetSession, serverDone := newActiveResumeResultSession(t, instanceID, test.result)
			runtime := newActiveCoordinatorTestRuntime()
			currentPath := tcpClientPath{index: 1, address: "192.0.2.1", destination: "server:1"}
			targetPath := tcpClientPath{index: 2, address: "192.0.2.2", destination: "server:1"}
			now := time.Now()
			currentPathSession := &tcpPathSession{
				path:        currentPath,
				session:     currentSession,
				generation:  1,
				rttEWMA:     float64(250 * time.Millisecond),
				lastProbe:   now,
				activeFlows: 1,
			}
			targetPathSession := &tcpPathSession{
				path:       targetPath,
				session:    targetSession,
				generation: 1,
				rttEWMA:    float64(10 * time.Millisecond),
				lastProbe:  now,
			}
			runtime.paths = map[string]tcpClientPath{"current": currentPath, "target": targetPath}
			runtime.sessions = map[string]*tcpPathSession{"current": currentPathSession, "target": targetPathSession}

			application, endpoint := net.Pipe()
			carrierConn, carrierPeer := net.Pipe()
			flow := tcpstream.NewFlow(tcpstream.StreamID{byte(index + 30)}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
				RecoveryTimeout: time.Minute,
				SingleCarrier:   true,
			})
			carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
			if err != nil {
				t.Fatal(err)
			}
			flow.Start()
			group := newTCPCarrierGroup(runtime)
			group.flow = flow
			group.started = true
			group.activeInterface = "current"
			group.activeCarrier = carrier
			group.activePathSession = currentPathSession
			group.slots["current"] = &tcpFlowSlot{path: currentPath, carrier: carrier}
			group.slots["target"] = &tcpFlowSlot{path: targetPath}
			group.serverInstanceID = instanceID
			group.resumeToken = tcpstream.ResumeToken{1}
			group.carrierGeneration = 1
			group.degradedSince = now.Add(-tcpPathDegradeHold)
			group.activeInFlight = true
			group.activeExecuting = tcpActiveWorkMigration
			t.Cleanup(func() {
				_ = flow.Close()
				_ = application.Close()
				_ = carrierPeer.Close()
			})

			group.runActiveMigration()

			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not answer migration Resume")
			}
			group.mu.Lock()
			degradedSince := group.degradedSince
			activeInterface := group.activeInterface
			inFlight := group.activeInFlight
			queued := group.activeQueued
			group.mu.Unlock()
			if !degradedSince.IsZero() || activeInterface != "current" || inFlight || queued {
				t.Fatalf("migration state = hold %v interface %q in-flight %v queued %v", degradedSince, activeInterface, inFlight, queued)
			}
			runtime.active.retryMu.Lock()
			retries := len(runtime.active.retries)
			runtime.active.retryMu.Unlock()
			if retries != 0 {
				t.Fatalf("migration Resume %s scheduled %d immediate retries", test.name, retries)
			}
			if state := flow.State(); state != tcpstream.FlowStateActive {
				t.Fatalf("migration Resume %s changed Flow state to %q", test.name, state)
			}
		})
	}
}

func TestFailedMigrationWithoutActiveCarrierQueuesRecovery(t *testing.T) {
	application, endpoint := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{1}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{})
	runtime := &tcpClientRuntime{}
	coordinator := &tcpActiveCoordinator{
		runtime:    runtime,
		recoveries: make(chan tcpActiveQueueToken, 1),
		opens:      make(chan tcpActiveQueueToken, 1),
		migrations: make(chan tcpActiveQueueToken, 1),
		retries:    make(map[*tcpCarrierGroup]tcpActiveWorkKind),
	}
	runtime.active = coordinator
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInFlight = true
	group.activeExecuting = tcpActiveWorkMigration
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
	})

	group.finishActiveWork(tcpActiveWorkNone, false)
	group.mu.Lock()
	queued := group.activeQueued
	work := group.activeWork
	group.mu.Unlock()
	if !queued || work != tcpActiveWorkRecovery {
		t.Fatalf("post-migration work = queued %v kind %d, want recovery", queued, work)
	}
	select {
	case token := <-coordinator.recoveries:
		if token.group != group {
			t.Fatal("recovery queue contains a different group")
		}
	default:
		t.Fatal("recovery was not enqueued")
	}
}

func TestFullMigrationQueuePreservesActiveFlow(t *testing.T) {
	application, endpoint := net.Pipe()
	carrierConn, carrierPeer := net.Pipe()
	flow := tcpstream.NewFlow(tcpstream.StreamID{2}, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		ChunkSize:          16,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: 1024,
		RecoveryTimeout:    time.Minute,
		SingleCarrier:      true,
	})
	if _, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
	flow.Start()
	runtime := &tcpClientRuntime{}
	coordinator := &tcpActiveCoordinator{
		runtime:    runtime,
		recoveries: make(chan tcpActiveQueueToken, 1),
		opens:      make(chan tcpActiveQueueToken, 1),
		migrations: make(chan tcpActiveQueueToken, 1),
		retries:    make(map[*tcpCarrierGroup]tcpActiveWorkKind),
	}
	runtime.active = coordinator
	coordinator.migrations <- tcpActiveQueueToken{group: newTCPCarrierGroup(runtime)}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.started = true
	group.activeInterface = "path-a"
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = carrierPeer.Close()
	})

	group.scheduleActive(tcpActiveWorkMigration)
	select {
	case <-flow.Done():
		t.Fatalf("full migration queue terminated active flow: %v", flow.Err())
	default:
	}
	group.mu.Lock()
	queued := group.activeQueued
	work := group.activeWork
	group.mu.Unlock()
	if queued || work != tcpActiveWorkNone {
		t.Fatalf("migration state = queued %v kind %d, want idle", queued, work)
	}
}

func TestClientRecoveryLimitFailsNewestFlowFirst(t *testing.T) {
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	transfer.TCP.MaxRecoveringStreams = 1
	runtime := &tcpClientRuntime{
		client: New(config.Client{Transfer: transfer}, "test", nil),
		groups: make(map[*tcpCarrierGroup]struct{}),
	}
	oldFlow := newClientRecoveringFlow(t, tcpstream.StreamID{1})
	newFlow := newClientRecoveringFlow(t, tcpstream.StreamID{2})
	oldGroup := newTCPCarrierGroup(runtime)
	oldGroup.flow = oldFlow
	oldGroup.order = 1
	newGroup := newTCPCarrierGroup(runtime)
	newGroup.flow = newFlow
	newGroup.order = 2
	runtime.groups[oldGroup] = struct{}{}
	runtime.groups[newGroup] = struct{}{}

	runtime.enforceRecoveryLimits()
	select {
	case <-newFlow.Done():
	case <-time.After(time.Second):
		t.Fatal("newest recovering flow was not failed at the limit")
	}
	if !errors.Is(newFlow.Err(), tcpstream.ErrNoCarriers) {
		t.Fatalf("newest flow error = %v, want ErrNoCarriers", newFlow.Err())
	}
	select {
	case <-oldFlow.Done():
		t.Fatal("oldest recovering flow was failed before the newest flow")
	default:
	}
}

func newClientRecoveringFlow(t *testing.T, streamID tcpstream.StreamID) *tcpstream.Flow {
	t.Helper()
	application, endpoint := net.Pipe()
	carrierConn, peer := net.Pipe()
	flow := tcpstream.NewFlow(streamID, endpoint, tcpstream.DirectionClientToServer, tcpstream.FlowConfig{
		ChunkSize:          16,
		CarrierQueueBytes:  1024,
		ReorderWindowBytes: 1024,
		RecoveryTimeout:    time.Minute,
		SingleCarrier:      true,
	})
	carrier, err := flow.Attach(carrierConn, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	flow.Start()
	if err := carrier.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-carrier.Detached():
	case <-time.After(time.Second):
		t.Fatal("carrier did not detach")
	}
	if flow.State() != tcpstream.FlowStateRecovering {
		t.Fatalf("flow state = %q, want recovering", flow.State())
	}
	t.Cleanup(func() {
		_ = flow.Close()
		_ = application.Close()
		_ = peer.Close()
	})
	return flow
}

func newActiveCoordinatorTestRuntime() *tcpClientRuntime {
	transfer := config.Transfer{TCP: config.TCPTransfer{CarrierMode: config.TCPCarrierModeActiveStandby}}
	transfer.ApplyDefaults()
	runtime := &tcpClientRuntime{
		client:   New(config.Client{Transfer: transfer}, "test", nil),
		carriers: make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
	}
	runtime.active = &tcpActiveCoordinator{
		runtime:    runtime,
		recoveries: make(chan tcpActiveQueueToken, 1),
		opens:      make(chan tcpActiveQueueToken, 1),
		migrations: make(chan tcpActiveQueueToken, 1),
		retries:    make(map[*tcpCarrierGroup]tcpActiveWorkKind),
	}
	return runtime
}

func newActiveSelectionSession(t *testing.T, instanceID tcpstream.ServerInstanceID) *tcpstream.Session {
	return newActiveSelectionSessionWithRetention(t, instanceID, 9*time.Second)
}

func newActiveSelectionSessionWithRetention(t *testing.T, instanceID tcpstream.ServerInstanceID, retention time.Duration) *tcpstream.Session {
	t.Helper()
	clientSession, serverSession := newActiveSessionPair(t, instanceID, retention)
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
	})
	return clientSession
}

func newActiveResumeResultSession(t *testing.T, instanceID tcpstream.ServerInstanceID, resumeResult tcpstream.ResumeResult) (*tcpstream.Session, <-chan error) {
	t.Helper()
	clientSession, serverSession := newActiveSessionPair(t, instanceID, 9*time.Second)
	serverDone := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err != nil {
			serverDone <- fmt.Errorf("accept Resume stream: %w", err)
			return
		}
		defer stream.Close()
		frame, err := tcpstream.ReadFrame(stream, maxPayload)
		if err != nil {
			serverDone <- fmt.Errorf("read Resume frame: %w", err)
			return
		}
		if frame.Type != tcpstream.FrameResume {
			serverDone <- errors.New("migration request was not Resume")
			return
		}
		serverDone <- tcpstream.WriteFrame(stream, tcpstream.NewResumeResultFrame(frame.StreamID, frame.Offset, resumeResult))
	}()
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
	})
	return clientSession, serverDone
}

func newActiveSessionPair(t *testing.T, instanceID tcpstream.ServerInstanceID, retention time.Duration) (*tcpstream.Session, *tcpstream.Session) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	type result struct {
		session *tcpstream.Session
		err     error
	}
	accepted := make(chan result, 1)
	serverConfig := tcpstream.SessionConfig{ActiveStandby: true, ServerInstanceID: instanceID, OrphanRetention: retention}
	go func() {
		session, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, serverConfig, nil)
		accepted <- result{session: session, err: err}
	}()
	clientSession, err := tcpstream.DialSession(clientConn, tcpstream.MaxPayloadSize, time.Second, tcpstream.SessionConfig{ActiveStandby: true})
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	if server.err != nil {
		_ = clientSession.Close()
		t.Fatal(server.err)
	}
	return clientSession, server.session
}
