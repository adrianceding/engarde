package clientrole

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

const tcpSessionManagerTestTimeout = 3 * time.Second

type tcpSessionManagerPeer struct {
	interfaceName string
	session       *tcpstream.Session
	err           error
}

func TestTCPPathSessionIsSharedAndSurvivesFlowClose(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, dialCount := installTCPSessionManagerDialer(t, runtime)
	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	peer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if peer.err != nil {
		t.Fatal(peer.err)
	}

	firstFlow, firstEndpoint := newTCPSessionManagerFlow(t, runtime, 1)
	firstGroup := assignTCPSessionManagerGroup(t, runtime, firstFlow)
	secondFlow, secondEndpoint := newTCPSessionManagerFlow(t, runtime, 2)
	secondGroup := assignTCPSessionManagerGroup(t, runtime, secondFlow)
	waitTCPSessionManagerCondition(t, func() bool {
		return firstFlow.CarrierCount() == 1 && secondFlow.CarrierCount() == 1 && peer.session.StreamCount() == 2
	})

	pathSession := runtime.sessions["path-a"]
	clientSession, generation, healthy := pathSession.current(path)
	if !healthy {
		t.Fatal("path session is not healthy after opening two flows")
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("physical dials = %d, want 1 for two flows", got)
	}

	_ = firstFlow.Close()
	runtime.releaseCarrierGroup(firstGroup)
	_ = firstEndpoint.Close()
	waitTCPSessionManagerCondition(t, func() bool { return clientSession.StreamCount() == 1 })
	current, currentGeneration, healthy := pathSession.current(path)
	if !healthy || current != clientSession || currentGeneration != generation {
		t.Fatal("closing one flow replaced or closed the shared physical session")
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("physical dials after flow close = %d, want 1", got)
	}
	select {
	case <-secondFlow.Done():
		t.Fatalf("closing the first flow ended the second: %v", secondFlow.Err())
	default:
	}

	_ = secondFlow.Close()
	runtime.releaseCarrierGroup(secondGroup)
	_ = secondEndpoint.Close()
}

func TestTCPPathSessionReconnectReopensLiveFlow(t *testing.T) {
	paths := map[string]tcpClientPath{
		"path-a": {index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"},
		"path-b": {index: 2, address: "192.0.2.2", destination: "198.51.100.1:59501"},
	}
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, _ := installTCPSessionManagerDialer(t, runtime)
	runtime.refreshCarrierGroups(paths)
	initialPeers := map[string]*tcpstream.Session{}
	for len(initialPeers) < len(paths) {
		peer := waitAnyTCPSessionManagerPeer(t, peers)
		if peer.err != nil {
			t.Fatal(peer.err)
		}
		initialPeers[peer.interfaceName] = peer.session
	}

	flow, endpoint := newTCPSessionManagerFlow(t, runtime, 3)
	group := assignTCPSessionManagerGroup(t, runtime, flow)
	if !waitTCPSessionManagerConditionUntil(func() bool { return flow.CarrierCount() == 2 }) {
		t.Fatalf("initial virtual streams did not converge: %s", tcpSessionManagerGroupState(group))
	}
	initialClient, initialGeneration, healthy := runtime.sessions["path-a"].current(paths["path-a"])
	if !healthy {
		t.Fatal("initial path-a session is not healthy")
	}
	if err := initialPeers["path-a"].Close(); err != nil {
		t.Fatal(err)
	}

	var replacementPeer *tcpstream.Session
	deadline := time.NewTimer(tcpSessionManagerTestTimeout)
	defer deadline.Stop()
	for replacementPeer == nil {
		select {
		case peer := <-peers:
			if peer.err != nil {
				t.Fatal(peer.err)
			}
			if peer.interfaceName == "path-a" {
				replacementPeer = peer.session
			}
		case <-deadline.C:
			t.Fatal("path-a physical session was not reconnected")
		}
	}
	if !waitTCPSessionManagerConditionUntil(func() bool {
		current, generation, currentHealthy := runtime.sessions["path-a"].current(paths["path-a"])
		return currentHealthy && current != initialClient && generation > initialGeneration && flow.CarrierCount() == 2
	}) {
		current, generation, currentHealthy := runtime.sessions["path-a"].current(paths["path-a"])
		t.Fatalf("reconnected virtual stream did not converge: healthy=%v replaced=%v generation=%d initial-generation=%d flow-error=%v %s", currentHealthy, current != initialClient, generation, initialGeneration, flow.Err(), tcpSessionManagerGroupState(group))
	}
	select {
	case <-flow.Done():
		t.Fatalf("flow ended while another path remained available: %v", flow.Err())
	default:
	}

	_ = flow.Close()
	runtime.releaseCarrierGroup(group)
	_ = endpoint.Close()
}

func TestTCPFlowStreamReopensOnHealthySession(t *testing.T) {
	paths := map[string]tcpClientPath{
		"path-a": {index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"},
		"path-b": {index: 2, address: "192.0.2.2", destination: "198.51.100.1:59501"},
	}
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, dialCount := installTCPSessionManagerDialer(t, runtime)
	runtime.refreshCarrierGroups(paths)
	for range paths {
		peer := waitAnyTCPSessionManagerPeer(t, peers)
		if peer.err != nil {
			t.Fatal(peer.err)
		}
	}

	flow, endpoint := newTCPSessionManagerFlow(t, runtime, 4)
	group := assignTCPSessionManagerGroup(t, runtime, flow)
	waitTCPSessionManagerCondition(t, func() bool {
		runtime.mu.Lock()
		registered := runtime.carriers[flow.ID()]["path-a"] != nil
		runtime.mu.Unlock()
		return flow.CarrierCount() == 2 && registered
	})
	pathSession := runtime.sessions["path-a"]
	physical, generation, healthy := pathSession.current(paths["path-a"])
	if !healthy {
		t.Fatal("path-a session is not healthy")
	}
	runtime.mu.Lock()
	initial := runtime.carriers[flow.ID()]["path-a"]
	runtime.mu.Unlock()
	if initial == nil {
		t.Fatal("path-a virtual carrier was not registered")
	}
	initial.Close()

	waitTCPSessionManagerCondition(t, func() bool {
		runtime.mu.Lock()
		replacement := runtime.carriers[flow.ID()]["path-a"]
		runtime.mu.Unlock()
		return replacement != nil && replacement != initial && flow.CarrierCount() == 2
	})
	current, currentGeneration, healthy := pathSession.current(paths["path-a"])
	if !healthy || current != physical || currentGeneration != generation {
		t.Fatal("reopening a virtual stream replaced the healthy physical session")
	}
	if got := dialCount.Load(); got != int32(len(paths)) {
		t.Fatalf("physical dials = %d, want %d", got, len(paths))
	}

	_ = flow.Close()
	runtime.releaseCarrierGroup(group)
	_ = endpoint.Close()
}

func TestTCPFlowCommitSurvivesConcurrentPathReplacement(t *testing.T) {
	runtime := newTCPSessionManagerTestRuntime(t)
	initialPath := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	replacementPath := tcpClientPath{index: 1, address: "192.0.2.1", destination: "203.0.113.1:59501"}
	flow, endpoint := newTCPSessionManagerFlow(t, runtime, 5)
	defer endpoint.Close()
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.slots["path-a"] = &tcpFlowSlot{
		path:              initialPath,
		session:           new(tcpstream.Session),
		sessionGeneration: 1,
		inFlight:          true,
	}
	callbackStarted := make(chan struct{})
	allowCallback := make(chan struct{})
	var successReplies atomic.Int32
	var failureReplies atomic.Int32
	group.beforeAttach = func() error {
		successReplies.Add(1)
		close(callbackStarted)
		<-allowCallback
		return nil
	}
	group.onOpenFailed = func(error) { failureReplies.Add(1) }
	session := group.slots["path-a"].session

	carrierConn, carrierPeer := net.Pipe()
	defer carrierPeer.Close()
	attachDone := make(chan struct{})
	go func() {
		group.attach("path-a", initialPath, session, 1, carrierConn, tcpstream.MaxPayloadSize)
		close(attachDone)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("success callback did not start")
	}
	group.syncPaths(map[string]tcpClientPath{"path-a": replacementPath})
	close(allowCallback)
	select {
	case <-attachDone:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("attach did not finish after path replacement")
	}

	group.failOpen(tcpstream.ErrNoCarriers)
	if got := successReplies.Load(); got != 1 {
		t.Fatalf("success replies = %d, want 1", got)
	}
	if got := failureReplies.Load(); got != 0 {
		t.Fatalf("failure replies after committed success = %d, want 0", got)
	}
	group.mu.Lock()
	committed := group.committed
	started := group.started
	group.mu.Unlock()
	if !committed || started {
		t.Fatalf("committed/started = %v/%v, want true/false", committed, started)
	}
	select {
	case <-flow.Done():
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("failed replacement did not reset the committed flow")
	}
	group.close()
}

func TestTCPFlowOpenRejectionBlocksOnlyCurrentSessionGeneration(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, _ := installTCPSessionManagerDialer(t, runtime)
	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	initialPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if initialPeer.err != nil {
		t.Fatal(initialPeer.err)
	}
	var session *tcpstream.Session
	var generation uint64
	waitTCPSessionManagerCondition(t, func() bool {
		var healthy bool
		session, generation, healthy = runtime.sessions["path-a"].current(path)
		return healthy
	})

	flow, endpoint := newTCPSessionManagerFlow(t, runtime, 6)
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	group := newTCPCarrierGroup(runtime)
	group.flow = flow
	group.destination = destination
	group.slots["path-a"] = &tcpFlowSlot{
		path:              path,
		session:           session,
		sessionGeneration: generation,
		inFlight:          true,
	}
	runtime.mu.Lock()
	runtime.groups[group] = struct{}{}
	runtime.flows[flow.ID()] = flow
	runtime.carriers[flow.ID()] = make(map[string]*tcpstream.Carrier)
	runtime.mu.Unlock()
	group.rejectOpenAttempt("path-a", path, session, generation)
	group.reconcile("path-a")
	time.Sleep(20 * time.Millisecond)
	if got := initialPeer.session.StreamCount(); got != 0 {
		t.Fatalf("same-generation rejection reopened %d virtual streams, want 0", got)
	}

	if err := initialPeer.session.Close(); err != nil {
		t.Fatal(err)
	}
	replacementPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if replacementPeer.err != nil {
		t.Fatal(replacementPeer.err)
	}
	waitTCPSessionManagerCondition(t, func() bool { return flow.CarrierCount() == 1 })
	current, currentGeneration, healthy := runtime.sessions["path-a"].current(path)
	if !healthy || current == session || currentGeneration <= generation {
		t.Fatal("new physical session generation did not replace the rejected generation")
	}

	_ = flow.Close()
	runtime.releaseCarrierGroup(group)
	_ = endpoint.Close()
}

func TestTCPPathSessionRefreshReplacesChangedPath(t *testing.T) {
	initial := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	changed := tcpClientPath{index: 1, address: "192.0.2.1", destination: "203.0.113.1:59501"}
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, dialCount := installTCPSessionManagerDialer(t, runtime)
	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": initial})
	firstPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if firstPeer.err != nil {
		t.Fatal(firstPeer.err)
	}
	firstPathSession := runtime.sessions["path-a"]

	if !runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": changed}) {
		t.Fatal("changed path refresh was rejected")
	}
	secondPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if secondPeer.err != nil {
		t.Fatal(secondPeer.err)
	}
	if runtime.sessions["path-a"] == firstPathSession {
		t.Fatal("changed path retained the old physical session manager")
	}
	select {
	case <-firstPeer.session.Done():
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("changed path did not close the old physical session")
	}
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("physical dials after path change = %d, want 2", got)
	}

	if !runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": changed}) {
		t.Fatal("stable path refresh was rejected")
	}
	time.Sleep(20 * time.Millisecond)
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("stable refresh created %d physical dials, want 2", got)
	}
}

func TestTCPPathSessionRetryBackoffIsBounded(t *testing.T) {
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		5 * time.Second,
		5 * time.Second,
	}
	for retryCount, wantDelay := range want {
		if got := tcpSessionRetryDelay(retryCount); got != wantDelay {
			t.Fatalf("retry %d delay = %v, want %v", retryCount, got, wantDelay)
		}
	}
	for _, delay := range []time.Duration{tcpSessionRetryInitialDelay, time.Second, tcpSessionRetryMaxDelay} {
		minimum := delay - delay/5
		maximum := min(delay+delay/5, tcpSessionRetryMaxDelay)
		for range 1000 {
			got := jitterTCPSessionRetryDelay(delay)
			if got < minimum || got > maximum {
				t.Fatalf("jittered delay for %v = %v, want [%v, %v]", delay, got, minimum, maximum)
			}
		}
	}
}

func TestTCPPathSessionShortLivedSuccessesPreserveRetryBackoff(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	runtime := newTCPSessionManagerTestRuntime(t)
	delays := make(chan time.Duration, 4)
	errors := make(chan error, 4)
	var timerCalls atomic.Int32
	originalTimer := newTCPSessionRetryTimer
	originalJitter := jitterTCPSessionRetryDelay
	originalDial := dialTCPOnInterface
	newTCPSessionRetryTimer = func(delay time.Duration) (<-chan time.Time, func()) {
		delays <- delay
		ticks := make(chan time.Time, 1)
		if timerCalls.Add(1) < 3 {
			ticks <- time.Now()
		}
		return ticks, func() {}
	}
	jitterTCPSessionRetryDelay = func(delay time.Duration) time.Duration { return delay }
	var dialCount atomic.Int32
	dialTCPOnInterface = func(context.Context, string, string, string, time.Duration) (net.Conn, error) {
		attempt := dialCount.Add(1)
		clientConn, serverConn := net.Pipe()
		go func() {
			serverSession, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), nil)
			if err != nil {
				errors <- err
				return
			}
			deadline := time.Now().Add(tcpSessionManagerTestTimeout)
			for time.Now().Before(deadline) {
				runtime.mu.Lock()
				pathSession := runtime.sessions["path-a"]
				runtime.mu.Unlock()
				if pathSession != nil {
					pathSession.mu.Lock()
					ready := pathSession.session != nil && pathSession.generation == uint64(attempt)
					pathSession.mu.Unlock()
					if ready {
						_ = serverSession.Close()
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
			_ = serverSession.Close()
			errors <- fmt.Errorf("attempt %d was not installed as a client session", attempt)
		}()
		return clientConn, nil
	}
	defer func() {
		newTCPSessionRetryTimer = originalTimer
		jitterTCPSessionRetryDelay = originalJitter
		dialTCPOnInterface = originalDial
	}()

	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for index, wantDelay := range want {
		select {
		case err := <-errors:
			t.Fatal(err)
		case got := <-delays:
			if got != wantDelay {
				t.Fatalf("retry %d delay = %v, want %v", index, got, wantDelay)
			}
		case <-time.After(tcpSessionManagerTestTimeout):
			t.Fatalf("timed out waiting for retry %d", index)
		}
	}
	runtime.shutdown()
}

func TestTCPSessionRetryStabilityBoundary(t *testing.T) {
	establishedAt := time.Unix(100, 0)
	if tcpSessionWasStable(establishedAt, establishedAt.Add(tcpSessionRetryMaxDelay-time.Nanosecond)) {
		t.Fatal("session shorter than the stability period reset retry backoff")
	}
	if !tcpSessionWasStable(establishedAt, establishedAt.Add(tcpSessionRetryMaxDelay)) {
		t.Fatal("session at the stability boundary did not reset retry backoff")
	}
}

func TestTCPCarrierGroupRegistersMaxStreamsAtomically(t *testing.T) {
	runtime := newTCPSessionManagerTestRuntime(t)
	runtime.client.cfg.Transfer.TCP.MaxStreams = 1

	type result struct {
		flow     *tcpstream.Flow
		group    *tcpCarrierGroup
		endpoint net.Conn
		err      error
	}
	const attempts = 16
	start := make(chan struct{})
	results := make(chan result, attempts)
	for index := range attempts {
		flow, endpoint := newTCPSessionManagerFlow(t, runtime, byte(index+10))
		go func() {
			<-start
			group, err := runtime.assignCarrierGroup(flow, tcpstream.Destination{}, nil, nil)
			results <- result{flow: flow, group: group, endpoint: endpoint, err: err}
		}()
	}
	close(start)

	all := make([]result, 0, attempts)
	successes := 0
	for range attempts {
		attempt := <-results
		all = append(all, attempt)
		if attempt.err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent assignments registered %d streams, want 1", successes)
	}
	for _, attempt := range all {
		_ = attempt.flow.Close()
		if attempt.group != nil {
			runtime.releaseCarrierGroup(attempt.group)
		}
		_ = attempt.endpoint.Close()
	}
	if len(runtime.flows) != 0 || len(runtime.groups) != 0 {
		t.Fatalf("cleanup left flows/groups = %d/%d", len(runtime.flows), len(runtime.groups))
	}
}

func newTCPSessionManagerTestRuntime(t *testing.T) *tcpClientRuntime {
	t.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.KeepaliveIntervalMillis = 50
	transfer.KeepaliveTimeoutMillis = 500
	client := New(config.Client{Transfer: transfer}, "test", nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &tcpClientRuntime{
		client:       client,
		ctx:          ctx,
		cancel:       cancel,
		flows:        make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:        make(map[string]tcpClientPath),
		carriers:     make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		sessions:     make(map[string]*tcpPathSession),
		groups:       make(map[*tcpCarrierGroup]struct{}),
		accepted:     make(map[*tcpAcceptedConn]struct{}),
		lastReceived: make(map[string]*atomic.Int64),
	}
	t.Cleanup(runtime.shutdown)
	return runtime
}

func installTCPSessionManagerDialer(t *testing.T, runtime *tcpClientRuntime) (<-chan tcpSessionManagerPeer, *atomic.Int32) {
	t.Helper()
	peers := make(chan tcpSessionManagerPeer, 32)
	var dialCount atomic.Int32
	originalDial := dialTCPOnInterface
	dialTCPOnInterface = func(ctx context.Context, _ string, _ string, interfaceName string, _ time.Duration) (net.Conn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		dialCount.Add(1)
		clientConn, serverConn := net.Pipe()
		go func() {
			session, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), nil)
			peers <- tcpSessionManagerPeer{interfaceName: interfaceName, session: session, err: err}
			if err == nil {
				serveTCPSessionManagerPeer(session)
			}
		}()
		return clientConn, nil
	}
	t.Cleanup(func() { dialTCPOnInterface = originalDial })
	return peers, &dialCount
}

func serveTCPSessionManagerPeer(session *tcpstream.Session) {
	for {
		conn, maxPayload, err := session.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			open, readErr := tcpstream.ReadFrame(conn, maxPayload)
			if readErr == nil && open.Type != tcpstream.FrameOpen {
				readErr = tcpstream.ErrInvalidFrame
			}
			if readErr == nil {
				readErr = tcpstream.WriteFrame(conn, tcpstream.Frame{
					Type:      tcpstream.FrameOpenResult,
					Direction: tcpstream.DirectionServerToClient,
					StreamID:  open.StreamID,
					Payload:   []byte{byte(tcpstream.OpenResultSuccess)},
				})
			}
			if readErr == nil {
				<-session.Done()
			}
			_ = conn.Close()
		}()
	}
}

func newTCPSessionManagerFlow(t *testing.T, runtime *tcpClientRuntime, lastByte byte) (*tcpstream.Flow, net.Conn) {
	t.Helper()
	endpoint, peer := net.Pipe()
	var streamID tcpstream.StreamID
	streamID[len(streamID)-1] = lastByte
	return tcpstream.NewFlow(streamID, endpoint, tcpstream.DirectionClientToServer, runtime.flowConfig()), peer
}

func assignTCPSessionManagerGroup(t *testing.T, runtime *tcpClientRuntime, flow *tcpstream.Flow) *tcpCarrierGroup {
	t.Helper()
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	var approvals atomic.Int32
	group, err := runtime.assignCarrierGroup(flow, destination, func() error {
		if approvals.Add(1) != 1 {
			return errors.New("SOCKS success callback called more than once")
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-group.startedReady:
	case <-flow.Done():
		t.Fatalf("flow failed before first virtual carrier: %v", flow.Err())
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("flow did not open a virtual carrier")
	}
	return group
}

func waitTCPSessionManagerPeer(t *testing.T, peers <-chan tcpSessionManagerPeer, interfaceName string) tcpSessionManagerPeer {
	t.Helper()
	deadline := time.NewTimer(tcpSessionManagerTestTimeout)
	defer deadline.Stop()
	for {
		select {
		case peer := <-peers:
			if peer.interfaceName == interfaceName {
				return peer
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s session", interfaceName)
			return tcpSessionManagerPeer{}
		}
	}
}

func waitAnyTCPSessionManagerPeer(t *testing.T, peers <-chan tcpSessionManagerPeer) tcpSessionManagerPeer {
	t.Helper()
	select {
	case peer := <-peers:
		return peer
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("timed out waiting for physical session")
		return tcpSessionManagerPeer{}
	}
}

func waitTCPSessionManagerCondition(t *testing.T, condition func() bool) {
	t.Helper()
	if waitTCPSessionManagerConditionUntil(condition) {
		return
	}
	t.Fatal("timed out waiting for session-manager condition")
}

func waitTCPSessionManagerConditionUntil(condition func() bool) bool {
	deadline := time.Now().Add(tcpSessionManagerTestTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func tcpSessionManagerGroupState(group *tcpCarrierGroup) string {
	carrierCount := group.flow.CarrierCount()
	group.mu.Lock()
	defer group.mu.Unlock()
	state := ""
	for interfaceName, slot := range group.slots {
		state += fmt.Sprintf("%s={carrier:%v in-flight:%v retrying:%v retry-count:%d rejected:%v generation:%d} ", interfaceName, slot.carrier != nil, slot.inFlight, slot.retrying, slot.retryCount, slot.openRejected, slot.sessionGeneration)
	}
	return fmt.Sprintf("flow-carriers=%d started=%v committed=%v failed=%v slots=[%s]", carrierCount, group.started, group.committed, group.failed, state)
}
