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
	retryTimers := installTCPManualSessionRetryTimer(t)
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
	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("replacement retry delay = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	pathSession := runtime.sessions["path-a"]
	pathSession.mu.Lock()
	retrying := pathSession.retrying
	retryCount := pathSession.retryCount
	generationBeforeTick := pathSession.generation
	pathSession.mu.Unlock()
	if !retrying || retryCount != 1 || generationBeforeTick != initialGeneration {
		t.Fatalf("retry state before tick = retrying %v/count %d/generation %d, want true/1/%d", retrying, retryCount, generationBeforeTick, initialGeneration)
	}
	retryTimers.assertNoPending(t)
	retryTimer.fire(t)

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
		return currentHealthy && current != initialClient && generation == initialGeneration+1 && flow.CarrierCount() == 2
	}) {
		current, generation, currentHealthy := runtime.sessions["path-a"].current(paths["path-a"])
		t.Fatalf("reconnected virtual stream did not converge: healthy=%v replaced=%v generation=%d initial-generation=%d flow-error=%v %s", currentHealthy, current != initialClient, generation, initialGeneration, flow.Err(), tcpSessionManagerGroupState(group))
	}
	select {
	case <-flow.Done():
		t.Fatalf("flow ended while another path remained available: %v", flow.Err())
	default:
	}
	retryTimers.assertNoPending(t)

	_ = flow.Close()
	runtime.releaseCarrierGroup(group)
	_ = endpoint.Close()
}

func TestTCPFlowStreamReopensOnHealthySession(t *testing.T) {
	paths := map[string]tcpClientPath{
		"path-a": {index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"},
		"path-b": {index: 2, address: "192.0.2.2", destination: "198.51.100.1:59501"},
	}
	retryTimers := installTCPManualFlowRetryTimer(t)
	previousNow := tcpRetryNow
	baseTime := time.Unix(200, 0)
	var nowNanos atomic.Int64
	nowNanos.Store(baseTime.UnixNano())
	var nowCalls atomic.Int32
	tcpRetryNow = func() time.Time {
		nowCalls.Add(1)
		return time.Unix(0, nowNanos.Load())
	}
	t.Cleanup(func() { tcpRetryNow = previousNow })
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
	waitTCPSessionManagerCondition(t, func() bool { return nowCalls.Load() >= int32(2*len(paths)) })
	group.mu.Lock()
	group.slots["path-a"].retryCount = 3
	group.mu.Unlock()
	nowNanos.Store(baseTime.Add(tcpFlowRetryStablePeriod).UnixNano())
	initial.Close()

	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("stable carrier retry delay = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	group.mu.Lock()
	slot := group.slots["path-a"]
	retrying := slot.retrying
	retryCount := slot.retryCount
	slotSession := slot.session
	slotGeneration := slot.sessionGeneration
	group.mu.Unlock()
	runtime.mu.Lock()
	removed := runtime.carriers[flow.ID()]["path-a"] == nil
	runtime.mu.Unlock()
	if !removed || !retrying || retryCount != 1 || slotSession != physical || slotGeneration != generation {
		t.Fatalf("stable retry state = removed %v/retrying %v/count %d/session-current %v/generation %d, want true/true/1/true/%d", removed, retrying, retryCount, slotSession == physical, slotGeneration, generation)
	}
	retryTimers.assertNoPending(t)
	retryTimer.fire(t)
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
	group.mu.Lock()
	slot = group.slots["path-a"]
	retrying = slot.retrying
	retryCount = slot.retryCount
	group.mu.Unlock()
	if retrying || retryCount != 1 {
		t.Fatalf("reopened slot state = retrying %v/count %d, want false/1", retrying, retryCount)
	}
	retryTimers.assertNoPending(t)

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
	retryTimers := installTCPManualSessionRetryTimer(t)
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
	group.mu.Lock()
	slot := group.slots["path-a"]
	rejected := slot.openRejected
	inFlight := slot.inFlight
	slotGeneration := slot.sessionGeneration
	group.mu.Unlock()
	if !rejected || inFlight || slotGeneration != generation {
		t.Fatalf("rejected slot state = rejected %v/in-flight %v/generation %d, want true/false/%d", rejected, inFlight, slotGeneration, generation)
	}
	if got := initialPeer.session.StreamCount(); got != 0 {
		t.Fatalf("same-generation rejection reopened %d virtual streams, want 0", got)
	}

	if err := initialPeer.session.Close(); err != nil {
		t.Fatal(err)
	}
	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("new-generation retry delay = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	retryTimers.assertNoPending(t)
	retryTimer.fire(t)
	replacementPeer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if replacementPeer.err != nil {
		t.Fatal(replacementPeer.err)
	}
	waitTCPSessionManagerCondition(t, func() bool { return flow.CarrierCount() == 1 })
	current, currentGeneration, healthy := runtime.sessions["path-a"].current(path)
	if !healthy || current == session || currentGeneration != generation+1 {
		t.Fatalf("new physical session = healthy %v/replaced %v/generation %d, want true/true/%d", healthy, current != session, currentGeneration, generation+1)
	}
	retryTimers.assertNoPending(t)

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
	secondPathSession := runtime.sessions["path-a"]
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
	if runtime.sessions["path-a"] != secondPathSession {
		t.Fatal("stable refresh replaced the current path session")
	}
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
	jitterCases := []struct {
		name   string
		delay  time.Duration
		offset time.Duration
		want   time.Duration
	}{
		{name: "lower boundary", delay: 100 * time.Millisecond, offset: 0, want: 80 * time.Millisecond},
		{name: "midpoint", delay: 100 * time.Millisecond, offset: 20 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "upper boundary", delay: 100 * time.Millisecond, offset: 40 * time.Millisecond, want: 120 * time.Millisecond},
		{name: "maximum cap", delay: tcpSessionRetryMaxDelay, offset: 2 * tcpSessionRetryMaxDelay / 5, want: tcpSessionRetryMaxDelay},
	}
	for _, testCase := range jitterCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := applyTCPSessionRetryJitter(testCase.delay, testCase.offset); got != testCase.want {
				t.Fatalf("jittered delay = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestTCPPathSessionRetryIgnoresStaleGenerationTimer(t *testing.T) {
	retryTimers := installTCPManualSessionRetryTimer(t)
	runtime := newTCPSessionManagerTestRuntime(t)
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	pathSession := newTCPPathSession(runtime, "path-a", path)
	pathSession.generation = 2
	pathSession.retrying = true
	pathSession.retryCount = 2

	done := make(chan struct{})
	go func() {
		pathSession.waitToRetry(1, tcpSessionRetryInitialDelay)
		close(done)
	}()
	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("stale retry delay = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	retryTimers.assertNoPending(t)
	retryTimer.fire(t)
	select {
	case <-done:
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("stale generation retry did not return")
	}

	pathSession.mu.Lock()
	generation := pathSession.generation
	retrying := pathSession.retrying
	retryCount := pathSession.retryCount
	inFlight := pathSession.inFlight
	pathSession.mu.Unlock()
	if generation != 2 || !retrying || retryCount != 2 || inFlight {
		t.Fatalf("state after stale timer = generation %d/retrying %v/count %d/in-flight %v, want 2/true/2/false", generation, retrying, retryCount, inFlight)
	}
	retryTimers.assertNoPending(t)
	pathSession.close()
}

func TestTCPPathSessionShortLivedSuccessesPreserveRetryBackoff(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	retryTimers := installTCPManualSessionRetryTimer(t)
	previousNow := tcpRetryNow
	now := time.Unix(123, 0)
	tcpRetryNow = func() time.Time { return now }
	t.Cleanup(func() { tcpRetryNow = previousNow })
	runtime := newTCPSessionManagerTestRuntime(t)
	peers := make(chan tcpSessionManagerPeer, 4)
	originalDial := dialTCPOnInterface
	var dialCount atomic.Int32
	dialTCPOnInterface = func(context.Context, string, string, string, time.Duration) (net.Conn, error) {
		dialCount.Add(1)
		clientConn, serverConn := net.Pipe()
		go func() {
			serverSession, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), nil)
			peers <- tcpSessionManagerPeer{interfaceName: "path-a", session: serverSession, err: err}
		}()
		return clientConn, nil
	}
	t.Cleanup(func() { dialTCPOnInterface = originalDial })

	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	pathSession := runtime.sessions["path-a"]
	for index, wantDelay := range want {
		peer := waitTCPSessionManagerPeer(t, peers, "path-a")
		if peer.err != nil {
			t.Fatal(peer.err)
		}
		wantGeneration := uint64(index + 1)
		waitTCPSessionManagerCondition(t, func() bool {
			_, generation, healthy := pathSession.current(path)
			return healthy && generation == wantGeneration
		})
		if err := peer.session.Close(); err != nil {
			t.Fatal(err)
		}
		retryTimer := retryTimers.next(t)
		if retryTimer.delay != wantDelay {
			t.Fatalf("retry %d delay = %v, want %v", index, retryTimer.delay, wantDelay)
		}
		pathSession.mu.Lock()
		retryCount := pathSession.retryCount
		generation := pathSession.generation
		pathSession.mu.Unlock()
		if retryCount != index+1 || generation != wantGeneration {
			t.Fatalf("retry %d state = count %d/generation %d, want %d/%d", index, retryCount, generation, index+1, wantGeneration)
		}
		retryTimers.assertNoPending(t)
		if index+1 < len(want) {
			retryTimer.fire(t)
		}
	}
	if got := dialCount.Load(); got != int32(len(want)) {
		t.Fatalf("physical dials = %d, want %d", got, len(want))
	}
	runtime.shutdown()
}

func TestTCPPathSessionStableSuccessResetsRetryBackoff(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	retryTimers := installTCPManualSessionRetryTimer(t)
	previousNow := tcpRetryNow
	baseTime := time.Unix(300, 0)
	var nowNanos atomic.Int64
	nowNanos.Store(baseTime.UnixNano())
	tcpRetryNow = func() time.Time { return time.Unix(0, nowNanos.Load()) }
	t.Cleanup(func() { tcpRetryNow = previousNow })
	runtime := newTCPSessionManagerTestRuntime(t)
	peers, dialCount := installTCPSessionManagerDialer(t, runtime)

	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	peer := waitTCPSessionManagerPeer(t, peers, "path-a")
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	pathSession := runtime.sessions["path-a"]
	waitTCPSessionManagerCondition(t, func() bool {
		_, generation, healthy := pathSession.current(path)
		return healthy && generation == 1
	})
	pathSession.mu.Lock()
	pathSession.retryCount = 3
	pathSession.mu.Unlock()
	nowNanos.Store(baseTime.Add(tcpSessionRetryMaxDelay).UnixNano())
	if err := peer.session.Close(); err != nil {
		t.Fatal(err)
	}

	retryTimer := retryTimers.next(t)
	if retryTimer.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("retry delay after stable session = %v, want %v", retryTimer.delay, tcpSessionRetryInitialDelay)
	}
	pathSession.mu.Lock()
	retrying := pathSession.retrying
	retryCount := pathSession.retryCount
	generation := pathSession.generation
	pathSession.mu.Unlock()
	if !retrying || retryCount != 1 || generation != 1 {
		t.Fatalf("retry state after stable session = retrying %v/count %d/generation %d, want true/1/1", retrying, retryCount, generation)
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("physical dials before retry tick = %d, want 1", got)
	}
	retryTimers.assertNoPending(t)
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

func TestTCPSessionProbeProtocolErrorClassification(t *testing.T) {
	if !tcpSessionProbeProtocolError(tcpstream.ErrInvalidFrame) {
		t.Fatal("ErrInvalidFrame was not classified as a Session-level probe error")
	}
	if !tcpSessionProbeProtocolError(fmt.Errorf("wrapped: %w", tcpstream.ErrPayloadLength)) {
		t.Fatal("wrapped ErrPayloadLength was not classified as a Session-level probe error")
	}
	if tcpSessionProbeProtocolError(errors.New("stream I/O failure")) {
		t.Fatal("ordinary stream I/O failure was classified as a Session-level probe error")
	}
}

func TestTCPPathSessionZeroProbeRTTRefreshesHealthWithoutBiasingScore(t *testing.T) {
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}
	session := newActiveSelectionSession(t, tcpstream.ServerInstanceID{1})
	probe := &tcpstream.SessionProbe{}
	pathSession := &tcpPathSession{
		path:    path,
		session: session,
		probe:   probe,
	}
	probedAt := time.Unix(100, 0)

	if !pathSession.recordProbeRTT(session, probe, 0, probedAt) {
		t.Fatal("successful zero-duration probe was rejected")
	}
	status := pathSession.qualityStatus(path, config.InterfaceCostNormal, probedAt)
	if status.state != "healthy" {
		t.Fatalf("quality state = %q, want healthy", status.state)
	}
	if status.rttMillis != 100 || status.jitterMillis != 0 || status.scoreMillis != 100 {
		t.Fatalf("quality RTT/jitter/score = %.3f/%.3f/%.3fms, want 100/0/100ms fallback", status.rttMillis, status.jitterMillis, status.scoreMillis)
	}

	pathSession.mu.Lock()
	pathSession.rttEWMA = float64(25 * time.Millisecond)
	pathSession.jitterEWMA = float64(5 * time.Millisecond)
	pathSession.lastProbe = time.Time{}
	pathSession.mu.Unlock()
	probedAt = probedAt.Add(time.Second)
	if !pathSession.recordProbeRTT(session, probe, 0, probedAt) {
		t.Fatal("second successful zero-duration probe was rejected")
	}
	status = pathSession.qualityStatus(path, config.InterfaceCostNormal, probedAt)
	if status.rttMillis != 25 || status.jitterMillis != 5 {
		t.Fatalf("quality RTT/jitter = %.3f/%.3fms, want 25/5ms", status.rttMillis, status.jitterMillis)
	}
	pathSession.mu.Lock()
	lastProbe := pathSession.lastProbe
	pathSession.mu.Unlock()
	if !lastProbe.Equal(probedAt) {
		t.Fatalf("last probe = %v, want %v", lastProbe, probedAt)
	}
}

func TestTCPPathSessionProbeFailureDoesNotReplaceSession(t *testing.T) {
	probeTimers := installTCPManualSessionProbeTimer(t)
	retryTimers := installTCPManualSessionRetryTimer(t)
	runtime := newTCPSessionManagerTestRuntime(t)
	runtime.client.cfg.Transfer.TCP.CarrierMode = config.TCPCarrierModeActiveStandby
	runtime.client.cfg.Transfer.ApplyDefaults()
	path := tcpClientPath{index: 1, address: "192.0.2.1", destination: "198.51.100.1:59501"}

	events := make(chan string, 4)
	serverReady := make(chan error, 1)
	serverDone := make(chan error, 1)
	originalDial := dialTCPOnInterface
	var dialCount atomic.Int32
	dialTCPOnInterface = func(context.Context, string, string, string, time.Duration) (net.Conn, error) {
		if dialCount.Add(1) != 1 {
			return nil, errors.New("unexpected physical redial")
		}
		clientConn, serverConn := net.Pipe()
		go func() {
			serverConfig := runtime.sessionConfig()
			serverConfig.ServerInstanceID = tcpstream.ServerInstanceID{1}
			serverConfig.OrphanRetention = time.Duration(runtime.client.cfg.Transfer.TCP.ServerOrphanRetentionMillis) * time.Millisecond
			session, _, err := tcpstream.AcceptSession(serverConn, tcpstream.MaxPayloadSize, time.Second, serverConfig, nil)
			serverReady <- err
			if err != nil {
				serverDone <- err
				return
			}
			defer session.Close()
			serverDone <- serveTCPSessionManagerProbeScript(session, events)
		}()
		return clientConn, nil
	}
	t.Cleanup(func() { dialTCPOnInterface = originalDial })

	runtime.refreshCarrierGroups(map[string]tcpClientPath{"path-a": path})
	select {
	case err := <-serverReady:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("timed out waiting for active-standby Session handshake")
	}
	waitTCPSessionManagerProbeEvent(t, events, "initial-failure")
	pathSession := runtime.sessions["path-a"]
	waitTCPSessionManagerCondition(t, func() bool {
		session, generation, available := pathSession.current(path)
		pathSession.mu.Lock()
		probe := pathSession.probe
		pathSession.mu.Unlock()
		return available && session != nil && generation == 1 && probe == nil
	})
	physicalSession, generation, available := pathSession.current(path)
	if !available || generation != 1 {
		t.Fatalf("initial Session availability/generation = %v/%d, want true/1", available, generation)
	}
	if quality := pathSession.qualityStatus(path, config.InterfaceCostNormal, tcpRetryNow()); quality.state != "degraded" {
		t.Fatalf("quality after initial probe failure = %q, want degraded", quality.state)
	}
	if physicalSession.IsClosed() || dialCount.Load() != 1 {
		t.Fatalf("initial probe failure closed/redialed Session = %v/%d, want false/1", physicalSession.IsClosed(), dialCount.Load())
	}
	retryTimers.assertNoPending(t)

	firstRetry := probeTimers.next(t)
	if firstRetry.delay != tcpSessionProbeStandbyInterval {
		t.Fatalf("first probe retry delay = %v, want %v", firstRetry.delay, tcpSessionProbeStandbyInterval)
	}
	firstRetry.fire(t)
	waitTCPSessionManagerProbeEvent(t, events, "first-recovery")
	var firstReplacement *tcpstream.SessionProbe
	waitTCPSessionManagerCondition(t, func() bool {
		pathSession.mu.Lock()
		firstReplacement = pathSession.probe
		pathSession.mu.Unlock()
		return firstReplacement != nil && pathSession.qualityStatus(path, config.InterfaceCostNormal, tcpRetryNow()).state == "healthy"
	})

	failedEstablishedProbe := probeTimers.next(t)
	failedEstablishedProbe.fire(t)
	waitTCPSessionManagerProbeEvent(t, events, "established-failure")
	waitTCPSessionManagerCondition(t, func() bool {
		pathSession.mu.Lock()
		probe := pathSession.probe
		pathSession.mu.Unlock()
		return probe == nil
	})
	pathSession.mu.Lock()
	lastSuccessfulProbe := pathSession.lastProbe
	pathSession.mu.Unlock()
	if quality := pathSession.qualityStatus(path, config.InterfaceCostNormal, lastSuccessfulProbe.Add(tcpSessionProbeStandbyInterval)); quality.state != "healthy" {
		t.Fatalf("quality after one established probe failure = %q, want healthy during grace period", quality.state)
	}
	staleAt := lastSuccessfulProbe.Add(2*tcpSessionProbeStandbyInterval + tcpSessionProbeTimeout + time.Nanosecond)
	if quality := pathSession.qualityStatus(path, config.InterfaceCostNormal, staleAt); quality.state != "degraded" {
		t.Fatalf("quality after probe grace period = %q, want degraded", quality.state)
	}
	currentSession, currentGeneration, currentAvailable := pathSession.current(path)
	if !currentAvailable || currentSession != physicalSession || currentGeneration != generation || physicalSession.IsClosed() || dialCount.Load() != 1 {
		t.Fatalf("Session after established probe failure = available %v/same %v/generation %d/closed %v/dials %d, want true/true/1/false/1", currentAvailable, currentSession == physicalSession, currentGeneration, physicalSession.IsClosed(), dialCount.Load())
	}
	retryTimers.assertNoPending(t)

	secondRetry := probeTimers.next(t)
	secondRetry.fire(t)
	waitTCPSessionManagerProbeEvent(t, events, "second-recovery")
	waitTCPSessionManagerCondition(t, func() bool {
		pathSession.mu.Lock()
		probe := pathSession.probe
		pathSession.mu.Unlock()
		return probe != nil && probe != firstReplacement && pathSession.qualityStatus(path, config.InterfaceCostNormal, tcpRetryNow()).state == "healthy"
	})
	currentSession, currentGeneration, currentAvailable = pathSession.current(path)
	if !currentAvailable || currentSession != physicalSession || currentGeneration != generation || physicalSession.IsClosed() || dialCount.Load() != 1 {
		t.Fatalf("Session after probe recovery = available %v/same %v/generation %d/closed %v/dials %d, want true/true/1/false/1", currentAvailable, currentSession == physicalSession, currentGeneration, physicalSession.IsClosed(), dialCount.Load())
	}
	retryTimers.assertNoPending(t)

	protocolFailure := probeTimers.next(t)
	protocolFailure.fire(t)
	waitTCPSessionManagerProbeEvent(t, events, "protocol-failure")
	sessionRetry := retryTimers.next(t)
	if sessionRetry.delay != tcpSessionRetryInitialDelay {
		t.Fatalf("Session retry delay after probe protocol error = %v, want %v", sessionRetry.delay, tcpSessionRetryInitialDelay)
	}
	waitTCPSessionManagerCondition(t, func() bool {
		_, _, available := pathSession.current(path)
		return !available && physicalSession.IsClosed()
	})
	pathSession.mu.Lock()
	currentGeneration = pathSession.generation
	retrying := pathSession.retrying
	pathSession.mu.Unlock()
	if currentGeneration != generation || !retrying || dialCount.Load() != 1 {
		t.Fatalf("Session after probe protocol error = generation %d/retrying %v/dials %d, want 1/true/1", currentGeneration, retrying, dialCount.Load())
	}

	runtime.shutdown()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatal("probe script did not stop with the physical Session")
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

func serveTCPSessionManagerProbeScript(session *tcpstream.Session, events chan<- string) error {
	first, maxPayload, err := session.AcceptStream()
	if err != nil {
		return fmt.Errorf("accept initial probe: %w", err)
	}
	if _, err := readTCPSessionManagerPing(first, maxPayload); err != nil {
		_ = first.Close()
		return fmt.Errorf("read initial probe: %w", err)
	}
	_ = first.Close()
	events <- "initial-failure"

	second, maxPayload, err := session.AcceptStream()
	if err != nil {
		return fmt.Errorf("accept first replacement probe: %w", err)
	}
	ping, err := readTCPSessionManagerPing(second, maxPayload)
	if err != nil {
		_ = second.Close()
		return fmt.Errorf("read first replacement probe: %w", err)
	}
	if err := writeTCPSessionManagerPong(second, ping); err != nil {
		_ = second.Close()
		return fmt.Errorf("write first replacement Pong: %w", err)
	}
	events <- "first-recovery"
	if _, err := readTCPSessionManagerPing(second, maxPayload); err != nil {
		_ = second.Close()
		return fmt.Errorf("read established probe: %w", err)
	}
	_ = second.Close()
	events <- "established-failure"

	third, maxPayload, err := session.AcceptStream()
	if err != nil {
		return fmt.Errorf("accept second replacement probe: %w", err)
	}
	ping, err = readTCPSessionManagerPing(third, maxPayload)
	if err != nil {
		_ = third.Close()
		return fmt.Errorf("read second replacement probe: %w", err)
	}
	if err := writeTCPSessionManagerPong(third, ping); err != nil {
		_ = third.Close()
		return fmt.Errorf("write second replacement Pong: %w", err)
	}
	events <- "second-recovery"
	ping, err = readTCPSessionManagerPing(third, maxPayload)
	if err != nil {
		_ = third.Close()
		return fmt.Errorf("read protocol-failure probe: %w", err)
	}
	if err := tcpstream.WriteFrame(third, tcpstream.Frame{
		Type:      tcpstream.FramePong,
		Direction: tcpstream.DirectionServerToClient,
		Offset:    ping.Offset + 1,
	}); err != nil {
		_ = third.Close()
		return fmt.Errorf("write invalid probe Pong: %w", err)
	}
	events <- "protocol-failure"
	<-session.Done()
	_ = third.Close()
	return nil
}

func readTCPSessionManagerPing(conn net.Conn, maxPayload uint32) (tcpstream.Frame, error) {
	frame, err := tcpstream.ReadFrame(conn, maxPayload)
	if err != nil {
		return tcpstream.Frame{}, err
	}
	if frame.Type != tcpstream.FramePing {
		return tcpstream.Frame{}, fmt.Errorf("probe frame type = %d, want Ping", frame.Type)
	}
	return frame, nil
}

func writeTCPSessionManagerPong(conn net.Conn, ping tcpstream.Frame) error {
	return tcpstream.WriteFrame(conn, tcpstream.Frame{
		Type:      tcpstream.FramePong,
		Direction: tcpstream.DirectionServerToClient,
		Offset:    ping.Offset,
	})
}

func waitTCPSessionManagerProbeEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	select {
	case event := <-events:
		if event != want {
			t.Fatalf("probe event = %q, want %q", event, want)
		}
	case <-time.After(tcpSessionManagerTestTimeout):
		t.Fatalf("timed out waiting for probe event %q", want)
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
