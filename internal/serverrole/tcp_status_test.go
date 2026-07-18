package serverrole

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

func TestTCPServerStatusConcurrentWithRuntimeUpdates(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:          New(config.Server{Transfer: transfer}, "test", nil),
		streams:         make(map[tcpstream.StreamID]*tcpServerStream),
		closed:          make(map[tcpstream.StreamID]time.Time),
		closedOrder:     list.New(),
		closedItems:     make(map[tcpstream.StreamID]*list.Element),
		sessions:        make(map[*tcpstream.Session]struct{}),
		traffic:         make(map[string]*tcpServerTraffic),
		inactiveTraffic: list.New(),
	}
	flowConfig := tcpstream.FlowConfig{ChunkSize: 1, CarrierQueueBytes: 1, ReorderWindowBytes: 1}
	start := make(chan struct{})
	errCh := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 200 {
				status := runtime.status()
				if status.Streams != len(status.TCPStreams) {
					errCh <- fmt.Errorf("stream count/list = %d/%d", status.Streams, len(status.TCPStreams))
					return
				}
				if !sort.SliceIsSorted(status.TCPStreams, func(left, right int) bool {
					return status.TCPStreams[left].ID < status.TCPStreams[right].ID
				}) {
					errCh <- fmt.Errorf("TCP stream status is not sorted")
					return
				}
				if !sort.SliceIsSorted(status.Sockets, func(left, right int) bool {
					return status.Sockets[left].Address < status.Sockets[right].Address
				}) {
					errCh <- fmt.Errorf("socket status is not sorted")
					return
				}
			}
		}()
	}
	close(start)
	for index := range 400 {
		var streamID tcpstream.StreamID
		binary.BigEndian.PutUint64(streamID[:8], uint64(index+1))
		flow := tcpstream.NewFlow(streamID, nil, tcpstream.DirectionServerToClient, flowConfig)
		address := fmt.Sprintf("192.0.2.%d", index)
		now := time.Now()
		runtime.mu.Lock()
		runtime.streams[streamID] = &tcpServerStream{version: tcpstream.Version, destination: "example.com:443", flow: flow}
		traffic := runtime.trafficForAddressLocked(address, now)
		traffic.Data.RecordRX(index)
		runtime.rememberClosedLocked(streamID, now)
		if index >= 64 {
			var previous tcpstream.StreamID
			binary.BigEndian.PutUint64(previous[:8], uint64(index-63))
			delete(runtime.streams, previous)
		}
		runtime.mu.Unlock()
	}
	readers.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestTCPServerStatusReleasesRuntimeLockBeforeFlowSnapshot(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	streamID := tcpstream.StreamID{1}
	flow := tcpstream.NewFlow(streamID, nil, tcpstream.DirectionServerToClient, tcpstream.FlowConfig{
		ChunkSize: 1, CarrierQueueBytes: 1, ReorderWindowBytes: 1,
	})
	runtime := &tcpServerRuntime{
		server:          New(config.Server{Transfer: transfer}, "test", nil),
		streams:         map[tcpstream.StreamID]*tcpServerStream{streamID: {version: tcpstream.Version, destination: "example.com:443", flow: flow}},
		closed:          make(map[tcpstream.StreamID]time.Time),
		closedOrder:     list.New(),
		closedItems:     make(map[tcpstream.StreamID]*list.Element),
		traffic:         make(map[string]*tcpServerTraffic),
		inactiveTraffic: list.New(),
	}

	previousCarrierCount := tcpFlowCarrierCount
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		tcpFlowCarrierCount = previousCarrierCount
	})
	tcpFlowCarrierCount = func(got *tcpstream.Flow) int {
		if got != flow {
			t.Errorf("CarrierCount flow = %p, want %p", got, flow)
		}
		close(started)
		<-release
		return 7
	}

	statusDone := make(chan control.ServerStatus, 1)
	go func() { statusDone <- runtime.status() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("status did not reach the flow snapshot")
	}

	mutationDone := make(chan struct{})
	go func() {
		runtime.mu.Lock()
		delete(runtime.streams, streamID)
		runtime.mu.Unlock()
		close(mutationDone)
	}()
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(release) })
		t.Fatal("flow snapshot held the runtime lock")
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case status := <-statusDone:
		if status.Streams != 1 || len(status.TCPStreams) != 1 || status.Carriers != 7 || status.TCPStreams[0].Carriers != 7 {
			t.Fatalf("snapshot status = %#v", status)
		}
	case <-time.After(time.Second):
		t.Fatal("status did not finish after releasing the flow snapshot")
	}
}

func TestTCPServerPruneRebuildsChronologicalTracking(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		closed:  make(map[tcpstream.StreamID]time.Time),
		traffic: make(map[string]*tcpServerTraffic),
	}
	now := time.Unix(1_700_000_000, 0)
	expiredFirst := tcpstream.StreamID{1}
	future := tcpstream.StreamID{2}
	expiredSecond := tcpstream.StreamID{3}
	runtime.closed[future] = now.Add(time.Minute)
	runtime.closed[expiredSecond] = now.Add(-time.Second)
	runtime.closed[expiredFirst] = now.Add(-time.Minute)

	oldestTraffic := &tcpServerTraffic{address: "192.0.2.1"}
	oldestTraffic.touch(now.Add(-tcpServerTrafficTTL - time.Minute))
	futureTraffic := &tcpServerTraffic{address: "192.0.2.2"}
	futureTraffic.touch(now)
	expiredTraffic := &tcpServerTraffic{address: "192.0.2.3"}
	expiredTraffic.touch(now.Add(-tcpServerTrafficTTL - time.Second))
	runtime.traffic[futureTraffic.address] = futureTraffic
	runtime.traffic[expiredTraffic.address] = expiredTraffic
	runtime.traffic[oldestTraffic.address] = oldestTraffic

	runtime.pruneState(now)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.closed) != 1 || !runtime.closed[future].Equal(now.Add(time.Minute)) {
		t.Fatalf("closed tombstones after prune = %#v, want only future entry", runtime.closed)
	}
	if runtime.closedOrder.Len() != 1 || runtime.closedOrder.Front().Value.(tcpstream.StreamID) != future {
		t.Fatalf("closed tracking was not rebuilt in expiration order")
	}
	if len(runtime.traffic) != 1 || runtime.traffic[futureTraffic.address] != futureTraffic {
		t.Fatalf("traffic after prune = %#v, want only future entry", runtime.traffic)
	}
	if runtime.inactiveTraffic.Len() != 1 || runtime.inactiveTraffic.Front().Value.(*tcpServerTraffic) != futureTraffic {
		t.Fatalf("traffic tracking was not rebuilt in last-used order")
	}
}
