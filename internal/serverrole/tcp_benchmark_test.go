package serverrole

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

func BenchmarkTCPServerStatus(b *testing.B) {
	for _, entries := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("entries-%d", entries), func(b *testing.B) {
			runtime := benchmarkTCPServerRuntime(entries)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				status := runtime.status()
				if len(status.TCPStreams) != entries || len(status.Sockets) != entries {
					b.Fatalf("status entries = %d streams/%d sockets, want %d", len(status.TCPStreams), len(status.Sockets), entries)
				}
			}
		})
	}
}

func BenchmarkTCPServerPruneSteadyState(b *testing.B) {
	const entries = tcpServerClosedCacheSafetyLimit
	runtime := benchmarkTCPServerPruneRuntime(entries)
	now := time.Unix(1_700_000_000, 0).Add(tcpServerClosedTTL / 2)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		runtime.pruneState(now)
	}
}

func BenchmarkTCPServerCarrierObserverDataFrame(b *testing.B) {
	traffic := &tcpServerTraffic{active: 1}
	observer := tcpCarrierObserver(traffic)
	frame := tcpstream.Frame{Type: tcpstream.FrameData, Payload: make([]byte, 16*1024)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		observer.Read(frame)
	}
}

func benchmarkTCPServerRuntime(entries int) *tcpServerRuntime {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = entries * 2
	runtime := &tcpServerRuntime{
		server:          New(config.Server{ListenAddr: "127.0.0.1:59501", Transfer: transfer}, "benchmark", nil),
		streams:         make(map[tcpstream.StreamID]*tcpServerStream, entries),
		closed:          make(map[tcpstream.StreamID]time.Time, entries),
		closedOrder:     list.New(),
		closedItems:     make(map[tcpstream.StreamID]*list.Element, entries),
		sessions:        make(map[*tcpstream.Session]struct{}),
		traffic:         make(map[string]*tcpServerTraffic, entries),
		inactiveTraffic: list.New(),
	}
	now := time.Now()
	flowConfig := tcpstream.FlowConfig{ChunkSize: 1, CarrierQueueBytes: 1, ReorderWindowBytes: 1}
	for index := range entries {
		var streamID tcpstream.StreamID
		binary.BigEndian.PutUint64(streamID[:8], uint64(index+1))
		runtime.streams[streamID] = &tcpServerStream{
			version:     tcpstream.Version,
			destination: "example.com:443",
			flow:        tcpstream.NewFlow(streamID, nil, tcpstream.DirectionServerToClient, flowConfig),
		}
		runtime.rememberClosedLocked(streamID, now)
		address := fmt.Sprintf("192.0.%d.%d", index>>8, index&255)
		traffic := &tcpServerTraffic{address: address, active: 1}
		traffic.touch(now)
		traffic.Data.RecordRX(16 * 1024)
		runtime.traffic[address] = traffic
	}
	return runtime
}

func benchmarkTCPServerPruneRuntime(entries int) *tcpServerRuntime {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:          New(config.Server{Transfer: transfer}, "benchmark", nil),
		closed:          make(map[tcpstream.StreamID]time.Time, entries),
		closedOrder:     list.New(),
		closedItems:     make(map[tcpstream.StreamID]*list.Element, entries),
		traffic:         make(map[string]*tcpServerTraffic, entries),
		inactiveTraffic: list.New(),
	}
	base := time.Unix(1_700_000_000, 0)
	for index := range entries {
		var streamID tcpstream.StreamID
		binary.BigEndian.PutUint64(streamID[:8], uint64(index+1))
		runtime.rememberClosedLocked(streamID, base)
		address := fmt.Sprintf("192.0.%d.%d", index>>8, index&255)
		traffic := &tcpServerTraffic{address: address}
		traffic.touch(base)
		runtime.traffic[address] = traffic
		traffic.inactiveElement = runtime.inactiveTraffic.PushBack(traffic)
	}
	return runtime
}
