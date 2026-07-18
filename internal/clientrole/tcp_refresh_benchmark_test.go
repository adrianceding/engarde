package clientrole

import (
	"context"
	"fmt"
	"testing"

	"github.com/adrianceding/engarde/internal/tcpstream"
)

func BenchmarkTCPClientRefreshStablePaths(b *testing.B) {
	for _, groupCount := range []int{1, 32, 128, 512, 2048} {
		for _, pathCount := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("groups=%d/paths=%d", groupCount, pathCount), func(b *testing.B) {
				runtime, paths := benchmarkTCPClientRefreshRuntime(groupCount, pathCount)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if !runtime.refreshCarrierGroups(paths) {
						b.Fatal("stable refresh was rejected")
					}
				}
			})
		}
	}
}

func benchmarkTCPClientRefreshRuntime(groupCount, pathCount int) (*tcpClientRuntime, map[string]tcpClientPath) {
	paths := make(map[string]tcpClientPath, pathCount)
	for pathIndex := range pathCount {
		name := fmt.Sprintf("path-%d", pathIndex)
		paths[name] = tcpClientPath{
			index:       pathIndex + 1,
			address:     fmt.Sprintf("192.0.2.%d", pathIndex+1),
			destination: "198.51.100.1:59501",
		}
	}
	runtime := &tcpClientRuntime{
		ctx:      context.Background(),
		paths:    cloneTCPPaths(paths),
		sessions: make(map[string]*tcpPathSession, pathCount),
		groups:   make(map[*tcpCarrierGroup]struct{}, groupCount),
	}
	for interfaceName, path := range paths {
		runtime.sessions[interfaceName] = newTCPPathSession(runtime, interfaceName, path)
	}
	for range groupCount {
		group := newTCPCarrierGroup(runtime)
		for interfaceName, path := range paths {
			group.slots[interfaceName] = &tcpFlowSlot{
				path:    path,
				carrier: new(tcpstream.Carrier),
			}
		}
		runtime.groups[group] = struct{}{}
	}
	return runtime, paths
}
