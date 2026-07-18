package stats

import (
	"sync/atomic"

	"github.com/adrianceding/engarde/internal/control"
)

type Counters struct {
	rxPackets      atomic.Uint64
	rxBytes        atomic.Uint64
	txPackets      atomic.Uint64
	txBytes        atomic.Uint64
	dropPackets    atomic.Uint64
	dropBytes      atomic.Uint64
	skippedPackets atomic.Uint64
	skippedBytes   atomic.Uint64
}

func (counters *Counters) RecordRX(bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	counters.rxPackets.Add(1)
	counters.rxBytes.Add(uint64(bytes))
}

func (counters *Counters) RecordTX(bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	counters.txPackets.Add(1)
	counters.txBytes.Add(uint64(bytes))
}

func (counters *Counters) RecordDrop(bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	counters.dropPackets.Add(1)
	counters.dropBytes.Add(uint64(bytes))
}

func (counters *Counters) RecordSkip(bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	counters.skippedPackets.Add(1)
	counters.skippedBytes.Add(uint64(bytes))
}

func (counters *Counters) Snapshot() control.TrafficCounters {
	return control.TrafficCounters{
		RXPackets:      counters.rxPackets.Load(),
		RXBytes:        counters.rxBytes.Load(),
		TXPackets:      counters.txPackets.Load(),
		TXBytes:        counters.txBytes.Load(),
		DropPackets:    counters.dropPackets.Load(),
		DropBytes:      counters.dropBytes.Load(),
		SkippedPackets: counters.skippedPackets.Load(),
		SkippedBytes:   counters.skippedBytes.Load(),
	}
}

type Traffic struct {
	Data    Counters
	Control Counters
}

func (traffic *Traffic) Snapshot() control.TrafficStats {
	if traffic == nil {
		return control.TrafficStats{}
	}
	return control.TrafficStats{
		Data:    traffic.Data.Snapshot(),
		Control: traffic.Control.Snapshot(),
	}
}
