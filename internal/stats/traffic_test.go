package stats

import (
	"sync"
	"testing"

	"github.com/adrianceding/engarde/internal/control"
)

func TestCountersRecordAndSnapshot(t *testing.T) {
	var counters Counters
	counters.RecordRX(64)
	counters.RecordRX(-1)
	counters.RecordTX(128)
	counters.RecordTX(-1)
	counters.RecordDrop(256)
	counters.RecordDrop(-1)
	counters.RecordSkip(512)
	counters.RecordSkip(-1)

	want := control.TrafficCounters{
		RXPackets:      2,
		RXBytes:        64,
		TXPackets:      2,
		TXBytes:        128,
		DropPackets:    2,
		DropBytes:      256,
		SkippedPackets: 2,
		SkippedBytes:   512,
	}
	if got := counters.Snapshot(); got != want {
		t.Fatalf("counter snapshot = %#v, want %#v", got, want)
	}
}

func TestCountersZeroValueSnapshot(t *testing.T) {
	var counters Counters
	if got := counters.Snapshot(); got != (control.TrafficCounters{}) {
		t.Fatalf("zero-value counter snapshot = %#v", got)
	}
}

func TestTrafficSnapshotSeparatesDataAndControl(t *testing.T) {
	traffic := &Traffic{}
	traffic.Data.RecordRX(1024)
	traffic.Data.RecordTX(2048)
	traffic.Control.RecordDrop(32)
	traffic.Control.RecordSkip(64)

	want := control.TrafficStats{
		Data: control.TrafficCounters{
			RXPackets: 1,
			RXBytes:   1024,
			TXPackets: 1,
			TXBytes:   2048,
		},
		Control: control.TrafficCounters{
			DropPackets:    1,
			DropBytes:      32,
			SkippedPackets: 1,
			SkippedBytes:   64,
		},
	}
	if got := traffic.Snapshot(); got != want {
		t.Fatalf("traffic snapshot = %#v, want %#v", got, want)
	}
}

func TestNilTrafficSnapshot(t *testing.T) {
	var traffic *Traffic
	if got := traffic.Snapshot(); got != (control.TrafficStats{}) {
		t.Fatalf("nil traffic snapshot = %#v", got)
	}
}

func TestCountersConcurrentRecordingAndSnapshot(t *testing.T) {
	const (
		writers    = 32
		iterations = 1000
	)
	var counters Counters
	start := make(chan struct{})
	stopReader := make(chan struct{})
	readerDone := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(writers)
	for range writers {
		go func() {
			defer writerWG.Done()
			<-start
			for range iterations {
				counters.RecordRX(1)
				counters.RecordTX(2)
				counters.RecordDrop(3)
				counters.RecordSkip(4)
			}
		}()
	}
	go func() {
		defer close(readerDone)
		<-start
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = counters.Snapshot()
			}
		}
	}()
	close(start)
	writerWG.Wait()
	close(stopReader)
	<-readerDone

	total := uint64(writers * iterations)
	want := control.TrafficCounters{
		RXPackets:      total,
		RXBytes:        total,
		TXPackets:      total,
		TXBytes:        total * 2,
		DropPackets:    total,
		DropBytes:      total * 3,
		SkippedPackets: total,
		SkippedBytes:   total * 4,
	}
	if got := counters.Snapshot(); got != want {
		t.Fatalf("concurrent counter snapshot = %#v, want %#v", got, want)
	}
}
