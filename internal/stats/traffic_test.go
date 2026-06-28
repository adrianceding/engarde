package stats

import "testing"

func TestCountersRecordDrop(t *testing.T) {
	var counters Counters
	counters.RecordDrop(128)
	counters.RecordDrop(-1)

	snapshot := counters.Snapshot()
	if snapshot.DropPackets != 2 || snapshot.DropBytes != 128 {
		t.Fatalf("drop counters = %d packets/%d bytes, want 2/128", snapshot.DropPackets, snapshot.DropBytes)
	}
}
