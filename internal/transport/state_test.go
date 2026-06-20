package transport

import (
	"testing"
	"time"
)

func TestPendingRingCompletesExactPacketID(t *testing.T) {
	ring := NewPendingRing(2)
	first := PacketID{Session: 1, Sequence: 1}
	second := PacketID{Session: 1, Sequence: 3}
	ring.Put(PendingRecord{ID: first, PathID: "eth0", SentAt: 10, Tries: 1, Payload: []byte("first")})
	ring.Put(PendingRecord{ID: second, PathID: "eth1", SentAt: 20, Tries: 2, Payload: []byte("second")})

	if _, ok := ring.Complete(first); ok {
		t.Fatal("overwritten packet id completed")
	}
	record, ok := ring.Complete(second)
	if !ok {
		t.Fatal("current packet id did not complete")
	}
	if record.PathID != "eth1" || len(record.PathIDs) != 1 || record.PathIDs[0] != "eth1" || record.SentAt != 20 || record.Tries != 2 || string(record.Payload) != "second" {
		t.Fatalf("record = %#v", record)
	}
	if _, ok := ring.Complete(second); ok {
		t.Fatal("completed packet id completed twice")
	}
}

func TestPendingRingUpdatePaths(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", Payload: []byte("first")})
	if !ring.UpdatePaths(id, []string{"eth1", "eth2", "eth1"}) {
		t.Fatal("UpdatePaths returned false")
	}
	record, ok := ring.Get(id)
	if !ok {
		t.Fatal("record missing")
	}
	want := []string{"eth0", "eth1", "eth2"}
	if len(record.PathIDs) != len(want) {
		t.Fatalf("PathIDs = %#v", record.PathIDs)
	}
	for i := range want {
		if record.PathIDs[i] != want[i] {
			t.Fatalf("PathIDs = %#v, want %#v", record.PathIDs, want)
		}
	}
}

func TestPendingRingDueRetriesAndDrops(t *testing.T) {
	ring := NewPendingRing(2)
	first := PacketID{Session: 1, Sequence: 1}
	second := PacketID{Session: 1, Sequence: 2}
	ring.Put(PendingRecord{ID: first, PathID: "eth0", SentAt: 100, Tries: 0, Payload: []byte("first")})
	ring.Put(PendingRecord{ID: second, PathID: "eth1", SentAt: 100, Tries: 1, Payload: []byte("second")})

	due := ring.Due(250, 100, 1000, 1)
	if len(due) != 1 || due[0].ID != first || string(due[0].Payload) != "first" || due[0].Tries != 1 {
		t.Fatalf("due = %#v", due)
	}
	if _, ok := ring.Get(second); ok {
		t.Fatal("record at max retries was not dropped")
	}
	due = ring.Due(460, 100, 1000, 1)
	if len(due) != 0 {
		t.Fatalf("due after max retry = %#v", due)
	}
	if _, ok := ring.Get(first); ok {
		t.Fatal("record after max retry was not dropped")
	}
}

func TestPendingRingDueUsesRecordTimeoutAndBackoff(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", SentAt: 100, TimeoutMillis: 300, Payload: []byte("first")})

	if due := ring.Due(350, 100, 1000, 2); len(due) != 0 {
		t.Fatalf("early due = %#v", due)
	}
	due := ring.Due(410, 100, 1000, 2)
	if len(due) != 1 || due[0].TimeoutMillis != 600 || due[0].Tries != 1 {
		t.Fatalf("due = %#v", due)
	}
	if due := ring.Due(900, 100, 1000, 2); len(due) != 0 {
		t.Fatalf("early second retry = %#v", due)
	}
	due = ring.Due(1010, 100, 1000, 2)
	if len(due) != 1 || due[0].TimeoutMillis != 1000 || due[0].Tries != 2 {
		t.Fatalf("second due = %#v", due)
	}
}

func TestDuplicateWindowSuppressesRecentDuplicates(t *testing.T) {
	window := NewDuplicateWindow(2)
	first := PacketID{Session: 1, Sequence: 1}
	second := PacketID{Session: 1, Sequence: 3}
	if window.SeenOrRecord(first) {
		t.Fatal("first packet marked duplicate")
	}
	if !window.SeenOrRecord(first) {
		t.Fatal("repeat packet was not duplicate")
	}
	if window.SeenOrRecord(second) {
		t.Fatal("different packet in same slot marked duplicate")
	}
	if window.SeenOrRecord(first) {
		t.Fatal("overwritten old packet marked duplicate")
	}
}

func TestPathStatsSuccessFailureAndEligibility(t *testing.T) {
	var stats PathStats
	if stats.Eligible(100, time.Second) {
		t.Fatal("path without success should not be eligible")
	}
	stats.MarkSuccess(1000, 80)
	if !stats.Eligible(1200, time.Second) {
		t.Fatal("recent success should be eligible")
	}
	if stats.SmoothedRTT != 80 || stats.RTTVariance != 40 || stats.Failures != 0 {
		t.Fatalf("stats after success = %#v", stats)
	}
	stats.MarkSuccess(1300, 160)
	if stats.SmoothedRTT != 90 {
		t.Fatalf("smoothed rtt = %d, want 90", stats.SmoothedRTT)
	}
	if stats.RTO(50, 1000) != 290 {
		t.Fatalf("rto = %d, want 290", stats.RTO(50, 1000))
	}
	stats.MarkFailure(1400)
	if stats.Failures != 1 || stats.LastSeen != 1400 {
		t.Fatalf("stats after failure = %#v", stats)
	}
	if stats.RTO(50, 1000) != 580 {
		t.Fatalf("failure rto = %d, want 580", stats.RTO(50, 1000))
	}
	if stats.Eligible(3000, time.Second) {
		t.Fatal("stale success should not be eligible")
	}
}

func TestSelectPrimaryPathKeepsCurrentForSmallRTTChanges(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 100},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 90},
	}

	primary := SelectPrimaryPath("eth0", []string{"eth0", "eth1"}, stats, now, time.Second)

	if primary != "eth0" {
		t.Fatalf("primary = %q, want current path eth0", primary)
	}
}

func TestSelectPrimaryPathSwitchesForSignificantRTTChanges(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 100},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 70},
	}

	primary := SelectPrimaryPath("eth0", []string{"eth0", "eth1"}, stats, now, time.Second)

	if primary != "eth1" {
		t.Fatalf("primary = %q, want faster path eth1", primary)
	}
}

func TestSelectPrimaryPathSwitchesWhenCurrentIsNotEligible(t *testing.T) {
	now := int64(3000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: 1000, SmoothedRTT: 10},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 100},
	}

	primary := SelectPrimaryPath("eth0", []string{"eth0", "eth1"}, stats, now, time.Second)

	if primary != "eth1" {
		t.Fatalf("primary = %q, want eligible path eth1", primary)
	}
}

func TestSelectPrimaryPathSwitchesForFewerFailures(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 10, Failures: 1},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 100, Failures: 0},
	}

	primary := SelectPrimaryPath("eth0", []string{"eth0", "eth1"}, stats, now, time.Second)

	if primary != "eth1" {
		t.Fatalf("primary = %q, want lower-failure path eth1", primary)
	}
}
