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

func TestPendingRingDrop(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", Payload: []byte("first")})
	if !ring.Drop(id) {
		t.Fatal("Drop returned false")
	}
	if _, ok := ring.Get(id); ok {
		t.Fatal("dropped packet was still pending")
	}
	if ring.Drop(id) {
		t.Fatal("dropped packet dropped twice")
	}
}

func TestPendingRingRecordAttemptTracksLatestAndAllPaths(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", PathIDs: []string{"eth0"}, AttemptPathIDs: []string{"eth0"}, FallbackPathIDs: []string{"eth1", "eth2"}, Payload: []byte("first")})
	if !ring.RecordAttempt(id, []string{"eth1"}) {
		t.Fatal("RecordAttempt returned false")
	}
	record, ok := ring.Get(id)
	if !ok {
		t.Fatal("record missing")
	}
	if got, want := record.PathIDs, []string{"eth0", "eth1"}; !sameStrings(got, want) {
		t.Fatalf("PathIDs = %#v, want %#v", got, want)
	}
	if got, want := record.AttemptPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("AttemptPathIDs = %#v, want %#v", got, want)
	}
	if got, want := record.FallbackPathIDs, []string{"eth1", "eth2"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs = %#v, want %#v", got, want)
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

func TestPendingRingDuePreservesOriginalSentAtForRetryRTT(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", SentAt: 100, TimeoutMillis: 50, Payload: []byte("first")})

	due := ring.Due(200, 50, 1000, 2)
	if len(due) != 1 {
		t.Fatalf("due = %#v, want one retry", due)
	}
	record, ok := ring.Get(id)
	if !ok {
		t.Fatal("record missing after retry")
	}
	if record.SentAt != 100 {
		t.Fatalf("SentAt after retry = %d, want original 100", record.SentAt)
	}
}

func TestPendingRingRecordsInitialSentAtForAllAttemptPaths(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", PathIDs: []string{"eth0", "eth1"}, AttemptPathIDs: []string{"eth0", "eth1"}, SentAt: 100, Payload: []byte("first")})

	record, ok := ring.Get(id)
	if !ok {
		t.Fatal("record missing")
	}
	if got := record.SentAtForPath("eth0"); got != 100 {
		t.Fatalf("eth0 sentAt = %d, want 100", got)
	}
	if got := record.SentAtForPath("eth1"); got != 100 {
		t.Fatalf("eth1 sentAt = %d, want 100", got)
	}
}

func TestPendingRingRecordsSentAtPerAttemptPath(t *testing.T) {
	ring := NewPendingRing(2)
	id := PacketID{Session: 1, Sequence: 1}
	ring.Put(PendingRecord{ID: id, PathID: "eth0", SentAt: 100, TimeoutMillis: 50, Payload: []byte("first")})

	if due := ring.Due(200, 50, 1000, 2); len(due) != 1 {
		t.Fatalf("due = %#v, want one retry", due)
	}
	if !ring.RecordAttemptAt(id, []string{"eth1"}, 210) {
		t.Fatal("RecordAttemptAt returned false")
	}
	record, ok := ring.Get(id)
	if !ok {
		t.Fatal("record missing after retry")
	}
	if got := record.SentAtForPath("eth0"); got != 100 {
		t.Fatalf("eth0 sentAt = %d, want 100", got)
	}
	if got := record.SentAtForPath("eth1"); got != 210 {
		t.Fatalf("eth1 sentAt = %d, want 210", got)
	}
	if record.SentAt != 100 || record.LastSentAt != 210 {
		t.Fatalf("record timing = %#v, want original 100 and last 210", record)
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

func TestPathStatsTimeoutScoreUsesFixedWindowDecay(t *testing.T) {
	stats := PathStats{TimeoutScore: 800, TimeoutScoreUpdatedAt: 1000}
	if got, want := timeoutScoreAt(stats, 16000), int64(400); got != want {
		t.Fatalf("timeout score halfway through window = %d, want %d", got, want)
	}
	if got := timeoutScoreAt(stats, 31000); got != 0 {
		t.Fatalf("timeout score after window = %d, want 0", got)
	}
}

func TestSelectPathSelectionKeepsCurrentFirstForSmallScoreChanges(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 100},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 90},
	}

	selection := SelectPathSelection(PathSelection{FirstPathIDs: []string{"eth0"}}, []string{"eth0", "eth1"}, stats, now, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth0"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
	if got, want := selection.FallbackPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionExpandsWithoutSwitchingSimilarCurrentFirst(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 100, RTTVariance: 10, TimeoutScore: 500},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 95, RTTVariance: 10},
	}

	selection := SelectPathSelection(PathSelection{FirstPathIDs: []string{"eth0"}}, []string{"eth0", "eth1"}, stats, now, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth0", "eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionSwitchesForSignificantScoreChanges(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 100},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 70},
	}

	selection := SelectPathSelection(PathSelection{FirstPathIDs: []string{"eth0"}}, []string{"eth0", "eth1"}, stats, now, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionDropsIneligibleCurrentFirst(t *testing.T) {
	now := int64(3000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: 1000, SmoothedRTT: 10},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 100},
	}

	selection := SelectPathSelection(PathSelection{FirstPathIDs: []string{"eth0"}}, []string{"eth0", "eth1"}, stats, now, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
	if got, want := selection.FallbackPathIDs, []string{"eth0"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionKeepsTimedOutPathAsFallback(t *testing.T) {
	now := int64(3000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSeen: now, LastSuccess: 1000, SmoothedRTT: 10, TimeoutScore: 500},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 100},
	}

	selection := SelectPathSelection(PathSelection{FirstPathIDs: []string{"eth0"}}, []string{"eth0", "eth1"}, stats, now, 100*time.Millisecond)

	if got, want := selection.FirstPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
	if got, want := selection.FallbackPathIDs, []string{"eth0"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionShrinksFirstPathsSlowly(t *testing.T) {
	stats := map[string]PathStats{}
	current := PathSelection{FirstPathIDs: []string{"eth0", "eth1"}, FirstPathCountChangedAt: 1000}
	stats["eth0"] = PathStats{ID: "eth0", LastSuccess: 1500, SmoothedRTT: 80}
	stats["eth1"] = PathStats{ID: "eth1", LastSuccess: 1500, SmoothedRTT: 90}

	selection := SelectPathSelection(current, []string{"eth0", "eth1"}, stats, 1500, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth0", "eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs before hold = %#v, want %#v", got, want)
	}
	stats["eth0"] = PathStats{ID: "eth0", LastSuccess: 10500, SmoothedRTT: 80}
	stats["eth1"] = PathStats{ID: "eth1", LastSuccess: 10500, SmoothedRTT: 90}

	selection = SelectPathSelection(current, []string{"eth0", "eth1"}, stats, 10500, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth0", "eth1"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs before fixed hold = %#v, want %#v", got, want)
	}
	stats["eth0"] = PathStats{ID: "eth0", LastSuccess: 11100, SmoothedRTT: 80}
	stats["eth1"] = PathStats{ID: "eth1", LastSuccess: 11100, SmoothedRTT: 90}

	selection = SelectPathSelection(current, []string{"eth0", "eth1"}, stats, 11100, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth0"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs after hold = %#v, want %#v", got, want)
	}
	if got, want := selection.FallbackPathIDs, []string{"eth1"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs after hold = %#v, want %#v", got, want)
	}
}

func TestSelectPathSelectionExpandsFirstPathsForRisk(t *testing.T) {
	now := int64(1000)
	stats := map[string]PathStats{
		"eth0": {ID: "eth0", LastSuccess: now, SmoothedRTT: 80, TimeoutScore: 500},
		"eth1": {ID: "eth1", LastSuccess: now, SmoothedRTT: 90, TimeoutScore: 300},
		"eth2": {ID: "eth2", LastSuccess: now, SmoothedRTT: 100, TimeoutScore: 100},
		"eth3": {ID: "eth3", LastSuccess: now, SmoothedRTT: 110, TimeoutScore: 100},
	}

	selection := SelectPathSelection(PathSelection{}, []string{"eth0", "eth1", "eth2", "eth3"}, stats, now, time.Second)

	if got, want := selection.FirstPathIDs, []string{"eth2", "eth3"}; !sameStrings(got, want) {
		t.Fatalf("FirstPathIDs = %#v, want %#v", got, want)
	}
	if got, want := selection.FallbackPathIDs, []string{"eth1", "eth0"}; !sameStrings(got, want) {
		t.Fatalf("FallbackPathIDs = %#v, want %#v", got, want)
	}
}

func TestPathSelectionRole(t *testing.T) {
	selection := PathSelection{FirstPathIDs: []string{"eth0", "eth1"}, FallbackPathIDs: []string{"eth2"}}
	if got := selection.Role("eth0"); got != PathRoleFirst {
		t.Fatalf("eth0 role = %q, want first", got)
	}
	if got := selection.Role("eth2"); got != PathRoleFallback {
		t.Fatalf("eth2 role = %q, want fallback", got)
	}
	if got := selection.Role("eth3"); got != "" {
		t.Fatalf("eth3 role = %q, want empty", got)
	}
}

func sameStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
