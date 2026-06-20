package transport

import (
	"sort"
	"time"
)

type PendingRing struct {
	slots []pendingSlot
}

type pendingSlot struct {
	active        bool
	id            PacketID
	pathID        string
	pathIDs       []string
	sentAt        int64
	tries         int
	timeoutMillis int64
	payload       []byte
}

type PendingRecord struct {
	ID            PacketID
	PathID        string
	PathIDs       []string
	SentAt        int64
	Tries         int
	TimeoutMillis int64
	Payload       []byte
}

func NewPendingRing(capacity int) *PendingRing {
	if capacity < 1 {
		capacity = 1
	}
	return &PendingRing{slots: make([]pendingSlot, capacity)}
}

func (ring *PendingRing) Put(record PendingRecord) {
	slot := &ring.slots[record.ID.Sequence%uint64(len(ring.slots))]
	pathIDs := normalizePathIDs(record.PathID, record.PathIDs)
	*slot = pendingSlot{active: true, id: record.ID, pathID: record.PathID, pathIDs: pathIDs, sentAt: record.SentAt, tries: record.Tries, timeoutMillis: record.TimeoutMillis, payload: append([]byte(nil), record.Payload...)}
}

func (ring *PendingRing) Complete(id PacketID) (PendingRecord, bool) {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return PendingRecord{}, false
	}
	record := slot.record()
	slot.active = false
	slot.payload = nil
	slot.pathIDs = nil
	return record, true
}

func (ring *PendingRing) Get(id PacketID) (PendingRecord, bool) {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return PendingRecord{}, false
	}
	return slot.record(), true
}

func (ring *PendingRing) UpdatePaths(id PacketID, pathIDs []string) bool {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return false
	}
	slot.pathIDs = normalizePathIDs(slot.pathID, pathIDs)
	return true
}

func (ring *PendingRing) Due(now int64, minTimeoutMillis int64, maxTimeoutMillis int64, maxRetries int) []RetryRecord {
	if minTimeoutMillis < 0 {
		return nil
	}
	if minTimeoutMillis < 1 {
		minTimeoutMillis = 1
	}
	if maxTimeoutMillis < minTimeoutMillis {
		maxTimeoutMillis = minTimeoutMillis
	}
	due := make([]RetryRecord, 0)
	for i := range ring.slots {
		slot := &ring.slots[i]
		timeoutMillis := clampTimeout(slot.timeoutMillis, minTimeoutMillis, maxTimeoutMillis)
		if !slot.active || now-slot.sentAt < timeoutMillis {
			continue
		}
		if slot.tries >= maxRetries {
			slot.active = false
			slot.payload = nil
			slot.pathIDs = nil
			continue
		}
		slot.tries++
		slot.sentAt = now
		slot.timeoutMillis = clampTimeout(timeoutMillis*2, minTimeoutMillis, maxTimeoutMillis)
		due = append(due, RetryRecord{PendingRecord: slot.record()})
	}
	return due
}

func (slot pendingSlot) record() PendingRecord {
	return PendingRecord{ID: slot.id, PathID: slot.pathID, PathIDs: append([]string(nil), slot.pathIDs...), SentAt: slot.sentAt, Tries: slot.tries, TimeoutMillis: slot.timeoutMillis, Payload: append([]byte(nil), slot.payload...)}
}

func normalizePathIDs(primary string, pathIDs []string) []string {
	seen := make(map[string]struct{}, len(pathIDs)+1)
	normalized := make([]string, 0, len(pathIDs)+1)
	if primary != "" {
		seen[primary] = struct{}{}
		normalized = append(normalized, primary)
	}
	for _, pathID := range pathIDs {
		if pathID == "" {
			continue
		}
		if _, ok := seen[pathID]; ok {
			continue
		}
		seen[pathID] = struct{}{}
		normalized = append(normalized, pathID)
	}
	return normalized
}

func clampTimeout(timeoutMillis int64, minTimeoutMillis int64, maxTimeoutMillis int64) int64 {
	if timeoutMillis < minTimeoutMillis {
		return minTimeoutMillis
	}
	if timeoutMillis > maxTimeoutMillis {
		return maxTimeoutMillis
	}
	return timeoutMillis
}

type DuplicateWindow struct {
	slots []duplicateSlot
}

type duplicateSlot struct {
	seen bool
	id   PacketID
}

func NewDuplicateWindow(capacity int) *DuplicateWindow {
	if capacity < 1 {
		capacity = 1
	}
	return &DuplicateWindow{slots: make([]duplicateSlot, capacity)}
}

func (window *DuplicateWindow) SeenOrRecord(id PacketID) bool {
	slot := &window.slots[id.Sequence%uint64(len(window.slots))]
	if slot.seen && slot.id == id {
		return true
	}
	*slot = duplicateSlot{seen: true, id: id}
	return false
}

type PathStats struct {
	ID          string
	LastSeen    int64
	LastSuccess int64
	SmoothedRTT int64
	RTTVariance int64
	Failures    int
}

const (
	PathSwitchRTTMarginMillis  int64 = 25
	PathSwitchRTTMarginPercent int64 = 25
)

func (stats *PathStats) MarkSuccess(now int64, rtt int64) {
	stats.LastSeen = now
	stats.LastSuccess = now
	stats.Failures = 0
	if rtt < 0 {
		rtt = 0
	}
	if stats.SmoothedRTT == 0 {
		stats.SmoothedRTT = rtt
		stats.RTTVariance = rtt / 2
		return
	}
	delta := stats.SmoothedRTT - rtt
	if delta < 0 {
		delta = -delta
	}
	stats.RTTVariance = (stats.RTTVariance*3 + delta) / 4
	stats.SmoothedRTT = (stats.SmoothedRTT*7 + rtt) / 8
}

func (stats *PathStats) MarkSeen(now int64) {
	stats.LastSeen = now
}

func (stats *PathStats) MarkFailure(now int64) {
	stats.LastSeen = now
	stats.Failures++
}

func (stats PathStats) Eligible(now int64, timeout time.Duration) bool {
	if stats.LastSuccess == 0 {
		return false
	}
	return now-stats.LastSuccess <= timeout.Milliseconds()
}

func SelectPrimaryPath(current string, candidates []string, pathStats map[string]PathStats, now int64, timeout time.Duration) string {
	eligible := make([]string, 0, len(candidates))
	for _, id := range candidates {
		stats, ok := pathStats[id]
		if ok && stats.Eligible(now, timeout) {
			eligible = append(eligible, id)
		}
	}
	if len(eligible) == 0 {
		return ""
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		return pathStatsLess(eligible[i], eligible[j], pathStats)
	})
	best := eligible[0]
	if current == "" || current == best || !containsPathID(eligible, current) {
		return best
	}
	currentStats, currentOK := pathStats[current]
	bestStats, bestOK := pathStats[best]
	if !currentOK || !bestOK || !currentStats.Eligible(now, timeout) {
		return best
	}
	if shouldSwitchPrimary(currentStats, bestStats) {
		return best
	}
	return current
}

func pathStatsLess(leftID string, rightID string, pathStats map[string]PathStats) bool {
	left := pathStats[leftID]
	right := pathStats[rightID]
	if left.Failures != right.Failures {
		return left.Failures < right.Failures
	}
	if left.SmoothedRTT != right.SmoothedRTT {
		return left.SmoothedRTT < right.SmoothedRTT
	}
	return leftID < rightID
}

func shouldSwitchPrimary(current PathStats, candidate PathStats) bool {
	if candidate.Failures != current.Failures {
		return candidate.Failures < current.Failures
	}
	return rttSignificantlyBetter(candidate.SmoothedRTT, current.SmoothedRTT)
}

func rttSignificantlyBetter(candidateRTT int64, currentRTT int64) bool {
	if currentRTT <= 0 || candidateRTT >= currentRTT {
		return false
	}
	margin := currentRTT * PathSwitchRTTMarginPercent / 100
	if margin < PathSwitchRTTMarginMillis {
		margin = PathSwitchRTTMarginMillis
	}
	return currentRTT-candidateRTT >= margin
}

func containsPathID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func (stats PathStats) RTO(minTimeoutMillis int64, maxTimeoutMillis int64) int64 {
	if minTimeoutMillis < 1 {
		minTimeoutMillis = 1
	}
	if maxTimeoutMillis < minTimeoutMillis {
		maxTimeoutMillis = minTimeoutMillis
	}
	if stats.SmoothedRTT <= 0 {
		return minTimeoutMillis
	}
	variance := stats.RTTVariance
	if variance < 1 {
		variance = 1
	}
	timeoutMillis := stats.SmoothedRTT + variance*4
	if timeoutMillis < minTimeoutMillis {
		timeoutMillis = minTimeoutMillis
	}
	for i := 0; i < stats.Failures && i < 4; i++ {
		timeoutMillis *= 2
		if timeoutMillis >= maxTimeoutMillis {
			return maxTimeoutMillis
		}
	}
	return clampTimeout(timeoutMillis, minTimeoutMillis, maxTimeoutMillis)
}
