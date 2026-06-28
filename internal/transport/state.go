package transport

import (
	"sort"
	"time"
)

type PendingRing struct {
	slots []pendingSlot
}

type pendingSlot struct {
	active          bool
	id              PacketID
	pathID          string
	pathIDs         []string
	attemptPathIDs  []string
	fallbackPathIDs []string
	sentAt          int64
	lastSentAt      int64
	sentAtByPath    map[string]int64
	tries           int
	timeoutMillis   int64
	payload         []byte
}

type PendingRecord struct {
	ID              PacketID
	PathID          string
	PathIDs         []string
	AttemptPathIDs  []string
	FallbackPathIDs []string
	SentAt          int64
	LastSentAt      int64
	SentAtByPath    map[string]int64
	Tries           int
	TimeoutMillis   int64
	Payload         []byte
}

func (record PendingRecord) SentAtForPath(pathID string) int64 {
	if record.SentAtByPath != nil {
		if sentAt, ok := record.SentAtByPath[pathID]; ok {
			return sentAt
		}
	}
	if containsPathID(record.AttemptPathIDs, pathID) && record.LastSentAt > 0 {
		return record.LastSentAt
	}
	return record.SentAt
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
	attemptPathIDs := normalizePathIDs("", record.AttemptPathIDs)
	if len(attemptPathIDs) == 0 {
		attemptPathIDs = append([]string(nil), pathIDs...)
	}
	lastSentAt := record.LastSentAt
	if lastSentAt == 0 {
		lastSentAt = record.SentAt
	}
	sentAtByPath := cloneSentAtByPath(record.SentAtByPath)
	if sentAtByPath == nil && len(attemptPathIDs) > 0 {
		sentAtByPath = make(map[string]int64, len(attemptPathIDs))
	}
	for _, pathID := range attemptPathIDs {
		if _, ok := sentAtByPath[pathID]; !ok {
			sentAtByPath[pathID] = lastSentAt
		}
	}
	*slot = pendingSlot{active: true, id: record.ID, pathID: record.PathID, pathIDs: pathIDs, attemptPathIDs: attemptPathIDs, fallbackPathIDs: normalizePathIDs("", record.FallbackPathIDs), sentAt: record.SentAt, lastSentAt: lastSentAt, sentAtByPath: sentAtByPath, tries: record.Tries, timeoutMillis: record.TimeoutMillis, payload: append([]byte(nil), record.Payload...)}
}

func (ring *PendingRing) Complete(id PacketID) (PendingRecord, bool) {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return PendingRecord{}, false
	}
	record := slot.record()
	slot.clear()
	return record, true
}

func (ring *PendingRing) Drop(id PacketID) bool {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return false
	}
	slot.clear()
	return true
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
	slot.attemptPathIDs = normalizePathIDs("", pathIDs)
	return true
}

func (ring *PendingRing) RecordAttempt(id PacketID, pathIDs []string) bool {
	return ring.RecordAttemptAt(id, pathIDs, 0)
}

func (ring *PendingRing) RecordAttemptAt(id PacketID, pathIDs []string, sentAt int64) bool {
	slot := &ring.slots[id.Sequence%uint64(len(ring.slots))]
	if !slot.active || slot.id != id {
		return false
	}
	attemptPathIDs := normalizePathIDs("", pathIDs)
	slot.pathIDs = mergePathIDs(slot.pathIDs, attemptPathIDs)
	slot.attemptPathIDs = attemptPathIDs
	if sentAt == 0 {
		sentAt = slot.lastSentAt
	}
	if sentAt == 0 {
		sentAt = slot.sentAt
	}
	slot.lastSentAt = sentAt
	if slot.sentAtByPath == nil && len(attemptPathIDs) > 0 {
		slot.sentAtByPath = make(map[string]int64, len(attemptPathIDs))
	}
	for _, pathID := range attemptPathIDs {
		slot.sentAtByPath[pathID] = sentAt
	}
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
		lastSentAt := slot.lastSentAt
		if lastSentAt == 0 {
			lastSentAt = slot.sentAt
		}
		if !slot.active || now-lastSentAt < timeoutMillis {
			continue
		}
		if slot.tries >= maxRetries {
			slot.clear()
			continue
		}
		slot.tries++
		slot.lastSentAt = now
		slot.timeoutMillis = clampTimeout(timeoutMillis*2, minTimeoutMillis, maxTimeoutMillis)
		due = append(due, RetryRecord{PendingRecord: slot.record()})
	}
	return due
}

func (slot *pendingSlot) clear() {
	slot.active = false
	slot.payload = nil
	slot.pathIDs = nil
	slot.attemptPathIDs = nil
	slot.fallbackPathIDs = nil
	slot.sentAtByPath = nil
}

func (slot pendingSlot) record() PendingRecord {
	return PendingRecord{ID: slot.id, PathID: slot.pathID, PathIDs: append([]string(nil), slot.pathIDs...), AttemptPathIDs: append([]string(nil), slot.attemptPathIDs...), FallbackPathIDs: append([]string(nil), slot.fallbackPathIDs...), SentAt: slot.sentAt, LastSentAt: slot.lastSentAt, SentAtByPath: cloneSentAtByPath(slot.sentAtByPath), Tries: slot.tries, TimeoutMillis: slot.timeoutMillis, Payload: append([]byte(nil), slot.payload...)}
}

func cloneSentAtByPath(values map[string]int64) map[string]int64 {
	if len(values) == 0 {
		return nil
	}
	clone := make(map[string]int64, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
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

func mergePathIDs(first []string, second []string) []string {
	merged := make([]string, 0, len(first)+len(second))
	seen := make(map[string]struct{}, len(first)+len(second))
	for _, id := range first {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		merged = append(merged, id)
	}
	for _, id := range second {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		merged = append(merged, id)
	}
	return merged
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
	ID                    string
	LastSeen              int64
	LastSuccess           int64
	SmoothedRTT           int64
	RTTVariance           int64
	Failures              int
	TimeoutScore          int64
	TimeoutScoreUpdatedAt int64
}

const (
	PathSwitchRTTMarginMillis             int64 = 25
	PathSwitchRTTMarginPercent            int64 = 25
	MaxFirstPathCount                           = 3
	PathSelectionTargetRisk               int64 = 50
	PathSelectionBaseRisk                 int64 = 10
	PathSelectionMaxRisk                  int64 = 900
	PathSelectionFailureRisk              int64 = 250
	PathSelectionJitterThreshold                = 75
	PathSelectionStaleThreshold                 = 50
	PathSelectionRiskScale                int64 = 1000
	PathSelectionFallbackWindowMillis     int64 = 15000
	PathSelectionFirstCountHoldMillis     int64 = 10000
	PathSelectionTimeoutScoreWindowMillis int64 = 30000
)

const (
	PathRoleFirst    = "first"
	PathRoleFallback = "fallback"
)

func (stats *PathStats) MarkSuccess(now int64, rtt int64) {
	stats.LastSeen = now
	stats.LastSuccess = now
	stats.Failures = 0
	if rtt < 0 {
		rtt = 0
	}
	stats.TimeoutScore = timeoutScoreAt(*stats, now) * 7 / 8
	stats.TimeoutScoreUpdatedAt = now
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
	stats.TimeoutScore = (timeoutScoreAt(*stats, now)*7 + PathSelectionRiskScale) / 8
	if stats.TimeoutScore > PathSelectionRiskScale {
		stats.TimeoutScore = PathSelectionRiskScale
	}
	stats.TimeoutScoreUpdatedAt = now
}

func (stats PathStats) Eligible(now int64, timeout time.Duration) bool {
	if stats.LastSuccess == 0 {
		return false
	}
	return now-stats.LastSuccess <= timeout.Milliseconds()
}

func (stats PathStats) FallbackEligible(now int64, timeout time.Duration) bool {
	if stats.LastSuccess == 0 {
		return false
	}
	if stats.Eligible(now, timeout) {
		return true
	}
	windowMillis := fallbackWindowMillis()
	return now-stats.LastSuccess <= windowMillis || stats.LastSeen > 0 && now-stats.LastSeen <= windowMillis
}

type PathSelection struct {
	FirstPathIDs            []string
	FallbackPathIDs         []string
	FirstPathCountChangedAt int64
}

func (selection PathSelection) Role(id string) string {
	if containsPathID(selection.FirstPathIDs, id) {
		return PathRoleFirst
	}
	if containsPathID(selection.FallbackPathIDs, id) {
		return PathRoleFallback
	}
	return ""
}

func (selection PathSelection) Without(id string) PathSelection {
	return PathSelection{FirstPathIDs: removePathID(selection.FirstPathIDs, id), FallbackPathIDs: removePathID(selection.FallbackPathIDs, id), FirstPathCountChangedAt: selection.FirstPathCountChangedAt}
}

func SelectPathSelection(current PathSelection, candidates []string, pathStats map[string]PathStats, now int64, timeout time.Duration) PathSelection {
	firstCandidates := make([]string, 0, len(candidates))
	fallbackCandidates := make([]string, 0, len(candidates))
	for _, id := range candidates {
		stats, ok := pathStats[id]
		if !ok {
			continue
		}
		if stats.Eligible(now, timeout) {
			firstCandidates = append(firstCandidates, id)
			continue
		}
		if stats.FallbackEligible(now, timeout) {
			fallbackCandidates = append(fallbackCandidates, id)
		}
	}
	sortPathIDs(firstCandidates, pathStats, now)
	sortPathIDs(fallbackCandidates, pathStats, now)
	if len(firstCandidates) == 0 {
		return PathSelection{FallbackPathIDs: append([]string(nil), fallbackCandidates...), FirstPathCountChangedAt: firstPathCountChangedAt(current, 0, now)}
	}
	ranked := stabilizePathRanking(current, firstCandidates, pathStats)
	desiredFirstCount := selectFirstPathCount(ranked, pathStats, now, timeout)
	firstCount, changedAt := stabilizeFirstPathCount(current, desiredFirstCount, len(ranked), now)
	fallbackPathIDs := mergePathIDs(ranked[firstCount:], fallbackCandidates)
	return PathSelection{FirstPathIDs: append([]string(nil), ranked[:firstCount]...), FallbackPathIDs: fallbackPathIDs, FirstPathCountChangedAt: changedAt}
}

func sortPathIDs(ids []string, pathStats map[string]PathStats, now int64) {
	sort.SliceStable(ids, func(i, j int) bool {
		return pathStatsLess(ids[i], ids[j], pathStats, now)
	})
}

func stabilizePathRanking(current PathSelection, ranked []string, pathStats map[string]PathStats) []string {
	if len(current.FirstPathIDs) == 0 || len(ranked) == 0 {
		return ranked
	}
	best := ranked[0]
	stable := make([]string, 0, len(ranked))
	for _, id := range current.FirstPathIDs {
		if !containsPathID(ranked, id) || containsPathID(stable, id) {
			continue
		}
		if shouldKeepPathAhead(id, best, pathStats) {
			stable = append(stable, id)
		}
	}
	for _, id := range ranked {
		if !containsPathID(stable, id) {
			stable = append(stable, id)
		}
	}
	return stable
}

func pathStatsLess(leftID string, rightID string, pathStats map[string]PathStats, now int64) bool {
	left := pathStats[leftID]
	right := pathStats[rightID]
	leftScore := pathScore(left, now)
	rightScore := pathScore(right, now)
	if leftScore != rightScore {
		return leftScore < rightScore
	}
	if left.SmoothedRTT != right.SmoothedRTT {
		return left.SmoothedRTT < right.SmoothedRTT
	}
	return leftID < rightID
}

func pathScore(stats PathStats, now int64) int64 {
	score := stats.SmoothedRTT + stats.RTTVariance*4 + timeoutScoreAt(stats, now)/4 + int64(stats.Failures)*PathSelectionFailureRisk
	if score < 0 {
		return 0
	}
	return score
}

func shouldKeepPathAhead(currentID string, bestID string, pathStats map[string]PathStats) bool {
	if currentID == bestID {
		return true
	}
	current, currentOK := pathStats[currentID]
	best, bestOK := pathStats[bestID]
	if !currentOK || !bestOK {
		return false
	}
	return !scoreSignificantlyBetter(pathSwitchScore(best), pathSwitchScore(current))
}

func pathSwitchScore(stats PathStats) int64 {
	score := stats.SmoothedRTT + stats.RTTVariance*4
	if score < 0 {
		return 0
	}
	return score
}

func scoreSignificantlyBetter(candidateScore int64, currentScore int64) bool {
	if currentScore <= 0 || candidateScore >= currentScore {
		return false
	}
	margin := currentScore * PathSwitchRTTMarginPercent / 100
	if margin < PathSwitchRTTMarginMillis {
		margin = PathSwitchRTTMarginMillis
	}
	return currentScore-candidateScore >= margin
}

func selectFirstPathCount(ranked []string, pathStats map[string]PathStats, now int64, timeout time.Duration) int {
	if len(ranked) == 0 {
		return 0
	}
	limit := len(ranked)
	if limit > MaxFirstPathCount {
		limit = MaxFirstPathCount
	}
	combinedRisk := PathSelectionRiskScale
	for i := 0; i < limit; i++ {
		risk := pathRisk(pathStats[ranked[i]], now, timeout)
		combinedRisk = combinedRisk * risk / PathSelectionRiskScale
		if combinedRisk <= PathSelectionTargetRisk {
			return i + 1
		}
	}
	return limit
}

func stabilizeFirstPathCount(current PathSelection, desiredCount int, candidateCount int, now int64) (int, int64) {
	if candidateCount == 0 {
		return 0, 0
	}
	if desiredCount < 1 {
		desiredCount = 1
	}
	if desiredCount > candidateCount {
		desiredCount = candidateCount
	}
	currentCount := len(current.FirstPathIDs)
	if currentCount > candidateCount {
		currentCount = candidateCount
	}
	if currentCount < 1 || desiredCount > currentCount {
		return desiredCount, now
	}
	if desiredCount == currentCount {
		return desiredCount, firstPathCountChangedAt(current, desiredCount, now)
	}
	changedAt := firstPathCountChangedAt(current, currentCount, now)
	if now-changedAt < PathSelectionFirstCountHoldMillis {
		return currentCount, changedAt
	}
	return desiredCount, now
}

func firstPathCountChangedAt(current PathSelection, count int, now int64) int64 {
	if len(current.FirstPathIDs) == count && current.FirstPathCountChangedAt > 0 {
		return current.FirstPathCountChangedAt
	}
	return now
}

func fallbackWindowMillis() int64 {
	return PathSelectionFallbackWindowMillis
}

func pathRisk(stats PathStats, now int64, timeout time.Duration) int64 {
	risk := PathSelectionBaseRisk + timeoutScoreAt(stats, now) + int64(stats.Failures)*PathSelectionFailureRisk
	if stats.SmoothedRTT > 0 && stats.RTTVariance > 0 {
		jitterPercent := stats.RTTVariance * 100 / stats.SmoothedRTT
		if jitterPercent > PathSelectionJitterThreshold {
			risk += (jitterPercent - PathSelectionJitterThreshold) * 4
		}
	}
	timeoutMillis := timeout.Milliseconds()
	if timeoutMillis > 0 && stats.LastSuccess > 0 {
		agePercent := (now - stats.LastSuccess) * 100 / timeoutMillis
		if agePercent > PathSelectionStaleThreshold {
			risk += (agePercent - PathSelectionStaleThreshold) * 5
		}
	}
	if risk < PathSelectionBaseRisk {
		return PathSelectionBaseRisk
	}
	if risk > PathSelectionMaxRisk {
		return PathSelectionMaxRisk
	}
	return risk
}

func timeoutScoreAt(stats PathStats, now int64) int64 {
	if stats.TimeoutScore <= 0 {
		return 0
	}
	if stats.TimeoutScoreUpdatedAt <= 0 || now <= stats.TimeoutScoreUpdatedAt {
		return stats.TimeoutScore
	}
	elapsed := now - stats.TimeoutScoreUpdatedAt
	if elapsed >= PathSelectionTimeoutScoreWindowMillis {
		return 0
	}
	return stats.TimeoutScore * (PathSelectionTimeoutScoreWindowMillis - elapsed) / PathSelectionTimeoutScoreWindowMillis
}
func containsPathID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func removePathID(ids []string, target string) []string {
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != target {
			filtered = append(filtered, id)
		}
	}
	return filtered
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
