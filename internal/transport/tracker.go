package transport

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

type Tracker struct {
	mu         sync.Mutex
	session    uint64
	nextSeq    uint64
	pending    *PendingRing
	duplicates *DuplicateWindow
}

type RetryRecord struct {
	PendingRecord
}

func NewTracker(pendingCapacity, duplicateCapacity int) *Tracker {
	return &Tracker{
		session:    newSessionID(),
		pending:    NewPendingRing(pendingCapacity),
		duplicates: NewDuplicateWindow(duplicateCapacity),
	}
}

func NowMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (tracker *Tracker) NextID() PacketID {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.nextSeq++
	return PacketID{Session: tracker.session, Sequence: tracker.nextSeq}
}

func (tracker *Tracker) Track(record PendingRecord) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.pending.Put(record)
}

func (tracker *Tracker) Complete(id PacketID) (PendingRecord, bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.Complete(id)
}

func (tracker *Tracker) Drop(id PacketID) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.Drop(id)
}

func (tracker *Tracker) Get(id PacketID) (PendingRecord, bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.Get(id)
}

func (tracker *Tracker) UpdatePaths(id PacketID, pathIDs []string) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.UpdatePaths(id, pathIDs)
}

func (tracker *Tracker) RecordAttempt(id PacketID, pathIDs []string) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.RecordAttempt(id, pathIDs)
}

func (tracker *Tracker) SeenOrRecord(id PacketID) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.duplicates.SeenOrRecord(id)
}

func (tracker *Tracker) Due(now int64, minTimeoutMillis int64, maxTimeoutMillis int64, maxRetries int) []RetryRecord {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.pending.Due(now, minTimeoutMillis, maxTimeoutMillis, maxRetries)
}

func newSessionID() uint64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return binary.BigEndian.Uint64(buf[:])
	}
	return uint64(time.Now().UnixNano())
}
