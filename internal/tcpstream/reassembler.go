package tcpstream

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
)

var (
	ErrReorderWindow = errors.New("tcp stream reorder window exceeded")
	ErrOverlap       = errors.New("tcp stream overlap mismatch")
	ErrFIN           = errors.New("invalid tcp stream FIN")
	ErrSegmentLimit  = errors.New("tcp stream segment limit exceeded")
)

const MaxReorderSegments = 4096

type segment struct {
	offset uint64
	data   []byte
}

type Reassembler struct {
	mu          sync.Mutex
	nextOffset  uint64
	buffered    int
	windowBytes uint64
	finOffset   *uint64
	segments    []segment
}

func NewReassembler(windowBytes int) *Reassembler {
	return &Reassembler{windowBytes: uint64(windowBytes)}
}

func (reassembler *Reassembler) Push(offset uint64, payload []byte) error {
	return reassembler.push(offset, payload, true)
}

// pushOwned lets the data path transfer an immutable payload into the
// reassembler. Callers of Push retain the original copy-on-insert contract.
func (reassembler *Reassembler) pushOwned(offset uint64, payload []byte) error {
	return reassembler.push(offset, payload, false)
}

func (reassembler *Reassembler) push(offset uint64, payload []byte, copyPayload bool) error {
	if len(payload) == 0 || offset > ^uint64(0)-uint64(len(payload)) {
		return ErrInvalidFrame
	}
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()

	end := offset + uint64(len(payload))
	if end <= reassembler.nextOffset {
		return nil
	}
	if reassembler.finOffset != nil && end > *reassembler.finOffset {
		return ErrFIN
	}
	if offset < reassembler.nextOffset {
		trim := reassembler.nextOffset - offset
		payload = payload[trim:]
		offset = reassembler.nextOffset
		end = offset + uint64(len(payload))
	}
	if reassembler.windowBytes == 0 || end-reassembler.nextOffset > reassembler.windowBytes {
		return ErrReorderWindow
	}

	for _, existing := range reassembler.segments {
		existingEnd := existing.offset + uint64(len(existing.data))
		overlapStart := maxUint64(offset, existing.offset)
		overlapEnd := minUint64(end, existingEnd)
		if overlapStart < overlapEnd {
			incomingStart := overlapStart - offset
			existingStart := overlapStart - existing.offset
			length := overlapEnd - overlapStart
			if !bytes.Equal(payload[incomingStart:incomingStart+length], existing.data[existingStart:existingStart+length]) {
				return ErrOverlap
			}
		}
	}

	cursor := offset
	for _, existing := range reassembler.segments {
		existingEnd := existing.offset + uint64(len(existing.data))
		if existingEnd <= cursor || existing.offset >= end {
			continue
		}
		if cursor < existing.offset {
			reassembler.addSegment(cursor, payload[cursor-offset:existing.offset-offset], copyPayload)
		}
		if existingEnd > cursor {
			cursor = existingEnd
		}
	}
	if cursor < end {
		reassembler.addSegment(cursor, payload[cursor-offset:], copyPayload)
	}
	reassembler.mergeSegments()
	if len(reassembler.segments) > MaxReorderSegments {
		return ErrSegmentLimit
	}
	if uint64(reassembler.buffered) > reassembler.windowBytes {
		return ErrReorderWindow
	}
	return nil
}

func (reassembler *Reassembler) SetFIN(offset uint64) error {
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()
	if offset < reassembler.nextOffset {
		return ErrFIN
	}
	for _, item := range reassembler.segments {
		if item.offset+uint64(len(item.data)) > offset {
			return ErrFIN
		}
	}
	if reassembler.finOffset != nil && *reassembler.finOffset != offset {
		return ErrFIN
	}
	finOffset := offset
	reassembler.finOffset = &finOffset
	return nil
}

func (reassembler *Reassembler) DrainTo(writer io.Writer) (bool, error) {
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()
	drained := 0
	defer func() {
		if drained == 0 {
			return
		}
		remaining := copy(reassembler.segments, reassembler.segments[drained:])
		clear(reassembler.segments[remaining:])
		reassembler.segments = reassembler.segments[:remaining]
	}()
	for drained < len(reassembler.segments) && reassembler.segments[drained].offset == reassembler.nextOffset {
		item := &reassembler.segments[drained]
		for len(item.data) > 0 {
			written, err := writer.Write(item.data)
			if written > 0 {
				item.offset += uint64(written)
				item.data = item.data[written:]
				reassembler.nextOffset += uint64(written)
				reassembler.buffered -= written
			}
			if err != nil {
				return false, err
			}
			if written <= 0 {
				return false, io.ErrShortWrite
			}
		}
		drained++
	}
	return reassembler.finOffset != nil && reassembler.nextOffset == *reassembler.finOffset, nil
}

func (reassembler *Reassembler) BufferedBytes() int {
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()
	return reassembler.buffered
}

func (reassembler *Reassembler) NextOffset() uint64 {
	reassembler.mu.Lock()
	defer reassembler.mu.Unlock()
	return reassembler.nextOffset
}

func (reassembler *Reassembler) addSegment(offset uint64, payload []byte, copyPayload bool) {
	if len(payload) == 0 {
		return
	}
	if copyPayload {
		payload = append([]byte(nil), payload...)
	} else {
		payload = payload[:len(payload):len(payload)]
	}
	reassembler.segments = append(reassembler.segments, segment{offset: offset, data: payload})
	reassembler.buffered += len(payload)
}

func (reassembler *Reassembler) mergeSegments() {
	if len(reassembler.segments) < 2 {
		return
	}
	slices.SortFunc(reassembler.segments, func(first, second segment) int {
		return cmp.Compare(first.offset, second.offset)
	})
	merged := reassembler.segments[:0]
	for _, item := range reassembler.segments {
		if len(merged) == 0 {
			merged = append(merged, item)
			continue
		}
		last := &merged[len(merged)-1]
		lastEnd := last.offset + uint64(len(last.data))
		if lastEnd == item.offset {
			last.data = append(last.data, item.data...)
			continue
		}
		if lastEnd > item.offset {
			panic(fmt.Sprintf("overlapping tcp stream segments at %d", item.offset))
		}
		merged = append(merged, item)
	}
	reassembler.segments = merged
}

func minUint64(first, second uint64) uint64 {
	if first < second {
		return first
	}
	return second
}

func maxUint64(first, second uint64) uint64 {
	if first > second {
		return first
	}
	return second
}
