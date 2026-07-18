package tcpstream

import (
	"bytes"
	"errors"
	"testing"
)

type partialErrorWriter struct {
	calls int
	err   error
}

func (writer *partialErrorWriter) Write(payload []byte) (int, error) {
	writer.calls++
	if writer.calls == 1 {
		return len(payload), nil
	}
	return 1, writer.err
}

func TestReassemblerOrdersAndDeduplicates(t *testing.T) {
	reassembler := NewReassembler(64)
	if err := reassembler.Push(3, []byte("def")); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	finished, err := reassembler.DrainTo(&output)
	if err != nil {
		t.Fatal(err)
	}
	if finished || output.Len() != 0 || reassembler.NextOffset() != 0 || reassembler.BufferedBytes() != 3 {
		t.Fatalf("gap state = finished %v/output %q/offset %d/buffered %d", finished, output.String(), reassembler.NextOffset(), reassembler.BufferedBytes())
	}

	if err := reassembler.Push(3, []byte("def")); err != nil {
		t.Fatal(err)
	}
	if buffered := reassembler.BufferedBytes(); buffered != 3 {
		t.Fatalf("buffered bytes after duplicate = %d, want 3", buffered)
	}
	if err := reassembler.Push(0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	finished, err = reassembler.DrainTo(&output)
	if err != nil {
		t.Fatal(err)
	}
	if finished || output.String() != "abcdef" || reassembler.BufferedBytes() != 0 {
		t.Fatalf("finished=%v output=%q buffered=%d", finished, output.String(), reassembler.BufferedBytes())
	}
	if reassembler.NextOffset() != 6 {
		t.Fatalf("next offset = %d, want 6", reassembler.NextOffset())
	}
	if err := reassembler.Push(0, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if buffered := reassembler.BufferedBytes(); buffered != 0 {
		t.Fatalf("delivered duplicate buffered %d bytes, want 0", buffered)
	}
}

func TestReassemblerFINWaitsForGapThenCompletes(t *testing.T) {
	reassembler := NewReassembler(64)
	if err := reassembler.Push(3, []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.SetFIN(6); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	finished, err := reassembler.DrainTo(&output)
	if err != nil {
		t.Fatal(err)
	}
	if finished || output.Len() != 0 || reassembler.NextOffset() != 0 {
		t.Fatalf("FIN before gap = finished %v/output %q/offset %d", finished, output.String(), reassembler.NextOffset())
	}
	if err := reassembler.Push(0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	finished, err = reassembler.DrainTo(&output)
	if err != nil {
		t.Fatal(err)
	}
	if !finished || output.String() != "abcdef" || reassembler.NextOffset() != 6 {
		t.Fatalf("FIN after gap = finished %v/output %q/offset %d", finished, output.String(), reassembler.NextOffset())
	}
}

func TestReassemblerPushCopiesInput(t *testing.T) {
	reassembler := NewReassembler(1024)
	payload := []byte("immutable")
	if err := reassembler.Push(0, payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'

	var output bytes.Buffer
	if _, err := reassembler.DrainTo(&output); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "immutable" {
		t.Fatalf("drained payload = %q, want immutable", got)
	}
}

func TestReassemblerDrainRetainsPartialSegmentAfterError(t *testing.T) {
	wantErr := errors.New("partial write")
	reassembler := NewReassembler(1024)
	reassembler.segments = []segment{
		{offset: 0, data: []byte("abc")},
		{offset: 3, data: []byte("def")},
	}
	reassembler.buffered = 6

	finished, err := reassembler.DrainTo(&partialErrorWriter{err: wantErr})
	if finished || !errors.Is(err, wantErr) {
		t.Fatalf("first drain = finished %v/error %v, want false/%v", finished, err, wantErr)
	}
	if reassembler.nextOffset != 4 || reassembler.buffered != 2 || len(reassembler.segments) != 1 || reassembler.segments[0].offset != 4 || string(reassembler.segments[0].data) != "ef" {
		t.Fatalf("state after partial error = offset %d/buffered %d/segments %#v", reassembler.nextOffset, reassembler.buffered, reassembler.segments)
	}

	var remaining bytes.Buffer
	if finished, err = reassembler.DrainTo(&remaining); err != nil || finished || remaining.String() != "ef" {
		t.Fatalf("second drain = finished %v/error %v/data %q, want false/nil/ef", finished, err, remaining.String())
	}
	if reassembler.nextOffset != 6 || reassembler.buffered != 0 || len(reassembler.segments) != 0 {
		t.Fatalf("final state = offset %d/buffered %d/segments %#v", reassembler.nextOffset, reassembler.buffered, reassembler.segments)
	}
}

func TestReassemblerRejectsConflictingOverlapAndWindow(t *testing.T) {
	reassembler := NewReassembler(8)
	if err := reassembler.Push(2, []byte("cdef")); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.Push(3, []byte("XXX")); !errors.Is(err, ErrOverlap) {
		t.Fatalf("overlap error = %v", err)
	}
	if err := reassembler.Push(8, []byte("x")); !errors.Is(err, ErrReorderWindow) {
		t.Fatalf("window error = %v", err)
	}
}

func TestReassemblerFINValidation(t *testing.T) {
	reassembler := NewReassembler(1024)
	if err := reassembler.Push(4, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.SetFIN(7); !errors.Is(err, ErrFIN) {
		t.Fatalf("short FIN error = %v", err)
	}
	if err := reassembler.SetFIN(8); err != nil {
		t.Fatal(err)
	}
	if err := reassembler.SetFIN(9); !errors.Is(err, ErrFIN) {
		t.Fatalf("conflicting FIN error = %v", err)
	}
}

func TestReassemblerLimitsSparseSegments(t *testing.T) {
	reassembler := NewReassembler(MaxReorderSegments*2 + 2)
	for index := 0; index < MaxReorderSegments; index++ {
		if err := reassembler.Push(uint64(index*2+1), []byte{'x'}); err != nil {
			t.Fatalf("Push %d returned error: %v", index, err)
		}
	}
	if err := reassembler.Push(uint64(MaxReorderSegments*2+1), []byte{'x'}); !errors.Is(err, ErrSegmentLimit) {
		t.Fatalf("segment limit error = %v", err)
	}
}
