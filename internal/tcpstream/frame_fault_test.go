package tcpstream

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type blockedFragmentWriter struct {
	bytes.Buffer
	maximum int
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   int
}

func (writer *blockedFragmentWriter) Write(payload []byte) (int, error) {
	writer.once.Do(func() {
		close(writer.started)
		<-writer.release
	})
	writer.calls++
	if len(payload) > writer.maximum {
		payload = payload[:writer.maximum]
	}
	return writer.Buffer.Write(payload)
}

type zeroProgressWriter struct{}

func (zeroProgressWriter) Write([]byte) (int, error) {
	return 0, nil
}

type retainingErrorWriter struct {
	payload []byte
	err     error
}

func (writer *retainingErrorWriter) Write(payload []byte) (int, error) {
	writer.payload = payload
	return 0, writer.err
}

func TestWriteFrameHandlesBlockedFragmentedWriter(t *testing.T) {
	payload := make([]byte, 4097)
	for index := range payload {
		payload[index] = byte(index*31 + 7)
	}
	want := Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  StreamID{1, 2, 3, 4},
		Offset:    1<<33 + 19,
		Payload:   payload,
	}
	writer := &blockedFragmentWriter{
		maximum: 7,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- WriteFrame(writer, want) }()

	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("frame write did not reach the blocked writer")
	}
	select {
	case err := <-done:
		t.Fatalf("frame write completed while its writer was blocked: %v", err)
	default:
	}
	close(writer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fragmented frame write did not finish")
	}

	if writer.calls <= 1 {
		t.Fatalf("writer calls = %d, want multiple short writes", writer.calls)
	}
	got, err := ReadFrame(bytes.NewReader(writer.Bytes()), MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.Direction != want.Direction || got.StreamID != want.StreamID || got.Offset != want.Offset || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("decoded fragmented frame = %#v, want %#v", got, want)
	}
}

func TestFrameWritesRejectWriterWithoutProgress(t *testing.T) {
	frame := Frame{
		Type:      FrameAck,
		Direction: DirectionClientToServer,
		Payload:   []byte{0},
	}
	if err := WriteFrame(zeroProgressWriter{}, frame); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteFrame error = %v, want %v", err, io.ErrShortWrite)
	}
	if err := WritePreface(zeroProgressWriter{}, Preface{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WritePreface error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWriteFrameDoesNotRecycleBufferAfterWriteError(t *testing.T) {
	injected := errors.New("injected queued write failure")
	writer := &retainingErrorWriter{err: injected}
	first := Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  StreamID{1},
		Payload:   bytes.Repeat([]byte{0x11}, 1024),
	}
	if err := WriteFrame(writer, first); !errors.Is(err, injected) {
		t.Fatalf("first WriteFrame error = %v, want %v", err, injected)
	}
	wantRetained := append([]byte(nil), writer.payload...)

	second := first
	second.StreamID = StreamID{2}
	second.Payload = bytes.Repeat([]byte{0x22}, len(first.Payload))
	if err := WriteFrame(io.Discard, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(writer.payload, wantRetained) {
		t.Fatal("failed WriteFrame buffer was recycled while the writer still retained it")
	}
}
