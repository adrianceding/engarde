package tcpstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestPrefaceAndFrameRoundTrip(t *testing.T) {
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	if err := WritePreface(&buffer, Preface{}); err != nil {
		t.Fatal(err)
	}
	want := Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 9, Payload: []byte("payload")}
	if err := WriteFrame(&buffer, want); err != nil {
		t.Fatal(err)
	}
	preface, err := ReadPreface(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buffer, preface.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.Direction != want.Direction || got.StreamID != want.StreamID || got.Offset != want.Offset || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("frame = %#v, want %#v", got, want)
	}
}

func TestReadFrameRejectsPayloadBeforeAllocation(t *testing.T) {
	header := make([]byte, HeaderSize)
	header[0] = byte(FrameData)
	binary.BigEndian.PutUint16(header[2:4], HeaderSize)
	binary.BigEndian.PutUint32(header[4:8], MaxPayloadSize+1)
	if _, err := ReadFrame(bytes.NewReader(header), MaxPayloadSize); !errors.Is(err, ErrPayloadLength) {
		t.Fatalf("error = %v, want ErrPayloadLength", err)
	}
}

func TestFrameValidation(t *testing.T) {
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameOpen, Direction: DirectionServerToClient}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("OPEN error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameData, Direction: DirectionClientToServer}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("empty DATA error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameRST, Direction: DirectionClientToServer, Payload: []byte{1}}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("RST error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameOpenResult, Direction: DirectionClientToServer, StreamID: StreamID{1}, Payload: []byte{byte(OpenResultSuccess)}}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("OPEN_RESULT error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameAck, Direction: DirectionClientToServer, Payload: []byte{2}}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("ACK error = %v", err)
	}
}

func TestAckFrameRoundTrip(t *testing.T) {
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	want := Frame{Type: FrameAck, Direction: DirectionClientToServer, StreamID: streamID, Offset: 1234, Payload: []byte{1}}
	var buffer bytes.Buffer
	if err := WriteFrame(&buffer, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buffer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.Direction != want.Direction || got.StreamID != want.StreamID || got.Offset != want.Offset || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("ACK round trip = %#v, want %#v", got, want)
	}
}

func TestPrefaceRejectsUnsupportedVersions(t *testing.T) {
	for _, version := range []byte{1, 2, 3} {
		buffer := make([]byte, PrefaceSize)
		binary.BigEndian.PutUint32(buffer[0:4], Magic)
		buffer[4] = version
		binary.BigEndian.PutUint16(buffer[6:8], PrefaceSize)
		binary.BigEndian.PutUint32(buffer[8:12], MaxPayloadSize)
		if _, err := ReadPreface(bytes.NewReader(buffer)); !errors.Is(err, ErrInvalidPreface) {
			t.Fatalf("version %d error = %v, want ErrInvalidPreface", version, err)
		}
	}
}

func TestPrefaceAuthRequiredFlagRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	if err := WritePreface(&buffer, Preface{Version: Version, Flags: PrefaceFlagAuthRequired}); err != nil {
		t.Fatal(err)
	}
	preface, err := ReadPreface(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if preface.Flags != PrefaceFlagAuthRequired {
		t.Fatalf("flags = %d, want %d", preface.Flags, PrefaceFlagAuthRequired)
	}
	if err := WritePreface(&bytes.Buffer{}, Preface{Flags: 0x80}); !errors.Is(err, ErrInvalidPreface) {
		t.Fatalf("unknown flag error = %v", err)
	}
}

func TestActiveStandbyPrefaceRoundTrip(t *testing.T) {
	want := Preface{
		Version:                     Version,
		Flags:                       PrefaceFlagAuthRequired | PrefaceFlagActiveStandby,
		MaxPayload:                  32 * 1024,
		ServerInstanceID:            ServerInstanceID{1, 2, 3},
		ServerOrphanRetentionMillis: 9000,
	}
	var buffer bytes.Buffer
	if err := WritePreface(&buffer, want); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != ActiveStandbyPrefaceSize {
		t.Fatalf("active-standby preface size = %d, want %d", buffer.Len(), ActiveStandbyPrefaceSize)
	}
	got, err := ReadPreface(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("preface = %#v, want %#v", got, want)
	}
}

func TestActiveStandbyPrefaceRejectsInvalidExtension(t *testing.T) {
	buffer, _, err := marshalPreface(Preface{Flags: PrefaceFlagActiveStandby, ServerInstanceID: ServerInstanceID{1}, ServerOrphanRetentionMillis: 9000})
	if err != nil {
		t.Fatal(err)
	}
	buffer[len(buffer)-1] = 1
	if _, err := ReadPreface(bytes.NewReader(buffer)); !errors.Is(err, ErrInvalidPreface) {
		t.Fatalf("reserved extension error = %v, want ErrInvalidPreface", err)
	}
	if _, err := ReadPreface(bytes.NewReader(buffer[:PrefaceSize])); err == nil {
		t.Fatal("truncated active-standby preface was accepted")
	}
}

func TestOpenAndResultRoundTrip(t *testing.T) {
	streamID, err := NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	destination, err := ParseDestination("ss.example.com:8388")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	if err := WritePreface(&buffer, Preface{Version: Version}); err != nil {
		t.Fatal(err)
	}
	open := Frame{Type: FrameOpen, Direction: DirectionClientToServer, StreamID: streamID, Payload: payload}
	if err := WriteFrame(&buffer, open); err != nil {
		t.Fatal(err)
	}
	result := Frame{Type: FrameOpenResult, Direction: DirectionServerToClient, StreamID: streamID, Payload: []byte{byte(OpenResultSuccess)}}
	if err := WriteFrame(&buffer, result); err != nil {
		t.Fatal(err)
	}
	preface, err := ReadPreface(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if preface.Version != Version {
		t.Fatalf("version = %d, want %d", preface.Version, Version)
	}
	gotOpen, err := ReadFrame(&buffer, preface.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	gotDestination, err := DecodeDestination(gotOpen.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if gotOpen.StreamID != streamID || gotDestination != destination {
		t.Fatalf("OPEN = %#v/%#v, want stream %x destination %#v", gotOpen, gotDestination, streamID, destination)
	}
	gotResult, err := ReadFrame(&buffer, preface.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if gotResult.Type != FrameOpenResult || OpenResult(gotResult.Payload[0]) != OpenResultSuccess {
		t.Fatalf("OPEN_RESULT = %#v", gotResult)
	}
}

func TestOpenValidation(t *testing.T) {
	destination, err := ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameOpen, Direction: DirectionClientToServer, StreamID: StreamID{1}}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("empty OPEN error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Type: FrameOpen, Direction: DirectionClientToServer, Payload: payload}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("zero stream ID OPEN error = %v", err)
	}
}

func TestRecoverableOpenAndResumeRoundTrip(t *testing.T) {
	streamID := StreamID{1}
	token := ResumeToken{2, 3, 4}
	destination, err := ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	wantOpen := RecoverableOpen{Destination: destination, ResumeToken: token, Generation: 1, RecoveryTimeoutMillis: 3000}
	openFrame, err := wantOpen.Frame(streamID)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	if err := WriteFrame(&buffer, openFrame); err != nil {
		t.Fatal(err)
	}
	readOpen, err := ReadFrame(&buffer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	gotOpen, err := DecodeRecoverableOpen(readOpen)
	if err != nil {
		t.Fatal(err)
	}
	if gotOpen != wantOpen {
		t.Fatalf("recoverable OPEN = %#v, want %#v", gotOpen, wantOpen)
	}

	wantResult := RecoverableOpenResult{Result: OpenResultSuccess, Generation: 1, ServerOrphanRetentionMillis: 9000}
	if err := WriteFrame(&buffer, wantResult.Frame(streamID)); err != nil {
		t.Fatal(err)
	}
	readResult, err := ReadFrame(&buffer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	gotResult, err := DecodeRecoverableOpenResult(readResult)
	if err != nil {
		t.Fatal(err)
	}
	if gotResult != wantResult {
		t.Fatalf("recoverable OPEN result = %#v, want %#v", gotResult, wantResult)
	}

	resume := NewResumeFrame(streamID, token, 2)
	if err := WriteFrame(&buffer, resume); err != nil {
		t.Fatal(err)
	}
	readResume, err := ReadFrame(&buffer, MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	gotToken, err := ResumeTokenFromFrame(readResume)
	if err != nil {
		t.Fatal(err)
	}
	if gotToken != token || readResume.Offset != 2 {
		t.Fatalf("RESUME = token %x generation %d", gotToken, readResume.Offset)
	}
	if err := WriteFrame(&buffer, NewResumeResultFrame(streamID, 2, ResumeResultSuccess)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(&buffer, MaxPayloadSize); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverableFrameValidation(t *testing.T) {
	destination, err := ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	valid := RecoverableOpen{Destination: destination, ResumeToken: ResumeToken{1}, Generation: 1, RecoveryTimeoutMillis: 3000}
	if _, err := (RecoverableOpen{Destination: destination, Generation: 1, RecoveryTimeoutMillis: 3000}).Frame(StreamID{1}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("zero resume token error = %v", err)
	}
	valid.Generation = 2
	if _, err := valid.Frame(StreamID{1}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("initial generation error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, NewResumeFrame(StreamID{1}, ResumeToken{1}, 1)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("stale RESUME generation error = %v", err)
	}
	if err := WriteFrame(&bytes.Buffer{}, (RecoverableOpenResult{Result: OpenResultSuccess, Generation: 1}).Frame(StreamID{1})); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("zero retention result error = %v", err)
	}
}
