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
