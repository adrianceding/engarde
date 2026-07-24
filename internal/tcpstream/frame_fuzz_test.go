package tcpstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func FuzzReadPreface(f *testing.F) {
	valid := fuzzPrefaceWire(0, MaxPayloadSize)
	f.Add(valid)
	f.Add(fuzzPrefaceWire(PrefaceFlagAuthRequired, 1))
	f.Add(fuzzPrefaceWire(PrefaceFlagAuthRequired, MaxPayloadSize))
	f.Add(fuzzPrefaceWire(PrefaceFlagActiveStandby, MaxPayloadSize))
	f.Add(append(append([]byte(nil), valid...), 0xa5))

	for _, size := range []int{0, 1, 4, 5, 8, 12, PrefaceSize - 1} {
		f.Add(append([]byte(nil), valid[:size]...))
	}
	for _, mutate := range []func([]byte){
		func(wire []byte) { wire[0] ^= 0xff },
		func(wire []byte) { wire[4] = Version + 1 },
		func(wire []byte) { wire[5] = 0x80 },
		func(wire []byte) { binary.BigEndian.PutUint16(wire[6:8], PrefaceSize-1) },
		func(wire []byte) { binary.BigEndian.PutUint32(wire[8:12], 0) },
		func(wire []byte) { binary.BigEndian.PutUint32(wire[8:12], MaxPayloadSize+1) },
		func(wire []byte) { wire[15] = 1 },
	} {
		wire := append([]byte(nil), valid...)
		mutate(wire)
		f.Add(wire)
	}

	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > ActiveStandbyPrefaceSize+32 {
			return
		}

		reader := bytes.NewReader(wire)
		preface, err := ReadPreface(reader)
		if len(wire) < PrefaceSize {
			if err == nil {
				t.Fatalf("ReadPreface accepted %d-byte input", len(wire))
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ReadPreface short-input error = %v, want EOF", err)
			}
			return
		}
		if err != nil {
			return
		}

		if preface.Version != Version {
			t.Fatalf("accepted preface version = %d, want %d", preface.Version, Version)
		}
		if preface.Flags&^prefaceKnownFlags != 0 {
			t.Fatalf("accepted unknown preface flags %#x", preface.Flags)
		}
		if preface.MaxPayload == 0 || preface.MaxPayload > MaxPayloadSize {
			t.Fatalf("accepted max payload = %d", preface.MaxPayload)
		}
		consumed := PrefaceSize
		if preface.Flags&PrefaceFlagActiveStandby != 0 {
			consumed = ActiveStandbyPrefaceSize
		}
		if reader.Len() != len(wire)-consumed {
			t.Fatalf("ReadPreface consumed %d bytes, want %d", len(wire)-reader.Len(), consumed)
		}

		var canonical bytes.Buffer
		if err := WritePreface(&canonical, preface); err != nil {
			t.Fatalf("WritePreface after successful read: %v", err)
		}
		if !bytes.Equal(canonical.Bytes(), wire[:consumed]) {
			t.Fatalf("accepted non-canonical preface %x; canonical encoding is %x", wire[:consumed], canonical.Bytes())
		}
		roundTrip, err := ReadPreface(bytes.NewReader(canonical.Bytes()))
		if err != nil {
			t.Fatalf("ReadPreface of canonical encoding: %v", err)
		}
		if roundTrip != preface {
			t.Fatalf("preface round trip = %#v, want %#v", roundTrip, preface)
		}
	})
}

func FuzzReadFrame(f *testing.F) {
	streamID := StreamID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	destination := []byte{byte(DestinationDomain), 11, 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm', 0x01, 0xbb}
	resumeToken := ResumeToken{1}
	recoverablePayload := make([]byte, recoverableOpenFixedPayloadSize+len(destination))
	copy(recoverablePayload[:resumeTokenSize], resumeToken[:])
	binary.BigEndian.PutUint32(recoverablePayload[resumeTokenSize:recoverableOpenFixedPayloadSize], 3000)
	copy(recoverablePayload[recoverableOpenFixedPayloadSize:], destination)
	validFrames := []Frame{
		{Type: FrameOpen, Direction: DirectionClientToServer, StreamID: streamID, Payload: destination},
		{Type: FrameData, Direction: DirectionClientToServer, StreamID: streamID, Offset: 9, Payload: []byte{0}},
		{Type: FrameFIN, Direction: DirectionServerToClient, StreamID: streamID, Offset: 10},
		{Type: FrameRST, Direction: DirectionClientToServer, StreamID: streamID, Payload: []byte{0x12, 0x34}},
		{Type: FrameAck, Direction: DirectionServerToClient, Offset: ^uint64(0), Payload: []byte{0}},
		{Type: FrameData, Direction: DirectionServerToClient, StreamID: streamID, Offset: ^uint64(0) - 1, Payload: []byte{0}},
		{Type: FrameAck, Direction: DirectionClientToServer, StreamID: streamID, Offset: 10, Payload: []byte{1}},
		{Type: FrameOpenResult, Direction: DirectionServerToClient, StreamID: streamID, Payload: []byte{byte(OpenResultPolicyDenied)}},
		{Type: FrameRecoverableOpen, Direction: DirectionClientToServer, StreamID: streamID, Offset: 1, Payload: recoverablePayload},
		(RecoverableOpenResult{Result: OpenResultSuccess, Generation: 1, ServerOrphanRetentionMillis: 9000}).Frame(streamID),
		NewResumeFrame(streamID, resumeToken, 2),
		NewResumeResultFrame(streamID, 2, ResumeResultSuccess),
		{Type: FramePing, Direction: DirectionClientToServer, Offset: 1},
		{Type: FramePong, Direction: DirectionServerToClient, Offset: 1},
	}
	for _, frame := range validFrames {
		f.Add(fuzzFrameWire(frame), uint32(MaxPayloadSize))
	}
	f.Add(fuzzFrameWire(Frame{
		Type:      FrameData,
		Direction: DirectionServerToClient,
		StreamID:  streamID,
		Offset:    ^uint64(0) - uint64(MaxPayloadSize),
		Payload:   bytes.Repeat([]byte{0xa5}, MaxPayloadSize),
	}), uint32(MaxPayloadSize))
	f.Add(fuzzFrameWire(validFrames[1]), uint32(0))
	f.Add(fuzzFrameWire(validFrames[1]), uint32(MaxPayloadSize+1))

	validData := fuzzFrameWire(Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  streamID,
		Offset:    17,
		Payload:   []byte("payload"),
	})
	f.Add([]byte{}, uint32(MaxPayloadSize))
	f.Add(append([]byte(nil), validData[:HeaderSize-1]...), uint32(MaxPayloadSize))
	f.Add(append([]byte(nil), validData[:len(validData)-1]...), uint32(MaxPayloadSize))
	f.Add(append(append([]byte(nil), validData...), 0xde, 0xad), uint32(MaxPayloadSize))

	wrongHeaderSize := append([]byte(nil), validData...)
	binary.BigEndian.PutUint16(wrongHeaderSize[2:4], HeaderSize-1)
	f.Add(wrongHeaderSize, uint32(MaxPayloadSize))
	tooLargeForLimit := append([]byte(nil), validData...)
	f.Add(tooLargeForLimit, uint32(len("payload")-1))
	tooLargeForProtocol := make([]byte, HeaderSize)
	tooLargeForProtocol[0] = byte(FrameData)
	tooLargeForProtocol[1] = byte(DirectionClientToServer)
	binary.BigEndian.PutUint16(tooLargeForProtocol[2:4], HeaderSize)
	binary.BigEndian.PutUint32(tooLargeForProtocol[4:8], MaxPayloadSize+1)
	f.Add(tooLargeForProtocol, uint32(MaxPayloadSize))
	malformed := append([]byte(nil), validData...)
	malformed[0] = 0xff
	f.Add(malformed, uint32(MaxPayloadSize))
	malformed = append([]byte(nil), validData...)
	malformed[1] = 0xff
	f.Add(malformed, uint32(MaxPayloadSize))
	malformed = append([]byte(nil), validData[:HeaderSize]...)
	binary.BigEndian.PutUint32(malformed[4:8], 0)
	f.Add(malformed, uint32(MaxPayloadSize))
	malformed = append([]byte(nil), validData...)
	binary.BigEndian.PutUint64(malformed[24:32], ^uint64(0))
	f.Add(malformed, uint32(MaxPayloadSize))

	f.Fuzz(func(t *testing.T, wire []byte, maxPayload uint32) {
		if len(wire) > HeaderSize+MaxPayloadSize+HeaderSize {
			return
		}
		effectiveMax := maxPayload
		if effectiveMax == 0 || effectiveMax > MaxPayloadSize {
			effectiveMax = MaxPayloadSize
		}

		reader := bytes.NewReader(wire)
		frame, err := ReadFrame(reader, maxPayload)
		if len(wire) < HeaderSize {
			if err == nil {
				t.Fatalf("ReadFrame accepted %d-byte header", len(wire))
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ReadFrame short-header error = %v, want EOF", err)
			}
			return
		}

		declaredPayload := binary.BigEndian.Uint32(wire[4:8])
		headerSizeValid := binary.BigEndian.Uint16(wire[2:4]) == HeaderSize
		if err != nil {
			if headerSizeValid && declaredPayload > effectiveMax && !errors.Is(err, ErrPayloadLength) {
				t.Fatalf("ReadFrame oversized-payload error = %v, want ErrPayloadLength", err)
			}
			if headerSizeValid && declaredPayload <= effectiveMax && uint64(len(wire)-HeaderSize) < uint64(declaredPayload) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ReadFrame truncated-payload error = %v, want EOF", err)
			}
			return
		}

		if uint32(len(frame.Payload)) != declaredPayload {
			t.Fatalf("decoded payload length = %d, header declares %d", len(frame.Payload), declaredPayload)
		}
		if uint32(len(frame.Payload)) > effectiveMax {
			t.Fatalf("decoded payload length = %d, limit = %d", len(frame.Payload), effectiveMax)
		}
		if err := validateFrame(frame); err != nil {
			t.Fatalf("ReadFrame returned invalid frame: %v", err)
		}
		consumed := HeaderSize + len(frame.Payload)
		if consumed > len(wire) || reader.Len() != len(wire)-consumed {
			t.Fatalf("ReadFrame consumed %d bytes, want %d", len(wire)-reader.Len(), consumed)
		}

		var canonical bytes.Buffer
		if err := WriteFrame(&canonical, frame); err != nil {
			t.Fatalf("WriteFrame after successful read: %v", err)
		}
		if !bytes.Equal(canonical.Bytes(), wire[:consumed]) {
			t.Fatalf("accepted frame differs from its canonical encoding (wire length %d, canonical length %d)", consumed, canonical.Len())
		}
		roundTrip, err := ReadFrame(bytes.NewReader(canonical.Bytes()), maxPayload)
		if err != nil {
			t.Fatalf("ReadFrame of canonical encoding: %v", err)
		}
		if !fuzzFramesEqual(roundTrip, frame) {
			t.Fatalf("frame round trip = %#v, want %#v", roundTrip, frame)
		}
	})
}

func fuzzPrefaceWire(flags uint8, maxPayload uint32) []byte {
	size := PrefaceSize
	if flags&PrefaceFlagActiveStandby != 0 {
		size = ActiveStandbyPrefaceSize
	}
	wire := make([]byte, size)
	binary.BigEndian.PutUint32(wire[0:4], Magic)
	wire[4] = Version
	wire[5] = flags
	binary.BigEndian.PutUint16(wire[6:8], uint16(size))
	binary.BigEndian.PutUint32(wire[8:12], maxPayload)
	return wire
}

func fuzzFrameWire(frame Frame) []byte {
	var wire bytes.Buffer
	if err := WriteFrame(&wire, frame); err != nil {
		panic(err)
	}
	return wire.Bytes()
}

func fuzzFramesEqual(left, right Frame) bool {
	return left.Type == right.Type &&
		left.Direction == right.Direction &&
		left.StreamID == right.StreamID &&
		left.Offset == right.Offset &&
		bytes.Equal(left.Payload, right.Payload)
}
