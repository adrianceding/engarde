package transport

import (
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	encoded, err := Encode(Frame{Type: FrameData, ID: PacketID{Session: 7, Sequence: 42}, SentAt: 1234, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if decoded.Type != FrameData || decoded.ID.Session != 7 || decoded.ID.Sequence != 42 || decoded.SentAt != 1234 || string(decoded.Payload) != "payload" {
		t.Fatalf("decoded frame = %#v", decoded)
	}
}

func TestFrameRejectsOversizedAndTruncatedPayload(t *testing.T) {
	oversized := make([]byte, MaxPayloadSize+1)
	if _, err := Encode(Frame{Type: FrameData, Payload: oversized}); !errors.Is(err, ErrPayloadLength) {
		t.Fatalf("oversized payload error = %v, want ErrPayloadLength", err)
	}
	encoded, err := Encode(Frame{Type: FrameData, Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded[:len(encoded)-1]); !errors.Is(err, ErrPayloadLength) {
		t.Fatalf("truncated payload error = %v, want ErrPayloadLength", err)
	}
}

func TestDecodeRejectsInvalidFrames(t *testing.T) {
	if _, err := Decode([]byte("raw-wireguard")); !errors.Is(err, ErrNotFrame) {
		t.Fatalf("raw payload error = %v, want ErrNotFrame", err)
	}
	encoded, err := Encode(Frame{Type: FrameAck, ID: PacketID{Session: 1, Sequence: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded[:HeaderSize-1]); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("short frame error = %v, want ErrShortFrame", err)
	}
	encoded[4] = Version + 1
	if _, err := Decode(encoded); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("version error = %v, want ErrUnknownVersion", err)
	}
	encoded[4] = Version
	encoded[5] = 255
	if _, err := Decode(encoded); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("type error = %v, want ErrUnknownType", err)
	}
}

func TestControlFrameClassification(t *testing.T) {
	for _, frameType := range []FrameType{FrameProbe, FrameProbeAck, FrameKeepalive, FrameKeepaliveAck, FrameAck} {
		if !IsControl(frameType) {
			t.Fatalf("%v should be control", frameType)
		}
	}
	if IsControl(FrameData) {
		t.Fatal("FrameData should not be control")
	}
}
