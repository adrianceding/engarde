package transport

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/adrianceding/engarde/internal/udp"
)

const (
	Magic   uint32 = 0x45474144
	Version uint8  = 1
)

const HeaderSize = 36

const MaxPayloadSize = udp.MaxPacketSize - HeaderSize

type FrameType uint8

const (
	FrameProbe FrameType = iota + 1
	FrameProbeAck
	FrameKeepalive
	FrameKeepaliveAck
	FrameData
	FrameAck
)

var (
	ErrNotFrame       = errors.New("not an engarde transport frame")
	ErrShortFrame     = errors.New("short transport frame")
	ErrUnknownVersion = errors.New("unknown transport version")
	ErrUnknownType    = errors.New("unknown transport frame type")
	ErrPayloadLength  = errors.New("invalid transport payload length")
)

type PacketID struct {
	Session  uint64
	Sequence uint64
}

type Frame struct {
	Type    FrameType
	ID      PacketID
	SentAt  int64
	Payload []byte
}

func Encode(frame Frame) ([]byte, error) {
	if !validFrameType(frame.Type) {
		return nil, fmt.Errorf("%w: %d", ErrUnknownType, frame.Type)
	}
	payloadLen := len(frame.Payload)
	if payloadLen > MaxPayloadSize {
		return nil, fmt.Errorf("%w: %d", ErrPayloadLength, payloadLen)
	}
	buf := make([]byte, HeaderSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], Magic)
	buf[4] = Version
	buf[5] = uint8(frame.Type)
	binary.BigEndian.PutUint16(buf[6:8], uint16(HeaderSize))
	binary.BigEndian.PutUint64(buf[8:16], frame.ID.Session)
	binary.BigEndian.PutUint64(buf[16:24], frame.ID.Sequence)
	binary.BigEndian.PutUint64(buf[24:32], uint64(frame.SentAt))
	binary.BigEndian.PutUint32(buf[32:36], uint32(payloadLen))
	copy(buf[HeaderSize:], frame.Payload)
	return buf, nil
}

func Decode(payload []byte) (Frame, error) {
	if len(payload) < 4 || binary.BigEndian.Uint32(payload[0:4]) != Magic {
		return Frame{}, ErrNotFrame
	}
	if len(payload) < HeaderSize {
		return Frame{}, ErrShortFrame
	}
	if payload[4] != Version {
		return Frame{}, fmt.Errorf("%w: %d", ErrUnknownVersion, payload[4])
	}
	frameType := FrameType(payload[5])
	if !validFrameType(frameType) {
		return Frame{}, fmt.Errorf("%w: %d", ErrUnknownType, frameType)
	}
	headerSize := int(binary.BigEndian.Uint16(payload[6:8]))
	if headerSize != HeaderSize || headerSize > len(payload) {
		return Frame{}, ErrPayloadLength
	}
	payloadLen := int(binary.BigEndian.Uint32(payload[32:36]))
	if payloadLen > MaxPayloadSize || len(payload)-headerSize != payloadLen {
		return Frame{}, ErrPayloadLength
	}
	return Frame{
		Type: frameType,
		ID: PacketID{
			Session:  binary.BigEndian.Uint64(payload[8:16]),
			Sequence: binary.BigEndian.Uint64(payload[16:24]),
		},
		SentAt:  int64(binary.BigEndian.Uint64(payload[24:32])),
		Payload: payload[headerSize:],
	}, nil
}

func IsControl(frameType FrameType) bool {
	switch frameType {
	case FrameProbe, FrameProbeAck, FrameKeepalive, FrameKeepaliveAck, FrameAck:
		return true
	}
	return false
}

func validFrameType(frameType FrameType) bool {
	switch frameType {
	case FrameProbe, FrameProbeAck, FrameKeepalive, FrameKeepaliveAck, FrameData, FrameAck:
		return true
	}
	return false
}
