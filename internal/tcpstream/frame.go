package tcpstream

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	Magic                   uint32 = 0x45475443
	Version                 uint8  = 4
	PrefaceSize                    = 16
	HeaderSize                     = 32
	MaxPayloadSize                 = 64 * 1024
	PrefaceFlagAuthRequired uint8  = 1 << 0
	prefaceKnownFlags              = PrefaceFlagAuthRequired
)

type FrameType uint8

const (
	FrameOpen FrameType = iota + 1
	FrameData
	FrameFIN
	FrameRST
	FrameAck
	FrameOpenResult
)

type OpenResult uint8

const (
	OpenResultSuccess OpenResult = iota
	OpenResultGeneralFailure
	OpenResultConnectionRefused
	OpenResultNetworkUnreachable
	OpenResultHostUnreachable
	OpenResultTimeout
	OpenResultPolicyDenied
)

type OpenError struct {
	Result OpenResult
}

func (err *OpenError) Error() string {
	return fmt.Sprintf("TCP destination open failed: result %d", err.Result)
}

type Direction uint8

const (
	DirectionClientToServer Direction = iota
	DirectionServerToClient
)

var (
	ErrInvalidPreface = errors.New("invalid tcp stream preface")
	ErrInvalidFrame   = errors.New("invalid tcp stream frame")
	ErrPayloadLength  = errors.New("invalid tcp stream payload length")
)

type StreamID [16]byte

func NewStreamID() (StreamID, error) {
	var id StreamID
	_, err := io.ReadFull(rand.Reader, id[:])
	return id, err
}

type Preface struct {
	Version    uint8
	Flags      uint8
	MaxPayload uint32
}

type Frame struct {
	Type      FrameType
	Direction Direction
	StreamID  StreamID
	Offset    uint64
	Payload   []byte
}

type pooledFrameWriteBuffer struct {
	data []byte
}

type frameReader struct {
	header [HeaderSize]byte
}

var (
	frameWrite1KiBPool  = sync.Pool{New: func() any { return &pooledFrameWriteBuffer{data: make([]byte, HeaderSize+1<<10)} }}
	frameWrite4KiBPool  = sync.Pool{New: func() any { return &pooledFrameWriteBuffer{data: make([]byte, HeaderSize+4<<10)} }}
	frameWrite16KiBPool = sync.Pool{New: func() any { return &pooledFrameWriteBuffer{data: make([]byte, HeaderSize+16<<10)} }}
	frameWrite32KiBPool = sync.Pool{New: func() any { return &pooledFrameWriteBuffer{data: make([]byte, HeaderSize+32<<10)} }}
	frameWrite64KiBPool = sync.Pool{New: func() any { return &pooledFrameWriteBuffer{data: make([]byte, HeaderSize+64<<10)} }}
)

func WritePreface(writer io.Writer, preface Preface) error {
	buffer, _, err := marshalPreface(preface)
	if err != nil {
		return err
	}
	return writeFull(writer, buffer[:])
}

func marshalPreface(preface Preface) ([PrefaceSize]byte, Preface, error) {
	var buffer [PrefaceSize]byte
	version := preface.Version
	if version == 0 {
		version = Version
	}
	if version != Version {
		return buffer, Preface{}, fmt.Errorf("%w: version %d", ErrInvalidPreface, version)
	}
	if preface.Flags & ^prefaceKnownFlags != 0 {
		return buffer, Preface{}, fmt.Errorf("%w: flags %d", ErrInvalidPreface, preface.Flags)
	}
	maxPayload := preface.MaxPayload
	if maxPayload == 0 {
		maxPayload = MaxPayloadSize
	}
	if maxPayload > MaxPayloadSize {
		return buffer, Preface{}, fmt.Errorf("%w: max payload %d", ErrInvalidPreface, maxPayload)
	}
	binary.BigEndian.PutUint32(buffer[0:4], Magic)
	buffer[4] = version
	buffer[5] = preface.Flags
	binary.BigEndian.PutUint16(buffer[6:8], PrefaceSize)
	binary.BigEndian.PutUint32(buffer[8:12], maxPayload)
	return buffer, Preface{Version: version, Flags: preface.Flags, MaxPayload: maxPayload}, nil
}

func ReadPreface(reader io.Reader) (Preface, error) {
	buffer := make([]byte, PrefaceSize)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return Preface{}, err
	}
	version := buffer[4]
	maxPayload := binary.BigEndian.Uint32(buffer[8:12])
	flags := buffer[5]
	if binary.BigEndian.Uint32(buffer[0:4]) != Magic || version != Version || flags & ^prefaceKnownFlags != 0 || binary.BigEndian.Uint16(buffer[6:8]) != PrefaceSize || maxPayload == 0 || maxPayload > MaxPayloadSize || binary.BigEndian.Uint32(buffer[12:16]) != 0 {
		return Preface{}, ErrInvalidPreface
	}
	return Preface{Version: version, Flags: flags, MaxPayload: maxPayload}, nil
}

func WriteFrame(writer io.Writer, frame Frame) error {
	if err := validateFrame(frame); err != nil {
		return err
	}
	pooled, pool := acquireFrameWriteBuffer(len(frame.Payload))
	buffer := pooled.data[:HeaderSize+len(frame.Payload)]
	buffer[0] = byte(frame.Type)
	buffer[1] = byte(frame.Direction)
	binary.BigEndian.PutUint16(buffer[2:4], HeaderSize)
	binary.BigEndian.PutUint32(buffer[4:8], uint32(len(frame.Payload)))
	copy(buffer[8:24], frame.StreamID[:])
	binary.BigEndian.PutUint64(buffer[24:32], frame.Offset)
	copy(buffer[HeaderSize:], frame.Payload)
	if err := writeFull(writer, buffer); err != nil {
		// An asynchronous writer may still own buffer after returning an
		// error (for example, a timed-out smux queue). Do not recycle it.
		return err
	}
	pool.Put(pooled)
	return nil
}

func acquireFrameWriteBuffer(payloadSize int) (*pooledFrameWriteBuffer, *sync.Pool) {
	var pool *sync.Pool
	switch {
	case payloadSize <= 1<<10:
		pool = &frameWrite1KiBPool
	case payloadSize <= 4<<10:
		pool = &frameWrite4KiBPool
	case payloadSize <= 16<<10:
		pool = &frameWrite16KiBPool
	case payloadSize <= 32<<10:
		pool = &frameWrite32KiBPool
	default:
		pool = &frameWrite64KiBPool
	}
	return pool.Get().(*pooledFrameWriteBuffer), pool
}

func ReadFrame(reader io.Reader, maxPayload uint32) (Frame, error) {
	return new(frameReader).read(reader, maxPayload)
}

func (decoder *frameReader) read(reader io.Reader, maxPayload uint32) (Frame, error) {
	if maxPayload == 0 || maxPayload > MaxPayloadSize {
		maxPayload = MaxPayloadSize
	}
	header := decoder.header[:]
	if _, err := io.ReadFull(reader, header); err != nil {
		return Frame{}, err
	}
	if binary.BigEndian.Uint16(header[2:4]) != HeaderSize {
		return Frame{}, ErrInvalidFrame
	}
	payloadLength := binary.BigEndian.Uint32(header[4:8])
	if payloadLength > maxPayload {
		return Frame{}, fmt.Errorf("%w: %d", ErrPayloadLength, payloadLength)
	}
	frame := Frame{
		Type:      FrameType(header[0]),
		Direction: Direction(header[1]),
		Offset:    binary.BigEndian.Uint64(header[24:32]),
	}
	copy(frame.StreamID[:], header[8:24])
	if payloadLength > 0 {
		frame.Payload = make([]byte, int(payloadLength))
		if _, err := io.ReadFull(reader, frame.Payload); err != nil {
			return Frame{}, err
		}
	}
	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func validateFrame(frame Frame) error {
	if frame.Direction != DirectionClientToServer && frame.Direction != DirectionServerToClient {
		return fmt.Errorf("%w: direction %d", ErrInvalidFrame, frame.Direction)
	}
	if len(frame.Payload) > MaxPayloadSize {
		return fmt.Errorf("%w: %d", ErrPayloadLength, len(frame.Payload))
	}
	switch frame.Type {
	case FrameOpen:
		if frame.Direction != DirectionClientToServer || frame.Offset != 0 {
			return ErrInvalidFrame
		}
		if frame.StreamID == (StreamID{}) {
			return ErrInvalidFrame
		}
		if _, err := DecodeDestination(frame.Payload); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidFrame, err)
		}
	case FrameData:
		if len(frame.Payload) == 0 || frame.Offset > ^uint64(0)-uint64(len(frame.Payload)) {
			return ErrInvalidFrame
		}
	case FrameFIN:
		if len(frame.Payload) != 0 {
			return ErrInvalidFrame
		}
	case FrameRST:
		if len(frame.Payload) != 2 {
			return ErrInvalidFrame
		}
	case FrameAck:
		if len(frame.Payload) != 1 || frame.Payload[0] > 1 {
			return ErrInvalidFrame
		}
	case FrameOpenResult:
		if frame.Direction != DirectionServerToClient || frame.Offset != 0 || frame.StreamID == (StreamID{}) || len(frame.Payload) != 1 || OpenResult(frame.Payload[0]) > OpenResultPolicyDenied {
			return ErrInvalidFrame
		}
	default:
		return fmt.Errorf("%w: type %d", ErrInvalidFrame, frame.Type)
	}
	return nil
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
