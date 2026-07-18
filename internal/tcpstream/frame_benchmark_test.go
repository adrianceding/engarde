package tcpstream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkWriteFrame(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
		b.Run(benchmarkPayloadName(size), func(b *testing.B) {
			frame, wire := benchmarkDataFrame(size)
			var writer bytes.Buffer
			writer.Grow(len(wire))

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				writer.Reset()
				if err := WriteFrame(&writer, frame); err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(writer.Bytes(), wire) {
					b.Fatalf("encoded frame differs for %d-byte payload", size)
				}
			}
		})
	}
}

func BenchmarkReadFrame(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
		b.Run(benchmarkPayloadName(size), func(b *testing.B) {
			want, wire := benchmarkDataFrame(size)
			reader := bytes.NewReader(wire)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				reader.Reset(wire)
				got, err := ReadFrame(reader, MaxPayloadSize)
				if err != nil {
					b.Fatal(err)
				}
				if got.Type != want.Type ||
					got.Direction != want.Direction ||
					got.StreamID != want.StreamID ||
					got.Offset != want.Offset ||
					!bytes.Equal(got.Payload, want.Payload) ||
					reader.Len() != 0 {
					b.Fatalf("decoded frame differs for %d-byte payload", size)
				}
			}
		})
	}
}

func benchmarkDataFrame(payloadSize int) (Frame, []byte) {
	payload := make([]byte, payloadSize)
	for index := range payload {
		payload[index] = byte(index*31 + 17)
	}

	frame := Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  StreamID{0: 0x01, 5: 0x23, 10: 0x45, 15: 0x67},
		Offset:    0x0102030405060708,
		Payload:   payload,
	}
	wire := make([]byte, HeaderSize+payloadSize)
	wire[0] = byte(frame.Type)
	wire[1] = byte(frame.Direction)
	binary.BigEndian.PutUint16(wire[2:4], HeaderSize)
	binary.BigEndian.PutUint32(wire[4:8], uint32(payloadSize))
	copy(wire[8:24], frame.StreamID[:])
	binary.BigEndian.PutUint64(wire[24:32], frame.Offset)
	copy(wire[HeaderSize:], payload)
	return frame, wire
}

func benchmarkPayloadName(size int) string {
	if size < 1<<10 {
		return fmt.Sprintf("%dB", size)
	}
	return fmt.Sprintf("%dKiB", size>>10)
}
