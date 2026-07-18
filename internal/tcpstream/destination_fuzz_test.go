package tcpstream

import "testing"

func FuzzDecodeDestinationRoundTrip(f *testing.F) {
	f.Add([]byte{byte(DestinationIPv4), 127, 0, 0, 1, 0x01, 0xbb})
	f.Add([]byte{byte(DestinationIPv6), 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x20, 0xfb})
	f.Add([]byte{byte(DestinationDomain), 11, 'E', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'C', 'O', 'M', 0, 80})
	f.Add([]byte{})
	f.Add([]byte{byte(DestinationDomain), 4, 'b', 'a', 'd', '_', 0, 80})
	f.Add([]byte{0xff, 127, 0, 0, 1, 0, 80})

	f.Fuzz(func(t *testing.T, payload []byte) {
		destination, err := DecodeDestination(payload)
		if err != nil {
			return
		}
		if destination.IsZero() || destination.Host() == "" || destination.Port() == 0 {
			t.Fatalf("successful decode produced an invalid destination: %#v", destination)
		}
		canonical, err := destination.Encode()
		if err != nil {
			t.Fatalf("Encode after successful DecodeDestination: %v", err)
		}
		roundTrip, err := DecodeDestination(canonical)
		if err != nil {
			t.Fatalf("DecodeDestination of canonical encoding %x: %v", canonical, err)
		}
		if roundTrip != destination {
			t.Fatalf("destination round trip = %#v, want %#v", roundTrip, destination)
		}
	})
}
