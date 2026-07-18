package tcpstream

import "testing"

func TestDestinationRoundTrip(t *testing.T) {
	tests := []struct {
		address string
		want    string
	}{
		{address: "192.0.2.1:443", want: "192.0.2.1:443"},
		{address: "[2001:db8::1]:8443", want: "[2001:db8::1]:8443"},
		{address: "Proxy.Example.COM.:1080", want: "proxy.example.com:1080"},
	}
	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			destination, err := ParseDestination(test.address)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := destination.Encode()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeDestination(payload)
			if err != nil {
				t.Fatal(err)
			}
			if decoded != destination || decoded.String() != test.want {
				t.Fatalf("decoded = %#v/%q, want %#v/%q", decoded, decoded.String(), destination, test.want)
			}
		})
	}
}

func TestDestinationRejectsInvalidValues(t *testing.T) {
	for _, address := range []string{"", "example.com", "example.com:0", "[fe80::1%eth0]:443", "bad_name.example:443"} {
		if _, err := ParseDestination(address); err == nil {
			t.Fatalf("ParseDestination(%q) succeeded", address)
		}
	}
	for _, payload := range [][]byte{{}, {0x03, 0, 0, 80}, {0x01, 127, 0, 0, 1, 0}} {
		if _, err := DecodeDestination(payload); err == nil {
			t.Fatalf("DecodeDestination(%x) succeeded", payload)
		}
	}
}
