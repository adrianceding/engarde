package path

import (
	"bytes"
	"errors"
	"net"
	"testing"

	"github.com/adrianceding/engarde/internal/config"
)

func TestIsAddressAllowed(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{address: "192.0.2.10", want: true},
		{address: "169.254.10.20", want: false},
		{address: "127.0.0.1", want: false},
		{address: "2001:db8::1", want: false},
		{address: "not-an-ip", want: false},
	}

	for _, test := range tests {
		if got := IsAddressAllowed(test.address); got != test.want {
			t.Fatalf("IsAddressAllowed(%q) = %v, want %v", test.address, got, test.want)
		}
	}
}

func TestToggleExclusionAndExcludeAlreadyExcluded(t *testing.T) {
	manager := NewManager(config.Client{ExcludedInterfaces: []string{"wg0"}})
	if status := manager.Exclude("wg0"); status != "already-excluded" {
		t.Fatalf("Exclude excluded = %q", status)
	}
	if status := manager.ToggleExclusion("eth0"); status != "ok" {
		t.Fatalf("ToggleExclusion = %q", status)
	}
	if !manager.IsExcluded("eth0") {
		t.Fatal("eth0 should be excluded after toggle")
	}
	manager.ToggleExclusion("eth0")
	if manager.IsExcluded("eth0") {
		t.Fatal("eth0 should be included after second toggle")
	}
}

func TestListInterfaces(t *testing.T) {
	originalList := listSystemInterfaces
	t.Cleanup(func() { listSystemInterfaces = originalList })
	listSystemInterfaces = func() ([]net.Interface, error) { return nil, errors.New("interfaces") }
	if err := ListInterfaces(&bytes.Buffer{}); err == nil {
		t.Fatal("ListInterfaces succeeded after interface error")
	}
	listSystemInterfaces = originalList

	var output bytes.Buffer
	if err := ListInterfaces(&output); err != nil {
		t.Fatalf("ListInterfaces returned error: %v", err)
	}
	if output.Len() == 0 {
		t.Fatal("ListInterfaces wrote no output")
	}
	failingWriter := writerFunc(func([]byte) (int, error) { return 0, errors.New("write") })
	if err := ListInterfaces(failingWriter); err == nil {
		t.Fatal("ListInterfaces succeeded with failing writer")
	}
}

func TestAddressByInterface(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("Interfaces returned error: %v", err)
	}
	if len(interfaces) == 0 {
		t.Skip("no interfaces available")
	}
	_ = AddressByInterface(interfaces[0])
	if got := AddressByInterface(net.Interface{Name: "definitely-not-real"}); got != "" {
		t.Fatalf("AddressByInterface missing interface = %q, want empty", got)
	}
}

type writerFunc func([]byte) (int, error)

func (fn writerFunc) Write(payload []byte) (int, error) { return fn(payload) }

func TestIsAddressAllowedAllowedPublicIP(t *testing.T) {
	if !IsAddressAllowed("8.8.8.8") {
		t.Fatal("8.8.8.8 should be allowed")
	}
}

func TestAddressByInterfaceAddrsError(t *testing.T) {
	originalInterfaceAddrs := interfaceAddrs
	t.Cleanup(func() { interfaceAddrs = originalInterfaceAddrs })
	interfaceAddrs = func(net.Interface) ([]net.Addr, error) { return nil, errors.New("addrs") }
	if got := AddressByInterface(net.Interface{Name: "bad"}); got != "" {
		t.Fatalf("AddressByInterface = %q, want empty string", got)
	}
}

func TestIsAddressAllowedSkipsCIDRParseError(t *testing.T) {
	originalParseCIDR := parseCIDR
	t.Cleanup(func() { parseCIDR = originalParseCIDR })
	parseCIDR = func(string) (net.IP, *net.IPNet, error) { return nil, nil, errors.New("cidr") }
	if !IsAddressAllowed("127.0.0.1") {
		t.Fatal("address should be allowed when disallowed CIDR parsing fails")
	}
}

func TestManagerExclusionSwaps(t *testing.T) {
	manager := NewManager(config.Client{ExcludedInterfaces: []string{"wg0"}})

	if !manager.IsExcluded("wg0") {
		t.Fatal("wg0 should start excluded")
	}
	if status := manager.Include("wg0"); status != "ok" {
		t.Fatalf("Include excluded interface = %q, want ok", status)
	}
	if manager.IsExcluded("wg0") {
		t.Fatal("wg0 should be included after Include")
	}
	if status := manager.Include("wg0"); status != "already-included" {
		t.Fatalf("Include included interface = %q, want already-included", status)
	}
	if status := manager.Exclude("wg0"); status != "ok" {
		t.Fatalf("Exclude included interface = %q, want ok", status)
	}
	if !manager.IsExcluded("wg0") {
		t.Fatal("wg0 should be excluded again")
	}
	if status := manager.ResetExclusions(); status != "ok" {
		t.Fatalf("ResetExclusions = %q, want ok", status)
	}
	if !manager.IsExcluded("wg0") {
		t.Fatal("wg0 should return to configured exclusion after reset")
	}
}

func TestManagerLabelsAndDestinations(t *testing.T) {
	manager := NewManager(config.Client{
		DstAddr:         "198.51.100.1:59501",
		InterfaceLabels: map[string]string{"eth0": "Main ISP"},
		DstOverrides: []config.DstOverride{
			{IfName: "eth1", DstAddr: "198.51.100.2:59501"},
		},
	})

	if label := manager.Label("eth0"); label != "Main ISP" {
		t.Fatalf("Label = %q, want Main ISP", label)
	}
	if dst := manager.Destination("eth0"); dst != "198.51.100.1:59501" {
		t.Fatalf("default Destination = %q", dst)
	}
	if dst := manager.Destination("eth1"); dst != "198.51.100.2:59501" {
		t.Fatalf("override Destination = %q", dst)
	}
}
