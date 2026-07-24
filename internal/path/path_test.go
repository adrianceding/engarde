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
	manager := NewManager(config.Client{ExcludedInterfaces: []string{"wg*"}})
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
	originalInterfaceAddrs := interfaceAddrs
	t.Cleanup(func() { interfaceAddrs = originalInterfaceAddrs })
	interfaceAddrs = func(net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
			&net.IPNet{IP: net.ParseIP("192.0.2.10"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}
	if got := AddressByInterface(net.Interface{Name: "test"}); got != "192.0.2.10" {
		t.Fatalf("AddressByInterface = %q, want 192.0.2.10", got)
	}
	interfaceAddrs = func(net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		}, nil
	}
	if got := AddressByInterface(net.Interface{Name: "loopback"}); got != "" {
		t.Fatalf("AddressByInterface disallowed addresses = %q, want empty", got)
	}
}

func TestAddressByInterfaceSelectsIPv4Deterministically(t *testing.T) {
	originalInterfaceAddrs := interfaceAddrs
	t.Cleanup(func() { interfaceAddrs = originalInterfaceAddrs })
	ascending := []net.Addr{
		&net.IPNet{IP: net.ParseIP("192.0.2.2"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("192.0.2.10"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("198.51.100.1"), Mask: net.CIDRMask(24, 32)},
	}
	descending := []net.Addr{ascending[2], ascending[1], ascending[0]}

	for _, addrs := range [][]net.Addr{ascending, descending} {
		interfaceAddrs = func(net.Interface) ([]net.Addr, error) { return addrs, nil }
		if got := AddressByInterface(net.Interface{Name: "multi-address"}); got != "192.0.2.2" {
			t.Fatalf("AddressByInterface = %q, want stable lowest address 192.0.2.2", got)
		}
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
	manager := NewManager(config.Client{
		IncludeInterfaces:  []string{"eth*", "wg?"},
		ExcludedInterfaces: []string{"wg0", "eth9", "br-*"},
	})

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
	if manager.IsExcluded("eth0") {
		t.Fatal("eth0 should return to configured inclusion after reset")
	}
	if !manager.IsExcluded("lo") {
		t.Fatal("lo should return to allowlist exclusion after reset")
	}
}

func TestManagerInterfacePatterns(t *testing.T) {
	tests := []struct {
		name     string
		included []string
		excluded []string
		ifName   string
		want     bool
	}{
		{
			name:     "empty lists allow by default",
			ifName:   "eth0",
			excluded: nil,
			want:     false,
		},
		{
			name:     "exact exclusion remains compatible",
			excluded: []string{"wg0"},
			ifName:   "wg0",
			want:     true,
		},
		{
			name:     "star matches docker bridge",
			excluded: []string{"br-*"},
			ifName:   "br-482fb47",
			want:     true,
		},
		{
			name:     "star requires prefix",
			excluded: []string{"br-*"},
			ifName:   "docker0",
			want:     false,
		},
		{
			name:     "question mark matches one character",
			excluded: []string{"eth?"},
			ifName:   "eth0",
			want:     true,
		},
		{
			name:     "question mark does not match two characters",
			excluded: []string{"eth?"},
			ifName:   "eth10",
			want:     false,
		},
		{
			name:     "character group matches",
			excluded: []string{"en[ops]0"},
			ifName:   "enp0",
			want:     true,
		},
		{
			name:     "character group rejects other characters",
			excluded: []string{"en[ops]0"},
			ifName:   "enx0",
			want:     false,
		},
		{
			name:     "matching allowlist entry includes",
			included: []string{"eth*"},
			ifName:   "eth0",
			want:     false,
		},
		{
			name:     "missing allowlist entry excludes",
			included: []string{"eth*"},
			ifName:   "wlan0",
			want:     true,
		},
		{
			name:     "exclude wins include conflict",
			included: []string{"br-*"},
			excluded: []string{"br-test*"},
			ifName:   "br-test123",
			want:     true,
		},
		{
			name:     "invalid exclude pattern is ignored",
			excluded: []string{"["},
			ifName:   "eth0",
			want:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(config.Client{
				IncludeInterfaces:  test.included,
				ExcludedInterfaces: test.excluded,
			})
			if got := manager.IsExcluded(test.ifName); got != test.want {
				t.Fatalf("IsExcluded(%q) = %v, want %v", test.ifName, got, test.want)
			}
		})
	}
}

func TestManagerPatternExclusionOverrides(t *testing.T) {
	manager := NewManager(config.Client{
		IncludeInterfaces:  []string{"eth*"},
		ExcludedInterfaces: []string{"eth9", "br-*"},
	})

	if status := manager.Exclude("eth0"); status != "ok" {
		t.Fatalf("Exclude included interface = %q, want ok", status)
	}
	if !manager.IsExcluded("eth0") {
		t.Fatal("eth0 should be excluded by runtime override")
	}
	if status := manager.Exclude("eth0"); status != "already-excluded" {
		t.Fatalf("Exclude excluded interface = %q, want already-excluded", status)
	}
	if status := manager.Include("br-docker"); status != "ok" {
		t.Fatalf("Include pattern-excluded interface = %q, want ok", status)
	}
	if manager.IsExcluded("br-docker") {
		t.Fatal("br-docker should be included by runtime override")
	}
	if status := manager.Include("br-docker"); status != "already-included" {
		t.Fatalf("Include included interface = %q, want already-included", status)
	}
	if status := manager.ResetExclusions(); status != "ok" {
		t.Fatalf("ResetExclusions = %q, want ok", status)
	}
	if manager.IsExcluded("eth0") {
		t.Fatal("eth0 should return to its allowlist inclusion after reset")
	}
	if !manager.IsExcluded("br-docker") {
		t.Fatal("br-docker should return to its configured exclusion after reset")
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
