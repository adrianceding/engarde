package path

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	stdpath "path"
	"sort"
	"strings"
	"sync"

	"github.com/adrianceding/engarde/internal/config"
)

var listSystemInterfaces = net.Interfaces
var parseCIDR = net.ParseCIDR
var interfaceAddrs = func(iface net.Interface) ([]net.Addr, error) { return iface.Addrs() }

type Manager struct {
	included []string
	excluded []string
	labels   map[string]string
	override map[string]string
	dstAddr  string

	mu    sync.RWMutex
	swaps map[string]bool
}

func NewManager(client config.Client) *Manager {
	overrides := make(map[string]string, len(client.DstOverrides))
	for _, override := range client.DstOverrides {
		overrides[override.IfName] = override.DstAddr
	}
	labels := make(map[string]string, len(client.InterfaceLabels))
	for name, label := range client.InterfaceLabels {
		labels[name] = label
	}
	return &Manager{
		included: append([]string(nil), client.IncludeInterfaces...),
		excluded: append([]string(nil), client.ExcludedInterfaces...),
		labels:   labels,
		override: overrides,
		dstAddr:  client.DstAddr,
		swaps:    make(map[string]bool),
	}
}

func (manager *Manager) IsExcluded(name string) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.isExcludedLocked(name)
}

func (manager *Manager) ToggleExclusion(name string) string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.swaps[name] {
		delete(manager.swaps, name)
	} else {
		manager.swaps[name] = true
	}
	return "ok"
}

func (manager *Manager) Include(name string) string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.isExcludedLocked(name) {
		return "already-included"
	}
	manager.toggleLocked(name)
	return "ok"
}

func (manager *Manager) Exclude(name string) string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.isExcludedLocked(name) {
		return "already-excluded"
	}
	manager.toggleLocked(name)
	return "ok"
}

func (manager *Manager) ResetExclusions() string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.swaps = make(map[string]bool)
	return "ok"
}

func (manager *Manager) Label(name string) string {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.labels[name]
}

func (manager *Manager) Destination(name string) string {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if dstAddr, ok := manager.override[name]; ok {
		return dstAddr
	}
	return manager.dstAddr
}

func (manager *Manager) isExcludedLocked(name string) bool {
	excluded := len(manager.included) > 0 && !matchesInterface(manager.included, name)
	excluded = excluded || matchesInterface(manager.excluded, name)
	if manager.swaps[name] {
		return !excluded
	}
	return excluded
}

func matchesInterface(patterns []string, name string) bool {
	for _, pattern := range patterns {
		matched, err := stdpath.Match(pattern, name)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func (manager *Manager) toggleLocked(name string) {
	if manager.swaps[name] {
		delete(manager.swaps, name)
		return
	}
	manager.swaps[name] = true
}

func ListInterfaces(writer io.Writer) error {
	interfaces, err := listSystemInterfaces()
	if err != nil {
		return err
	}
	for _, iface := range interfaces {
		if _, err := fmt.Fprintf(writer, "\r\n%s\r\n  Address: %s\r\n", iface.Name, AddressByInterface(iface)); err != nil {
			return err
		}
	}
	return nil
}

func AddressByInterface(iface net.Interface) string {
	addrs, err := interfaceAddrs(iface)
	if err != nil {
		return ""
	}
	addresses := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		address, ok := interfaceIPv4(addr)
		if ok && IsAddressAllowed(address.String()) {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return ""
	}
	sort.Slice(addresses, func(left, right int) bool {
		return addresses[left].Less(addresses[right])
	})
	return addresses[0].String()
}

func interfaceIPv4(addr net.Addr) (netip.Addr, bool) {
	if addr == nil {
		return netip.Addr{}, false
	}
	var ip net.IP
	switch addr := addr.(type) {
	case *net.IPNet:
		ip = addr.IP
	case *net.IPAddr:
		ip = addr.IP
	default:
		parsed, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			parsed = net.ParseIP(addr.String())
		}
		ip = parsed
	}
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}
	address = address.Unmap()
	return address, address.Is4()
}

func IsAddressAllowed(address string) bool {
	if strings.ContainsRune(address, ':') {
		return false
	}
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}
	for _, disallowedNetwork := range []string{"169.254.0.0/16", "127.0.0.0/8"} {
		_, subnet, err := parseCIDR(disallowedNetwork)
		if err != nil {
			continue
		}
		if subnet.Contains(ip) {
			return false
		}
	}
	return true
}
