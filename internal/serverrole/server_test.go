package serverrole

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
)

func TestNewAppliesTCPDefaults(t *testing.T) {
	cfg := config.Server{
		Transfer: config.Transfer{
			TCP: config.TCPTransfer{MaxPendingConnections: 23},
		},
	}

	server := New(cfg, "test-version", nil)

	want := config.Transfer{
		KeepaliveIntervalMillis: config.DefaultTransferKeepaliveIntervalMillis,
		KeepaliveTimeoutMillis:  config.DefaultTransferKeepaliveTimeoutMillis,
		TCP: config.TCPTransfer{
			ChunkSize:             config.DefaultTCPChunkSize,
			CarrierQueueBytes:     config.DefaultTCPCarrierQueueBytes,
			ReorderWindowBytes:    config.DefaultTCPReorderWindowBytes,
			DialTimeoutMillis:     config.DefaultTCPDialTimeoutMillis,
			OpenTimeoutMillis:     config.DefaultTCPOpenTimeoutMillis,
			WriteTimeoutMillis:    config.DefaultTCPWriteTimeoutMillis,
			MaxPendingConnections: 23,
		},
	}
	if got := server.cfg.Transfer; got != want {
		t.Fatalf("transfer defaults = %+v, want %+v", got, want)
	}
}

func TestRunRejectsInvalidTCPListenAddress(t *testing.T) {
	server := New(config.Server{ListenAddr: "missing-port"}, "", nil)

	err := server.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded with an invalid TCP listen address")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Net != "tcp" {
		t.Fatalf("Run error = %T %v, want a TCP listen error", err, err)
	}
	if runtime := server.getTCPRuntime(); runtime != nil {
		t.Fatal("Run installed a TCP runtime after listen failed")
	}
}

func TestStatusBeforeTCPRuntimeStarts(t *testing.T) {
	server := New(config.Server{
		Description: "TCP server",
		ListenAddr:  "0.0.0.0:59501",
		PeerAuth: &config.ServerPeerAuth{
			Users: map[string]string{"peer": "secret"},
		},
	}, "test-version", nil)

	statusValue, err := server.Status()
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	status, ok := statusValue.(control.ServerStatus)
	if !ok {
		t.Fatalf("Status returned %T, want control.ServerStatus", statusValue)
	}
	want := control.ServerStatus{
		Type:            "server",
		Version:         "test-version",
		Description:     "TCP server",
		ListenAddress:   "0.0.0.0:59501",
		PeerAuthEnabled: true,
		Sockets:         []control.WebSocket{},
	}
	if !reflect.DeepEqual(status, want) {
		t.Fatalf("Status = %#v, want %#v", status, want)
	}
}

func TestAllowedClients(t *testing.T) {
	tests := []struct {
		name    string
		allowed []string
		client  string
		want    bool
	}{
		{name: "default IPv4", client: "203.0.113.7", want: true},
		{name: "default IPv6", client: "2001:db8::7", want: true},
		{name: "exact IPv4", allowed: []string{"192.0.2.10"}, client: "192.0.2.10", want: true},
		{name: "other IPv4", allowed: []string{"192.0.2.10"}, client: "192.0.2.11", want: false},
		{name: "exact IPv6", allowed: []string{"2001:db8::10"}, client: "2001:db8::10", want: true},
		{name: "other IPv6", allowed: []string{"2001:db8::10"}, client: "2001:db8::11", want: false},
		{name: "IPv4 CIDR member", allowed: []string{"198.51.100.0/24"}, client: "198.51.100.200", want: true},
		{name: "IPv4 CIDR nonmember", allowed: []string{"198.51.100.0/24"}, client: "198.51.101.1", want: false},
		{name: "IPv6 CIDR member", allowed: []string{"2001:db8:1::/48"}, client: "2001:db8:1::ffff", want: true},
		{name: "IPv6 CIDR nonmember", allowed: []string{"2001:db8:1::/48"}, client: "2001:db8:2::1", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ip := net.ParseIP(test.client)
			if ip == nil {
				t.Fatalf("test client address %q did not parse", test.client)
			}
			server := New(config.Server{AllowedClients: test.allowed}, "", nil)
			if got := server.clientIPAllowed(ip); got != test.want {
				t.Fatalf("clientIPAllowed(%q) with %v = %v, want %v", test.client, test.allowed, got, test.want)
			}
		})
	}
}
