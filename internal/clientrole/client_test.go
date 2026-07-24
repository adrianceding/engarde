package clientrole

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
	cfg := config.Client{
		Transfer: config.Transfer{
			TCP: config.TCPTransfer{MaxStreams: 17},
		},
	}

	client := New(cfg, "test-version", nil)

	want := config.Transfer{
		KeepaliveIntervalMillis: config.DefaultTransferKeepaliveIntervalMillis,
		KeepaliveTimeoutMillis:  config.DefaultTransferKeepaliveTimeoutMillis,
		TCP: config.TCPTransfer{
			CarrierMode:        config.TCPCarrierModeRedundant,
			ChunkSize:          config.DefaultTCPChunkSize,
			CarrierQueueBytes:  config.DefaultTCPCarrierQueueBytes,
			ReorderWindowBytes: config.DefaultTCPReorderWindowBytes,
			DialTimeoutMillis:  config.DefaultTCPDialTimeoutMillis,
			OpenTimeoutMillis:  config.DefaultTCPOpenTimeoutMillis,
			WriteTimeoutMillis: config.DefaultTCPWriteTimeoutMillis,
			MaxStreams:         17,
		},
	}
	if got := client.cfg.Transfer; got != want {
		t.Fatalf("transfer defaults = %+v, want %+v", got, want)
	}
}

func TestRunRejectsInvalidTCPListenAddress(t *testing.T) {
	client := New(config.Client{
		ListenAddr: "missing-port",
		DstAddr:    "127.0.0.1:59501",
	}, "", nil)

	err := client.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded with an invalid TCP listen address")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Net != "tcp" {
		t.Fatalf("Run error = %T %v, want a TCP listen error", err, err)
	}
	if runtime := client.getTCPRuntime(); runtime != nil {
		t.Fatal("Run installed a TCP runtime after listen failed")
	}
}

func TestStatusBeforeTCPRuntimeStarts(t *testing.T) {
	client := New(config.Client{
		Description: "SOCKS5 client",
		ListenAddr:  "127.0.0.1:59401",
		SOCKS5Auth:  &config.Credentials{Username: "frontend", Password: "secret"},
		PeerAuth:    &config.Credentials{Username: "peer", Password: "secret"},
	}, "test-version", nil)

	statusValue, err := client.Status()
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	status, ok := statusValue.(control.ClientStatus)
	if !ok {
		t.Fatalf("Status returned %T, want control.ClientStatus", statusValue)
	}
	want := control.ClientStatus{
		Type:                "client",
		Version:             "test-version",
		Description:         "SOCKS5 client",
		ListenAddress:       "127.0.0.1:59401",
		FrontendAuthEnabled: true,
		PeerAuthEnabled:     true,
		Interfaces:          []control.WebInterface{},
	}
	if !reflect.DeepEqual(status, want) {
		t.Fatalf("Status = %#v, want %#v", status, want)
	}
}

func TestInterfaceExclusionActionsDelegateToPathManager(t *testing.T) {
	client := New(config.Client{ExcludedInterfaces: []string{"wg0"}}, "", nil)

	if !client.paths.IsExcluded("wg0") {
		t.Fatal("configured interface is initially included")
	}
	if got := client.Include("wg0"); got != "ok" || client.paths.IsExcluded("wg0") {
		t.Fatalf("Include configured exclusion = %q, excluded = %v; want ok/false", got, client.paths.IsExcluded("wg0"))
	}
	if got := client.Include("wg0"); got != "already-included" {
		t.Fatalf("Include already included interface = %q, want already-included", got)
	}
	if got := client.Exclude("wg0"); got != "ok" || !client.paths.IsExcluded("wg0") {
		t.Fatalf("Exclude included interface = %q, excluded = %v; want ok/true", got, client.paths.IsExcluded("wg0"))
	}
	if got := client.Exclude("wg0"); got != "already-excluded" {
		t.Fatalf("Exclude already excluded interface = %q, want already-excluded", got)
	}
	if got := client.ToggleOverride("eth0"); got != "ok" || !client.paths.IsExcluded("eth0") {
		t.Fatalf("ToggleOverride included interface = %q, excluded = %v; want ok/true", got, client.paths.IsExcluded("eth0"))
	}
	if got := client.ResetExclusions(); got != "ok" {
		t.Fatalf("ResetExclusions = %q, want ok", got)
	}
	if !client.paths.IsExcluded("wg0") || client.paths.IsExcluded("eth0") {
		t.Fatalf("exclusions after reset: wg0=%v eth0=%v, want true/false", client.paths.IsExcluded("wg0"), client.paths.IsExcluded("eth0"))
	}
}
