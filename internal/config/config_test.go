package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "engarde.yml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func defaultTransfer() Transfer {
	var transfer Transfer
	transfer.ApplyDefaults()
	return transfer
}

func TestLoadErrors(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := Load(filepath.Join(dir, "missing.yml")); err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("Load missing file error = %v", err)
	}

	badYAML := writeTestConfig(t, "client: [")
	if _, _, err := Load(badYAML); err == nil || !strings.Contains(err.Error(), "parse config") {
		t.Fatalf("Load bad YAML error = %v", err)
	}

	incomplete := writeTestConfig(t, "client:\n  listenAddr: 127.0.0.1:59401\n")
	if _, _, err := Load(incomplete); err == nil || !strings.Contains(err.Error(), "client.dstAddr") {
		t.Fatalf("Load incomplete client error = %v", err)
	}
}

func TestResolveRole(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want Role
	}{
		{
			name: "client",
			cfg:  Config{Client: Client{ListenAddr: "127.0.0.1:59401", DstAddr: "203.0.113.20:59501"}},
			want: RoleClient,
		},
		{
			name: "server",
			cfg:  Config{Server: Server{ListenAddr: "0.0.0.0:59501"}},
			want: RoleServer,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			role, err := test.cfg.ResolveRole()
			if err != nil {
				t.Fatalf("ResolveRole returned error: %v", err)
			}
			if role != test.want {
				t.Fatalf("ResolveRole = %q, want %q", role, test.want)
			}
		})
	}
}

func TestResolveRoleRejectsAmbiguousOrPartialConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		message string
	}{
		{
			name: "both complete",
			cfg: Config{
				Client: Client{ListenAddr: "127.0.0.1:59401", DstAddr: "203.0.113.20:59501"},
				Server: Server{ListenAddr: "0.0.0.0:59501"},
			},
			message: "both client and server",
		},
		{
			name: "complete client and partial server",
			cfg: Config{
				Client: Client{ListenAddr: "127.0.0.1:59401", DstAddr: "203.0.113.20:59501"},
				Server: Server{PeerAuth: &ServerPeerAuth{Users: map[string]string{"edge-a": "secret"}}},
			},
			message: "server configuration is also present",
		},
		{
			name: "complete server and partial client",
			cfg: Config{
				Client: Client{PeerAuth: &Credentials{Username: "edge-a", Password: "secret"}},
				Server: Server{ListenAddr: "0.0.0.0:59501"},
			},
			message: "client configuration is also present",
		},
		{
			name: "complete server and include-only client",
			cfg: Config{
				Client: Client{IncludeInterfaces: []string{"eth*"}},
				Server: Server{ListenAddr: "0.0.0.0:59501"},
			},
			message: "client configuration is also present",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.cfg.ResolveRole(); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("ResolveRole error = %v, want %q", err, test.message)
			}
		})
	}
}

func TestResolveRoleReportsMissingFields(t *testing.T) {
	if _, err := (Config{Client: Client{ListenAddr: "127.0.0.1:59401"}}).ResolveRole(); err == nil || !strings.Contains(err.Error(), "client.dstAddr") {
		t.Fatalf("client missing field error = %v", err)
	}
	if _, err := (Config{Server: Server{AllowUnsafeDynamicDestination: true}}).ResolveRole(); err == nil || !strings.Contains(err.Error(), "server.listenAddr") {
		t.Fatalf("server missing field error = %v", err)
	}
	if _, err := (Config{Client: Client{IncludeInterfaces: []string{"eth*"}}}).ResolveRole(); err == nil || !strings.Contains(err.Error(), "client.listenAddr") || !strings.Contains(err.Error(), "client.dstAddr") {
		t.Fatalf("include-only client error = %v", err)
	}
	if _, err := (Config{}).ResolveRole(); err == nil || !strings.Contains(err.Error(), "no complete") {
		t.Fatalf("empty config error = %v", err)
	}

	if got := strings.Join(missingClientFields(Client{}), ","); got != "client.listenAddr,client.dstAddr" {
		t.Fatalf("missingClientFields = %q", got)
	}
	if got := strings.Join(missingServerFields(Server{}), ","); got != "server.listenAddr" {
		t.Fatalf("missingServerFields = %q", got)
	}
}

func TestLoadClientDefaults(t *testing.T) {
	path := writeTestConfig(t, "client:\n  listenAddr: 127.0.0.1:59401\n  dstAddr: 203.0.113.20:59501\n")
	cfg, role, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	assertTransferDefaults(t, cfg.Client.Transfer)
	if cfg.Client.SOCKS5AuthEnabled() || cfg.Client.PeerAuthEnabled() {
		t.Fatalf("unexpected client authentication defaults: %#v", cfg.Client)
	}
}

func TestClientInterfacePatternsYAMLRoundTrip(t *testing.T) {
	want := Client{
		IncludeInterfaces:  []string{"eth0", "enp[0-9]s0"},
		ExcludedInterfaces: []string{"br-*", "docker0"},
	}
	content, err := yaml.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "includeInterfaces:") || !strings.Contains(string(content), "excludedInterfaces:") {
		t.Fatalf("marshaled client is missing interface fields:\n%s", content)
	}

	var got Client
	if err := yaml.UnmarshalStrict(content, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.IncludeInterfaces, want.IncludeInterfaces) || !reflect.DeepEqual(got.ExcludedInterfaces, want.ExcludedInterfaces) {
		t.Fatalf("interface patterns = include %#v, exclude %#v; want include %#v, exclude %#v", got.IncludeInterfaces, got.ExcludedInterfaces, want.IncludeInterfaces, want.ExcludedInterfaces)
	}
}

func TestLoadClientInterfacePatterns(t *testing.T) {
	path := writeTestConfig(t, `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
  includeInterfaces:
    - eth0
    - enp[0-9]s0
  excludedInterfaces:
    - br-*
    - docker0
`)
	cfg, role, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	if got := strings.Join(cfg.Client.IncludeInterfaces, ","); got != "eth0,enp[0-9]s0" {
		t.Fatalf("includeInterfaces = %q", got)
	}
	if got := strings.Join(cfg.Client.ExcludedInterfaces, ","); got != "br-*,docker0" {
		t.Fatalf("excludedInterfaces = %q", got)
	}
}

func TestClientInterfacePatternValidation(t *testing.T) {
	base := Client{
		ListenAddr: "127.0.0.1:59401",
		DstAddr:    "203.0.113.20:59501",
		Transfer:   defaultTransfer(),
	}
	valid := []Client{
		func() Client { client := base; client.IncludeInterfaces = []string{"eth0", "enp*"}; return client }(),
		func() Client { client := base; client.ExcludedInterfaces = []string{"docker0", "br-*"}; return client }(),
	}
	for _, client := range valid {
		if err := (Config{Client: client}).Validate(RoleClient); err != nil {
			t.Fatalf("valid interface patterns %#v returned error: %v", client, err)
		}
	}

	tests := []struct {
		name    string
		field   string
		pattern string
		set     func(*Client, string)
	}{
		{name: "invalid include glob", field: "client.includeInterfaces", pattern: "br-[", set: func(client *Client, pattern string) { client.IncludeInterfaces = []string{pattern} }},
		{name: "empty include glob", field: "client.includeInterfaces", pattern: "", set: func(client *Client, pattern string) { client.IncludeInterfaces = []string{pattern} }},
		{name: "invalid exclude glob", field: "client.excludedInterfaces", pattern: "br-[", set: func(client *Client, pattern string) { client.ExcludedInterfaces = []string{pattern} }},
		{name: "empty exclude glob", field: "client.excludedInterfaces", pattern: "", set: func(client *Client, pattern string) { client.ExcludedInterfaces = []string{pattern} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := base
			test.set(&client, test.pattern)
			err := (Config{Client: client}).Validate(RoleClient)
			if err == nil || !strings.Contains(err.Error(), test.field) || !strings.Contains(err.Error(), `"`+test.pattern+`"`) {
				t.Fatalf("validation error = %v, want field %q and pattern %q", err, test.field, test.pattern)
			}
		})
	}
}

func TestLoadRejectsInvalidClientInterfacePatterns(t *testing.T) {
	const base = `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
`
	tests := []struct {
		name    string
		field   string
		pattern string
	}{
		{name: "invalid include glob", field: "includeInterfaces", pattern: "br-["},
		{name: "empty include glob", field: "includeInterfaces", pattern: ""},
		{name: "invalid exclude glob", field: "excludedInterfaces", pattern: "br-["},
		{name: "empty exclude glob", field: "excludedInterfaces", pattern: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := writeTestConfig(t, base+"  "+test.field+":\n    - \""+test.pattern+"\"\n")
			_, _, err := Load(configPath)
			fullField := "client." + test.field
			if err == nil || !strings.Contains(err.Error(), fullField) || !strings.Contains(err.Error(), `"`+test.pattern+`"`) {
				t.Fatalf("Load error = %v, want field %q and pattern %q", err, fullField, test.pattern)
			}
		})
	}
}

func TestLoadServerDefaults(t *testing.T) {
	path := writeTestConfig(t, "server:\n  listenAddr: 0.0.0.0:59501\n  allowedClients:\n    - 192.0.2.0/24\n")
	cfg, role, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleServer {
		t.Fatalf("role = %q, want %q", role, RoleServer)
	}
	assertTransferDefaults(t, cfg.Server.Transfer)
	if strings.Join(cfg.Server.AllowedClients, ",") != "192.0.2.0/24" {
		t.Fatalf("allowedClients = %#v", cfg.Server.AllowedClients)
	}
}

func assertTransferDefaults(t *testing.T, transfer Transfer) {
	t.Helper()
	if transfer.KeepaliveIntervalMillis != DefaultTransferKeepaliveIntervalMillis || transfer.KeepaliveTimeoutMillis != DefaultTransferKeepaliveTimeoutMillis {
		t.Fatalf("keepalive defaults = %#v", transfer)
	}
	tcp := transfer.TCP
	if tcp.CarrierMode != TCPCarrierModeRedundant ||
		tcp.ChunkSize != DefaultTCPChunkSize ||
		tcp.CarrierQueueBytes != DefaultTCPCarrierQueueBytes ||
		tcp.ReorderWindowBytes != DefaultTCPReorderWindowBytes ||
		tcp.DialTimeoutMillis != DefaultTCPDialTimeoutMillis ||
		tcp.OpenTimeoutMillis != DefaultTCPOpenTimeoutMillis ||
		tcp.WriteTimeoutMillis != DefaultTCPWriteTimeoutMillis {
		t.Fatalf("TCP defaults = %#v", tcp)
	}
	if tcp.MaxStreams != 0 || tcp.MaxCarriersPerStream != 0 || tcp.MaxPendingConnections != 0 || tcp.MaxPendingStreams != 0 || tcp.MaxSessions != 0 {
		t.Fatalf("TCP limit defaults = %#v, want unlimited", tcp)
	}
	if tcp.ClientRecoveryTimeoutMillis != 0 || tcp.ServerOrphanRetentionMillis != 0 || tcp.ResumeOpenTimeoutMillis != 0 || tcp.MaxConcurrentResumes != 0 || tcp.MaxPendingResumes != 0 || tcp.MaxRecoveringStreams != 0 || tcp.MaxRecoveryBytes != 0 {
		t.Fatalf("inactive active-standby defaults = %#v, want zero", tcp)
	}
}

func TestApplyDefaultsPreservesExplicitValues(t *testing.T) {
	transfer := Transfer{
		KeepaliveIntervalMillis: -1,
		KeepaliveTimeoutMillis:  -2,
		TCP: TCPTransfer{
			ChunkSize:          -3,
			DialTimeoutMillis:  -4,
			OpenTimeoutMillis:  -5,
			WriteTimeoutMillis: -6,
		},
	}
	transfer.ApplyDefaults()
	if transfer.KeepaliveIntervalMillis != -1 || transfer.KeepaliveTimeoutMillis != -2 ||
		transfer.TCP.ChunkSize != -3 || transfer.TCP.DialTimeoutMillis != -4 ||
		transfer.TCP.OpenTimeoutMillis != -5 || transfer.TCP.WriteTimeoutMillis != -6 {
		t.Fatalf("explicit values overwritten: %#v", transfer)
	}

	cfg := Config{}
	cfg.ApplyDefaults(Role("unknown"))
}

func TestLoadTCPTuning(t *testing.T) {
	path := writeTestConfig(t, `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
  transfer:
    keepaliveIntervalMillis: 250
    keepaliveTimeoutMillis: 1500
    tcp:
      chunkSize: 8192
      carrierQueueBytes: 524288
      reorderWindowBytes: 2097152
      dialTimeoutMillis: 2000
      openTimeoutMillis: 3000
      writeTimeoutMillis: 4000
      maxStreams: 1500
      maxCarriersPerStream: 10
      maxPendingConnections: 4096
      maxPendingStreams: 2048
      maxSessions: 256
`)
	cfg, role, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	transfer := cfg.Client.Transfer
	if transfer.KeepaliveIntervalMillis != 250 || transfer.KeepaliveTimeoutMillis != 1500 {
		t.Fatalf("keepalive tuning = %#v", transfer)
	}
	want := TCPTransfer{
		CarrierMode:           TCPCarrierModeRedundant,
		ChunkSize:             8192,
		CarrierQueueBytes:     524288,
		ReorderWindowBytes:    2097152,
		DialTimeoutMillis:     2000,
		OpenTimeoutMillis:     3000,
		WriteTimeoutMillis:    4000,
		MaxStreams:            1500,
		MaxCarriersPerStream:  10,
		MaxPendingConnections: 4096,
		MaxPendingStreams:     2048,
		MaxSessions:           256,
	}
	if transfer.TCP != want {
		t.Fatalf("TCP tuning = %#v, want %#v", transfer.TCP, want)
	}
}

func TestLoadActiveStandbyDefaults(t *testing.T) {
	path := writeTestConfig(t, `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
  interfaceHints:
    wwan0:
      cost: metered
  transfer:
    tcp:
      carrierMode: active-standby
`)
	cfg, role, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	client := cfg.Client
	if client.PathSelection != PathSelectionAdaptive {
		t.Fatalf("path selection = %q, want %q", client.PathSelection, PathSelectionAdaptive)
	}
	if client.InterfaceHints["wwan0"].Cost != InterfaceCostMetered {
		t.Fatalf("interface hints = %#v", client.InterfaceHints)
	}
	tcp := client.Transfer.TCP
	if !tcp.ActiveStandby() || tcp.ClientRecoveryTimeoutMillis != DefaultTCPClientRecoveryTimeoutMillis || tcp.ServerOrphanRetentionMillis != DefaultTCPServerOrphanRetentionMillis || tcp.ResumeOpenTimeoutMillis != DefaultTCPResumeOpenTimeoutMillis {
		t.Fatalf("active-standby timeout defaults = %#v", tcp)
	}
	if tcp.MaxStreams != DefaultTCPActiveStandbyMaxStreams || tcp.MaxConcurrentResumes != DefaultTCPMaxConcurrentResumes || tcp.MaxPendingResumes != DefaultTCPMaxPendingResumes || tcp.MaxRecoveringStreams != DefaultTCPMaxRecoveringStreams || tcp.MaxRecoveryBytes != DefaultTCPMaxRecoveryBytes {
		t.Fatalf("active-standby limit defaults = %#v", tcp)
	}
}

func TestActiveStandbyValidation(t *testing.T) {
	valid := defaultTransfer()
	valid.TCP.CarrierMode = TCPCarrierModeActiveStandby
	valid.TCP.ApplyDefaults()
	if err := valid.Validate("client"); err != nil {
		t.Fatalf("active-standby defaults validation error = %v", err)
	}

	tests := []struct {
		name string
		set  func(*TCPTransfer)
		want string
	}{
		{name: "unknown mode", set: func(tcp *TCPTransfer) { tcp.CarrierMode = "priority" }, want: "carrierMode"},
		{name: "recovery timeout", set: func(tcp *TCPTransfer) { tcp.ClientRecoveryTimeoutMillis = -1 }, want: "clientRecoveryTimeoutMillis"},
		{name: "resume timeout", set: func(tcp *TCPTransfer) { tcp.ResumeOpenTimeoutMillis = tcp.ClientRecoveryTimeoutMillis }, want: "resumeOpenTimeoutMillis"},
		{name: "retention", set: func(tcp *TCPTransfer) { tcp.ServerOrphanRetentionMillis = tcp.ClientRecoveryTimeoutMillis }, want: "serverOrphanRetentionMillis"},
		{name: "stream limit", set: func(tcp *TCPTransfer) { tcp.MaxStreams = 0 }, want: "active-standby limits"},
		{name: "resume concurrency", set: func(tcp *TCPTransfer) { tcp.MaxConcurrentResumes = 0 }, want: "active-standby limits"},
		{name: "pending resumes", set: func(tcp *TCPTransfer) { tcp.MaxPendingResumes = 0 }, want: "active-standby limits"},
		{name: "recovering streams", set: func(tcp *TCPTransfer) { tcp.MaxRecoveringStreams = 0 }, want: "active-standby limits"},
		{name: "recovery bytes", set: func(tcp *TCPTransfer) { tcp.MaxRecoveryBytes = int64(tcp.ReorderWindowBytes) - 1 }, want: "maxRecoveryBytes"},
		{name: "recovering streams above max streams", set: func(tcp *TCPTransfer) { tcp.MaxRecoveringStreams = tcp.MaxStreams + 1 }, want: "maxRecoveringStreams"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := valid
			test.set(&invalid.TCP)
			if err := invalid.Validate("client"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClientPathSelectionAndHintValidation(t *testing.T) {
	transfer := defaultTransfer()
	client := Client{ListenAddr: "127.0.0.1:59401", DstAddr: "203.0.113.20:59501", Transfer: transfer}
	client.PathSelection = "fixed"
	if err := (Config{Client: client}).Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "pathSelection") {
		t.Fatalf("path selection validation error = %v", err)
	}
	client.PathSelection = PathSelectionAdaptive
	client.InterfaceHints = map[string]InterfaceHint{"wwan0": {Cost: "cheap"}}
	if err := (Config{Client: client}).Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "interfaceHints.wwan0.cost") {
		t.Fatalf("interface hint validation error = %v", err)
	}
	client.InterfaceHints = map[string]InterfaceHint{"": {Cost: InterfaceCostNormal}}
	if err := (Config{Client: client}).Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "empty interface name") {
		t.Fatalf("empty interface hint validation error = %v", err)
	}
}

func TestTransferValidation(t *testing.T) {
	valid := defaultTransfer()
	if err := valid.Validate("client"); err != nil {
		t.Fatalf("default transfer validation error = %v", err)
	}

	type validationTest struct {
		name string
		set  func(*Transfer)
		want string
	}
	tests := []validationTest{
		{name: "carrier mode", set: func(value *Transfer) { value.TCP.CarrierMode = "fixed" }, want: "carrierMode"},
		{name: "keepalive interval", set: func(value *Transfer) { value.KeepaliveIntervalMillis = 0 }, want: "keepaliveIntervalMillis"},
		{name: "keepalive timeout", set: func(value *Transfer) { value.KeepaliveTimeoutMillis = value.KeepaliveIntervalMillis }, want: "keepaliveTimeoutMillis"},
		{name: "chunk size zero", set: func(value *Transfer) { value.TCP.ChunkSize = 0 }, want: "chunkSize"},
		{name: "chunk size too large", set: func(value *Transfer) { value.TCP.ChunkSize = 64*1024 + 1 }, want: "chunkSize"},
		{name: "carrier queue", set: func(value *Transfer) { value.TCP.CarrierQueueBytes = 0 }, want: "carrierQueueBytes"},
		{name: "reorder window", set: func(value *Transfer) { value.TCP.ReorderWindowBytes = 0 }, want: "reorderWindowBytes"},
		{name: "dial timeout", set: func(value *Transfer) { value.TCP.DialTimeoutMillis = 0 }, want: "dialTimeoutMillis"},
		{name: "open timeout", set: func(value *Transfer) { value.TCP.OpenTimeoutMillis = 0 }, want: "openTimeoutMillis"},
		{name: "write timeout", set: func(value *Transfer) { value.TCP.WriteTimeoutMillis = 0 }, want: "writeTimeoutMillis"},
		{name: "max streams", set: func(value *Transfer) { value.TCP.MaxStreams = -1 }, want: "maxStreams"},
		{name: "max carriers", set: func(value *Transfer) { value.TCP.MaxCarriersPerStream = -1 }, want: "maxCarriersPerStream"},
		{name: "max pending", set: func(value *Transfer) { value.TCP.MaxPendingConnections = -1 }, want: "maxPendingConnections"},
		{name: "max pending streams", set: func(value *Transfer) { value.TCP.MaxPendingStreams = -1 }, want: "maxPendingStreams"},
		{name: "max sessions", set: func(value *Transfer) { value.TCP.MaxSessions = -1 }, want: "maxSessions"},
		{name: "client recovery timeout", set: func(value *Transfer) { value.TCP.ClientRecoveryTimeoutMillis = -1 }, want: "clientRecoveryTimeoutMillis"},
		{name: "server orphan retention", set: func(value *Transfer) { value.TCP.ServerOrphanRetentionMillis = -1 }, want: "serverOrphanRetentionMillis"},
		{name: "resume open timeout", set: func(value *Transfer) { value.TCP.ResumeOpenTimeoutMillis = -1 }, want: "resumeOpenTimeoutMillis"},
		{name: "max concurrent resumes", set: func(value *Transfer) { value.TCP.MaxConcurrentResumes = -1 }, want: "maxConcurrentResumes"},
		{name: "max pending resumes", set: func(value *Transfer) { value.TCP.MaxPendingResumes = -1 }, want: "maxPendingResumes"},
		{name: "max recovering streams", set: func(value *Transfer) { value.TCP.MaxRecoveringStreams = -1 }, want: "maxRecoveringStreams"},
		{name: "max recovery bytes", set: func(value *Transfer) { value.TCP.MaxRecoveryBytes = -1 }, want: "maxRecoveryBytes"},
	}
	if strconv.IntSize > 32 {
		tooLarge := int64(MaxTCPSessionBufferBytes) + 1
		tests = append(tests,
			validationTest{name: "carrier queue too large", set: func(value *Transfer) { value.TCP.CarrierQueueBytes = int(tooLarge) }, want: "carrierQueueBytes"},
			validationTest{name: "reorder window too large", set: func(value *Transfer) { value.TCP.ReorderWindowBytes = int(tooLarge) }, want: "reorderWindowBytes"},
		)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := valid
			test.set(&invalid)
			if err := invalid.Validate("client"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClientSOCKS5ListenerSafety(t *testing.T) {
	transfer := defaultTransfer()
	cfg := Config{Client: Client{
		ListenAddr: "0.0.0.0:59401",
		DstAddr:    "203.0.113.20:59501",
		Transfer:   transfer,
	}}
	if err := cfg.Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("unsafe listener validation error = %v", err)
	}
	cfg.Client.AllowUnsafeFrontend = true
	if err := cfg.Validate(RoleClient); err != nil {
		t.Fatalf("explicit unsafe listener validation error = %v", err)
	}

	for _, address := range []string{"127.0.0.1:59401", "[::1]:59401", "localhost:59401"} {
		cfg.Client.ListenAddr = address
		cfg.Client.AllowUnsafeFrontend = false
		if err := cfg.Validate(RoleClient); err != nil {
			t.Fatalf("loopback address %q validation error = %v", address, err)
		}
	}
}

func TestAuthenticationConfiguration(t *testing.T) {
	transfer := defaultTransfer()
	clientConfig := Config{Client: Client{
		ListenAddr: "127.0.0.1:59401",
		DstAddr:    "203.0.113.20:59501",
		SOCKS5Auth: &Credentials{Username: "local-user", Password: "local-secret"},
		PeerAuth:   &Credentials{Username: "edge-a", Password: "peer-secret"},
		Transfer:   transfer,
	}}
	if err := clientConfig.Validate(RoleClient); err != nil {
		t.Fatalf("authenticated client validation error = %v", err)
	}
	if !clientConfig.Client.SOCKS5AuthEnabled() || !clientConfig.Client.PeerAuthEnabled() {
		t.Fatal("client authentication helpers did not report configured auth")
	}

	serverConfig := Config{Server: Server{
		ListenAddr: "0.0.0.0:59501",
		PeerAuth:   &ServerPeerAuth{Users: map[string]string{"edge-a": "peer-secret", "edge-b": "other-secret"}},
		Transfer:   transfer,
	}}
	if err := serverConfig.Validate(RoleServer); err != nil {
		t.Fatalf("authenticated server validation error = %v", err)
	}
	if !serverConfig.Server.PeerAuthEnabled() {
		t.Fatal("server authentication helper did not report configured auth")
	}
}

func TestAuthenticationConfigurationRejectsInvalidCredentials(t *testing.T) {
	transfer := defaultTransfer()
	clientConfig := Config{Client: Client{
		ListenAddr: "127.0.0.1:59401",
		DstAddr:    "203.0.113.20:59501",
		SOCKS5Auth: &Credentials{},
		Transfer:   transfer,
	}}
	if err := clientConfig.Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "socks5Auth.username") {
		t.Fatalf("empty SOCKS5 auth validation error = %v", err)
	}
	clientConfig.Client.SOCKS5Auth = &Credentials{Username: strings.Repeat("u", 256), Password: "secret"}
	if err := clientConfig.Validate(RoleClient); err == nil || !strings.Contains(err.Error(), "socks5Auth.username") {
		t.Fatalf("long SOCKS5 username validation error = %v", err)
	}

	serverConfig := Config{Server: Server{
		ListenAddr: "0.0.0.0:59501",
		PeerAuth:   &ServerPeerAuth{},
		Transfer:   transfer,
	}}
	if err := serverConfig.Validate(RoleServer); err == nil || !strings.Contains(err.Error(), "at least one user") {
		t.Fatalf("empty server peer auth validation error = %v", err)
	}
}

func TestWebManagerCredentialsMustBeComplete(t *testing.T) {
	transfer := defaultTransfer()
	tests := []struct {
		name string
		role Role
		cfg  Config
	}{
		{
			name: "client username only",
			role: RoleClient,
			cfg: Config{Client: Client{
				ListenAddr: "127.0.0.1:59401",
				DstAddr:    "203.0.113.20:59501",
				Transfer:   transfer,
				WebManager: WebManager{ListenAddr: "127.0.0.1:9001", Username: "engarde"},
			}},
		},
		{
			name: "server password only",
			role: RoleServer,
			cfg: Config{Server: Server{
				ListenAddr:                    "0.0.0.0:59501",
				AllowUnsafeDynamicDestination: true,
				Transfer:                      transfer,
				WebManager:                    WebManager{ListenAddr: "127.0.0.1:9001", Password: "secret"},
			}},
		},
		{
			name: "credentials without listener",
			role: RoleClient,
			cfg: Config{Client: Client{
				ListenAddr: "127.0.0.1:59401",
				DstAddr:    "203.0.113.20:59501",
				Transfer:   transfer,
				WebManager: WebManager{Username: "engarde", Password: "secret"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cfg.Validate(test.role); err == nil || !strings.Contains(err.Error(), "webManager") {
				t.Fatalf("incomplete web manager credentials error = %v", err)
			}
		})
	}

	for _, role := range []Role{RoleClient, RoleServer} {
		cfg := Config{
			Client: Client{
				ListenAddr: "127.0.0.1:59401",
				DstAddr:    "203.0.113.20:59501",
				Transfer:   transfer,
				WebManager: WebManager{ListenAddr: "127.0.0.1:9001", Username: "engarde", Password: "secret"},
			},
			Server: Server{
				ListenAddr:                    "0.0.0.0:59501",
				AllowUnsafeDynamicDestination: true,
				Transfer:                      transfer,
				WebManager:                    WebManager{ListenAddr: "127.0.0.1:9001", Username: "engarde", Password: "secret"},
			},
		}
		if err := cfg.Validate(role); err != nil {
			t.Fatalf("complete %s web manager credentials validation error = %v", role, err)
		}
	}
}

func TestServerRequiresAdmissionControl(t *testing.T) {
	transfer := defaultTransfer()
	base := Config{Server: Server{ListenAddr: "0.0.0.0:59501", Transfer: transfer}}
	if err := base.Validate(RoleServer); err == nil || !strings.Contains(err.Error(), "allowUnsafeDynamicDestination") {
		t.Fatalf("unrestricted server validation error = %v", err)
	}

	tests := []struct {
		name   string
		server Server
	}{
		{
			name:   "allowed clients",
			server: Server{ListenAddr: "0.0.0.0:59501", AllowedClients: []string{"192.0.2.0/24"}, Transfer: transfer},
		},
		{
			name:   "peer auth",
			server: Server{ListenAddr: "0.0.0.0:59501", PeerAuth: &ServerPeerAuth{Users: map[string]string{"edge-a": "secret"}}, Transfer: transfer},
		},
		{
			name:   "explicit unsafe override",
			server: Server{ListenAddr: "0.0.0.0:59501", AllowUnsafeDynamicDestination: true, Transfer: transfer},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := (Config{Server: test.server}).Validate(RoleServer); err != nil {
				t.Fatalf("server validation error = %v", err)
			}
		})
	}
}

func TestAllowedClientsValidation(t *testing.T) {
	valid := writeTestConfig(t, `server:
  listenAddr: 0.0.0.0:59501
  allowedClients:
    - 192.0.2.10
    - 198.51.100.0/24
`)
	cfg, role, err := Load(valid)
	if err != nil {
		t.Fatal(err)
	}
	if role != RoleServer || strings.Join(cfg.Server.AllowedClients, ",") != "192.0.2.10,198.51.100.0/24" {
		t.Fatalf("role/allowedClients = %q/%#v", role, cfg.Server.AllowedClients)
	}

	for _, value := range []string{"bad-cidr", ""} {
		path := writeTestConfig(t, "server:\n  listenAddr: 0.0.0.0:59501\n  allowedClients:\n    - \""+value+"\"\n")
		if _, _, err := Load(path); err == nil || !strings.Contains(err.Error(), "server.allowedClients") {
			t.Fatalf("invalid allowed client %q error = %v", value, err)
		}
	}
}

func TestLoadRejectsRemovedModeFields(t *testing.T) {
	clientBase := "client:\n  listenAddr: 127.0.0.1:59401\n  dstAddr: 203.0.113.20:59501\n"
	serverBase := "server:\n  listenAddr: 0.0.0.0:59501\n  allowUnsafeDynamicDestination: true\n"
	tests := []struct {
		name    string
		content string
		field   string
	}{
		{name: "raw frontend", content: clientBase + "  frontend: raw\n", field: "frontend"},
		{name: "SOCKS5 frontend selector", content: clientBase + "  frontend: socks5\n", field: "frontend"},
		{name: "mode", content: clientBase + "  transfer:\n    mode: direct\n", field: "mode"},
		{name: "protocol", content: clientBase + "  transfer:\n    protocol: tcp\n", field: "protocol"},
		{name: "adaptive ack timeout", content: clientBase + "  transfer:\n    ackTimeoutMillis: 50\n", field: "ackTimeoutMillis"},
		{name: "direct receive timeout", content: clientBase + "  transfer:\n    directReceiveTimeout: 10\n", field: "directReceiveTimeout"},
		{name: "pending window", content: clientBase + "  transfer:\n    pendingWindow: 128\n", field: "pendingWindow"},
		{name: "duplicate window", content: clientBase + "  transfer:\n    duplicateWindow: 256\n", field: "duplicateWindow"},
		{name: "max retries", content: clientBase + "  transfer:\n    maxRetries: 1\n", field: "maxRetries"},
		{name: "UDP batch", content: clientBase + "  udpBatch:\n    enabled: true\n", field: "udpBatch"},
		{name: "UDP write timeout", content: clientBase + "  writeTimeout: 10\n", field: "writeTimeout"},
		{name: "UDP relay queue", content: clientBase + "  relayQueueSize: 256\n", field: "relayQueueSize"},
		{name: "fixed server destination", content: serverBase + "  dstAddr: 127.0.0.1:59301\n", field: "dstAddr"},
		{name: "dynamic selector", content: serverBase + "  allowDynamicDestination: true\n", field: "allowDynamicDestination"},
		{name: "UDP client timeout", content: serverBase + "  clientTimeout: 30\n", field: "clientTimeout"},
		{name: "UDP max clients", content: serverBase + "  maxClients: 128\n", field: "maxClients"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestConfig(t, test.content)
			if _, _, err := Load(path); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Load removed field error = %v, want %q", err, test.field)
			}
		})
	}
}

func TestLoadRejectsUnknownAndRemovedTCPFields(t *testing.T) {
	tests := []struct {
		name    string
		content string
		field   string
	}{
		{
			name:    "misspelled peer auth",
			content: "client:\n  listenAddr: 127.0.0.1:59401\n  dstAddr: 203.0.113.20:59501\n  peerAut: {}\n",
			field:   "peerAut",
		},
		{
			name:    "removed standby limit",
			content: "client:\n  listenAddr: 127.0.0.1:59401\n  dstAddr: 203.0.113.20:59501\n  transfer:\n    tcp:\n      maxStandbyCarriers: 1024\n",
			field:   "maxStandbyCarriers",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestConfig(t, test.content)
			if _, _, err := Load(path); err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Load strict field error = %v, want %q", err, test.field)
			}
		})
	}
}

func TestLoadRejectsInactiveRoleEvenWhenFieldIsZero(t *testing.T) {
	path := writeTestConfig(t, `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
server:
  transfer:
    tcp:
      maxStreams: 0
`)
	if _, _, err := Load(path); err == nil || !strings.Contains(err.Error(), "server configuration is also present") {
		t.Fatalf("inactive zero field error = %v", err)
	}
}

func TestLoadRejectsDurationStringTCPTimeout(t *testing.T) {
	path := writeTestConfig(t, `client:
  listenAddr: 127.0.0.1:59401
  dstAddr: 203.0.113.20:59501
  transfer:
    tcp:
      writeTimeoutMillis: 10ms
`)
	if _, _, err := Load(path); err == nil || !strings.Contains(err.Error(), "cannot unmarshal") || !strings.Contains(err.Error(), "10ms") {
		t.Fatalf("duration TCP timeout error = %v", err)
	}
}

func TestSOCKS5ExamplesLoad(t *testing.T) {
	tests := []struct {
		path string
		role Role
	}{
		{path: filepath.Join("..", "..", "examples", "config", "tcp-socks5-client.yml"), role: RoleClient},
		{path: filepath.Join("..", "..", "examples", "config", "tcp-socks5-server.yml"), role: RoleServer},
		{path: filepath.Join("..", "..", "engarde.yml.sample"), role: RoleClient},
	}
	for _, test := range tests {
		t.Run(filepath.Base(test.path), func(t *testing.T) {
			_, role, err := Load(test.path)
			if err != nil {
				t.Fatalf("Load(%q) returned error: %v", test.path, err)
			}
			if role != test.role {
				t.Fatalf("Load(%q) role = %q, want %q", test.path, role, test.role)
			}
		})
	}
}
