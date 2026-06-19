package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveRoleClientOnly(t *testing.T) {
	cfg := Config{
		Client: Client{ListenAddr: "127.0.0.1:59401", DstAddr: "1.2.3.4:59501"},
	}

	role, err := cfg.ResolveRole()
	if err != nil {
		t.Fatalf("ResolveRole returned error: %v", err)
	}
	if role != RoleClient {
		t.Fatalf("ResolveRole = %q, want %q", role, RoleClient)
	}
}

func TestResolveRoleReportsServerMissingFieldsAndEmptyConfig(t *testing.T) {
	_, err := Config{Server: Server{ListenAddr: "0.0.0.0:59501"}}.ResolveRole()
	if err == nil || !strings.Contains(err.Error(), "server.dstAddr") {
		t.Fatalf("server missing error = %v", err)
	}
	_, err = Config{}.ResolveRole()
	if err == nil || !strings.Contains(err.Error(), "no complete") {
		t.Fatalf("empty config error = %v", err)
	}
}

func TestLoadErrorsAndClientDefaults(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := Load(filepath.Join(dir, "missing.yml")); err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("Load missing file error = %v", err)
	}
	badYAML := filepath.Join(dir, "bad.yml")
	if err := os.WriteFile(badYAML, []byte("client: ["), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(badYAML); err == nil || !strings.Contains(err.Error(), "parse config") {
		t.Fatalf("Load bad yaml error = %v", err)
	}
	incompletePath := filepath.Join(dir, "incomplete.yml")
	if err := os.WriteFile(incompletePath, []byte("client:\n  listenAddr: 127.0.0.1:1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(incompletePath); err == nil || !strings.Contains(err.Error(), "client.dstAddr") {
		t.Fatalf("Load incomplete error = %v", err)
	}

	clientPath := filepath.Join(dir, "client.yml")
	if err := os.WriteFile(clientPath, []byte("client:\n  listenAddr: 127.0.0.1:1\n  dstAddr: 127.0.0.1:2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, role, err := Load(clientPath)
	if err != nil {
		t.Fatalf("Load client returned error: %v", err)
	}
	if role != RoleClient || cfg.Client.WriteTimeout != 10 {
		t.Fatalf("client role/defaults = %q/%v", role, cfg.Client.WriteTimeout)
	}
	if !cfg.Client.UDPBatch.IsEnabled() || cfg.Client.UDPBatch.ReadSize != DefaultUDPBatchReadSize || cfg.Client.UDPBatch.WriteSize != DefaultUDPBatchWriteSize {
		t.Fatalf("client udp batch defaults = %#v", cfg.Client.UDPBatch)
	}
	if cfg.Client.Transfer.Mode != TransferModeDirect || cfg.Client.Transfer.AckTimeoutMillis != DefaultTransferAckTimeoutMillis || cfg.Client.Transfer.PendingWindow != DefaultTransferPendingWindow {
		t.Fatalf("client transfer defaults = %#v", cfg.Client.Transfer)
	}
	if cfg.Client.Transfer.MaxRetriesValue() != DefaultTransferMaxRetries {
		t.Fatalf("client max retries default = %d", cfg.Client.Transfer.MaxRetriesValue())
	}
}

func TestMissingFieldHelpers(t *testing.T) {
	if got := strings.Join(missingClientFields(Client{}), ","); got != "client.listenAddr,client.dstAddr" {
		t.Fatalf("missingClientFields = %q", got)
	}
	if got := strings.Join(missingServerFields(Server{}), ","); got != "server.listenAddr,server.dstAddr" {
		t.Fatalf("missingServerFields = %q", got)
	}
}

func TestApplyDefaultsPreservesExplicitAndUnknown(t *testing.T) {
	cfg := Config{Client: Client{WriteTimeout: -1}, Server: Server{WriteTimeout: -1, ClientTimeout: -1}}
	cfg.ApplyDefaults(RoleClient)
	if cfg.Client.WriteTimeout != -1 {
		t.Fatalf("client WriteTimeout = %v, want -1", cfg.Client.WriteTimeout)
	}
	cfg.ApplyDefaults(RoleServer)
	if cfg.Server.WriteTimeout != -1 || cfg.Server.ClientTimeout != -1 {
		t.Fatalf("server defaults overwritten: %#v", cfg.Server)
	}
	cfg.ApplyDefaults(Role("unknown"))
}

func TestResolveRoleServerOnly(t *testing.T) {
	cfg := Config{
		Server: Server{ListenAddr: "0.0.0.0:59501", DstAddr: "127.0.0.1:59301"},
	}

	role, err := cfg.ResolveRole()
	if err != nil {
		t.Fatalf("ResolveRole returned error: %v", err)
	}
	if role != RoleServer {
		t.Fatalf("ResolveRole = %q, want %q", role, RoleServer)
	}
}

func TestResolveRoleRejectsTwoCompleteRoles(t *testing.T) {
	cfg := Config{
		Client: Client{ListenAddr: "127.0.0.1:59401", DstAddr: "1.2.3.4:59501"},
		Server: Server{ListenAddr: "0.0.0.0:59501", DstAddr: "127.0.0.1:59301"},
	}

	_, err := cfg.ResolveRole()
	if err == nil {
		t.Fatal("ResolveRole succeeded, want ambiguity error")
	}
	if !strings.Contains(err.Error(), "both client and server") {
		t.Fatalf("ResolveRole error = %q, want ambiguity message", err)
	}
}

func TestResolveRoleReportsMissingFields(t *testing.T) {
	cfg := Config{Client: Client{ListenAddr: "127.0.0.1:59401"}}

	_, err := cfg.ResolveRole()
	if err == nil {
		t.Fatal("ResolveRole succeeded, want missing field error")
	}
	if !strings.Contains(err.Error(), "client.dstAddr") {
		t.Fatalf("ResolveRole error = %q, want missing client.dstAddr", err)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("server:\n  listenAddr: \"0.0.0.0:59501\"\n  dstAddr: \"127.0.0.1:59301\"\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, role, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if role != RoleServer {
		t.Fatalf("role = %q, want %q", role, RoleServer)
	}
	if cfg.Server.WriteTimeout != 10 {
		t.Fatalf("WriteTimeout = %v, want 10", cfg.Server.WriteTimeout)
	}
	if cfg.Server.ClientTimeout != 30 {
		t.Fatalf("ClientTimeout = %v, want 30", cfg.Server.ClientTimeout)
	}
	if !cfg.Server.UDPBatch.IsEnabled() || cfg.Server.UDPBatch.ReadSize != DefaultUDPBatchReadSize || cfg.Server.UDPBatch.WriteSize != DefaultUDPBatchWriteSize {
		t.Fatalf("server udp batch defaults = %#v", cfg.Server.UDPBatch)
	}
	if cfg.Server.Transfer.Mode != TransferModeDirect || cfg.Server.Transfer.KeepaliveIntervalMillis != DefaultTransferKeepaliveIntervalMillis || cfg.Server.Transfer.DuplicateWindow != DefaultTransferDuplicateWindow {
		t.Fatalf("server transfer defaults = %#v", cfg.Server.Transfer)
	}
}

func TestLoadUDPBatchOverrides(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("client:\n  listenAddr: \"127.0.0.1:1\"\n  dstAddr: \"127.0.0.1:2\"\n  udpBatch:\n    enabled: false\n    readSize: 7\n    writeSize: 9\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, role, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	if cfg.Client.UDPBatch.IsEnabled() {
		t.Fatal("udp batch enabled after explicit false")
	}
	if cfg.Client.UDPBatch.EffectiveReadSize() != 7 || cfg.Client.UDPBatch.EffectiveWriteSize() != 9 {
		t.Fatalf("udp batch sizes = %#v", cfg.Client.UDPBatch)
	}
}

func TestUDPBatchDefaultHelpersNormalizeSizes(t *testing.T) {
	batch := UDPBatch{ReadSize: -1, WriteSize: 0}
	batch.ApplyDefaults()
	if !batch.IsEnabled() {
		t.Fatal("zero-value udp batch should default to enabled")
	}
	if batch.ReadSize != DefaultUDPBatchReadSize || batch.WriteSize != DefaultUDPBatchWriteSize {
		t.Fatalf("normalized udp batch = %#v", batch)
	}
}

func TestLoadTransferAdaptiveOverrides(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("client:\n  listenAddr: \"127.0.0.1:1\"\n  dstAddr: \"127.0.0.1:2\"\n  transfer:\n    mode: 2\n    ackTimeoutMillis: 25\n    keepaliveIntervalMillis: 500\n    keepaliveTimeoutMillis: 3000\n    pendingWindow: 128\n    duplicateWindow: 256\n    maxRetries: 3\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, role, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if role != RoleClient {
		t.Fatalf("role = %q, want %q", role, RoleClient)
	}
	transfer := cfg.Client.Transfer
	if !transfer.IsAdaptive() || transfer.Mode != TransferModeAdaptive || transfer.AckTimeoutMillis != 25 || transfer.KeepaliveIntervalMillis != 500 || transfer.KeepaliveTimeoutMillis != 3000 || transfer.PendingWindow != 128 || transfer.DuplicateWindow != 256 || transfer.MaxRetriesValue() != 3 {
		t.Fatalf("transfer overrides = %#v", transfer)
	}
}

func TestLoadTransferAllowsZeroMaxRetries(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("client:\n  listenAddr: \"127.0.0.1:1\"\n  dstAddr: \"127.0.0.1:2\"\n  transfer:\n    mode: adaptive\n    maxRetries: 0\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Client.Transfer.MaxRetriesValue() != 0 {
		t.Fatalf("maxRetries = %d, want 0", cfg.Client.Transfer.MaxRetriesValue())
	}
}

func TestLoadRejectsInvalidTransfer(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("server:\n  listenAddr: \"0.0.0.0:59501\"\n  dstAddr: \"127.0.0.1:59301\"\n  transfer:\n    mode: maybe\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := Load(configPath); err == nil || !strings.Contains(err.Error(), "server.transfer.mode") {
		t.Fatalf("Load invalid transfer error = %v", err)
	}
}

func TestLoadRejectsDurationStringWriteTimeout(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	content := []byte("server:\n  listenAddr: \"0.0.0.0:59501\"\n  dstAddr: \"127.0.0.1:59301\"\n  writeTimeout: 10ms\n")
	if err := os.WriteFile(configPath, content, 0600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := Load(configPath); err == nil || !strings.Contains(err.Error(), "cannot unmarshal") || !strings.Contains(err.Error(), "10ms") {
		t.Fatalf("Load duration writeTimeout error = %v, want duration parse error", err)
	}
}
