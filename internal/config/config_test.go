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
