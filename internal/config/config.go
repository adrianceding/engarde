package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

type Role string

const (
	RoleClient Role = "client"
	RoleServer Role = "server"
)

type Config struct {
	Client Client `yaml:"client"`
	Server Server `yaml:"server"`
}

const (
	DefaultUDPBatchReadSize  = 32
	DefaultUDPBatchWriteSize = 32
	DefaultServerMaxClients  = 128
)

const (
	TransferModeDirect   = "direct"
	TransferModeAdaptive = "adaptive"

	DefaultTransferAckTimeoutMillis        int64 = 50
	DefaultTransferKeepaliveIntervalMillis int64 = 1000
	DefaultTransferKeepaliveTimeoutMillis  int64 = 5000
	DefaultTransferPendingWindow                 = 4096
	DefaultTransferDuplicateWindow               = 8192
	DefaultTransferMaxRetries                    = 1
)

type UDPBatch struct {
	Enabled   *bool `yaml:"enabled"`
	ReadSize  int   `yaml:"readSize"`
	WriteSize int   `yaml:"writeSize"`
}

func (batch UDPBatch) IsEnabled() bool {
	return batch.Enabled == nil || *batch.Enabled
}

func (batch UDPBatch) EffectiveReadSize() int {
	if batch.ReadSize > 0 {
		return batch.ReadSize
	}
	return DefaultUDPBatchReadSize
}

func (batch UDPBatch) EffectiveWriteSize() int {
	if batch.WriteSize > 0 {
		return batch.WriteSize
	}
	return DefaultUDPBatchWriteSize
}

func (batch *UDPBatch) ApplyDefaults() {
	if batch.ReadSize <= 0 {
		batch.ReadSize = DefaultUDPBatchReadSize
	}
	if batch.WriteSize <= 0 {
		batch.WriteSize = DefaultUDPBatchWriteSize
	}
}

func (batch UDPBatch) present() bool {
	return batch.Enabled != nil || batch.ReadSize != 0 || batch.WriteSize != 0
}

type WebManager struct {
	ListenAddr string `yaml:"listenAddr"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
}

type Transfer struct {
	Mode                    string `yaml:"mode"`
	AckTimeoutMillis        int64  `yaml:"ackTimeoutMillis"`
	KeepaliveIntervalMillis int64  `yaml:"keepaliveIntervalMillis"`
	KeepaliveTimeoutMillis  int64  `yaml:"keepaliveTimeoutMillis"`
	DirectReceiveTimeout    int64  `yaml:"directReceiveTimeout"`
	PendingWindow           int    `yaml:"pendingWindow"`
	DuplicateWindow         int    `yaml:"duplicateWindow"`
	MaxRetries              *int   `yaml:"maxRetries"`
}

func (transfer *Transfer) ApplyDefaults() {
	transfer.Mode = normalizeTransferMode(transfer.Mode)
	if transfer.Mode == "" {
		transfer.Mode = TransferModeDirect
	}
	if transfer.AckTimeoutMillis == 0 {
		transfer.AckTimeoutMillis = DefaultTransferAckTimeoutMillis
	}
	if transfer.KeepaliveIntervalMillis == 0 {
		transfer.KeepaliveIntervalMillis = DefaultTransferKeepaliveIntervalMillis
	}
	if transfer.KeepaliveTimeoutMillis == 0 {
		transfer.KeepaliveTimeoutMillis = DefaultTransferKeepaliveTimeoutMillis
	}
	if transfer.PendingWindow == 0 {
		transfer.PendingWindow = DefaultTransferPendingWindow
	}
	if transfer.DuplicateWindow == 0 {
		transfer.DuplicateWindow = DefaultTransferDuplicateWindow
	}
	if transfer.MaxRetries == nil {
		maxRetries := DefaultTransferMaxRetries
		transfer.MaxRetries = &maxRetries
	}
}

func (transfer Transfer) Validate(prefix string) error {
	if transfer.Mode != TransferModeDirect && transfer.Mode != TransferModeAdaptive {
		return fmt.Errorf("invalid %s.transfer.mode %q", prefix, transfer.Mode)
	}
	if transfer.AckTimeoutMillis < 0 {
		return fmt.Errorf("%s.transfer.ackTimeoutMillis must not be negative", prefix)
	}
	if transfer.KeepaliveIntervalMillis < 0 {
		return fmt.Errorf("%s.transfer.keepaliveIntervalMillis must not be negative", prefix)
	}
	if transfer.KeepaliveTimeoutMillis < 0 {
		return fmt.Errorf("%s.transfer.keepaliveTimeoutMillis must not be negative", prefix)
	}
	if transfer.DirectReceiveTimeout < 0 {
		return fmt.Errorf("%s.transfer.directReceiveTimeout must not be negative", prefix)
	}
	if transfer.PendingWindow < 0 {
		return fmt.Errorf("%s.transfer.pendingWindow must not be negative", prefix)
	}
	if transfer.DuplicateWindow < 0 {
		return fmt.Errorf("%s.transfer.duplicateWindow must not be negative", prefix)
	}
	if transfer.MaxRetries == nil {
		return fmt.Errorf("%s.transfer.maxRetries must be set", prefix)
	}
	if *transfer.MaxRetries < 0 {
		return fmt.Errorf("%s.transfer.maxRetries must not be negative", prefix)
	}
	return nil
}

func (transfer Transfer) IsAdaptive() bool {
	return transfer.Mode == TransferModeAdaptive
}

func (transfer Transfer) MaxRetriesValue() int {
	if transfer.MaxRetries == nil {
		return DefaultTransferMaxRetries
	}
	return *transfer.MaxRetries
}

func (transfer Transfer) present() bool {
	return transfer.Mode != "" || transfer.AckTimeoutMillis != 0 || transfer.KeepaliveIntervalMillis != 0 || transfer.KeepaliveTimeoutMillis != 0 || transfer.DirectReceiveTimeout != 0 || transfer.PendingWindow != 0 || transfer.DuplicateWindow != 0 || transfer.MaxRetries != nil
}

func normalizeTransferMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", TransferModeDirect, "1", "mode1", "redundant":
		return TransferModeDirect
	case TransferModeAdaptive, "2", "mode2":
		return TransferModeAdaptive
	default:
		return mode
	}
}

type Client struct {
	Description        string            `yaml:"description"`
	ListenAddr         string            `yaml:"listenAddr"`
	DstAddr            string            `yaml:"dstAddr"`
	WriteTimeout       int64             `yaml:"writeTimeout"`
	ExcludedInterfaces []string          `yaml:"excludedInterfaces"`
	InterfaceLabels    map[string]string `yaml:"interfaceLabels"`
	DstOverrides       []DstOverride     `yaml:"dstOverrides"`
	UDPBatch           UDPBatch          `yaml:"udpBatch"`
	Transfer           Transfer          `yaml:"transfer"`
	WebManager         WebManager        `yaml:"webManager"`
}

type DstOverride struct {
	IfName  string `yaml:"ifName"`
	DstAddr string `yaml:"dstAddr"`
}

type Server struct {
	Description    string     `yaml:"description"`
	ListenAddr     string     `yaml:"listenAddr"`
	DstAddr        string     `yaml:"dstAddr"`
	WriteTimeout   int64      `yaml:"writeTimeout"`
	ClientTimeout  int64      `yaml:"clientTimeout"`
	AllowedClients []string   `yaml:"allowedClients"`
	MaxClients     *int       `yaml:"maxClients"`
	UDPBatch       UDPBatch   `yaml:"udpBatch"`
	Transfer       Transfer   `yaml:"transfer"`
	WebManager     WebManager `yaml:"webManager"`
}

func (server *Server) ApplyDefaults() {
	if server.MaxClients == nil {
		maxClients := DefaultServerMaxClients
		server.MaxClients = &maxClients
	}
}

func (server Server) MaxClientsValue() int {
	if server.MaxClients == nil {
		return DefaultServerMaxClients
	}
	return *server.MaxClients
}

func Load(filename string) (*Config, Role, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, "", fmt.Errorf("read config %q: %w", filename, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return nil, "", fmt.Errorf("parse config %q: %w", filename, err)
	}

	role, err := cfg.ResolveRole()
	if err != nil {
		return nil, "", err
	}
	cfg.ApplyDefaults(role)
	if err := cfg.Validate(role); err != nil {
		return nil, "", err
	}
	return &cfg, role, nil
}

func (cfg Config) ResolveRole() (Role, error) {
	clientComplete := cfg.Client.ListenAddr != "" && cfg.Client.DstAddr != ""
	serverComplete := cfg.Server.ListenAddr != "" && cfg.Server.DstAddr != ""

	switch {
	case clientComplete && serverComplete:
		return "", errors.New("both client and server configurations are complete; keep only one role in the config file")
	case clientComplete:
		return RoleClient, nil
	case serverComplete:
		return RoleServer, nil
	}

	missing := make([]string, 0, 4)
	if cfg.clientPresent() {
		missing = append(missing, missingClientFields(cfg.Client)...)
	}
	if cfg.serverPresent() {
		missing = append(missing, missingServerFields(cfg.Server)...)
	}
	if len(missing) == 0 {
		return "", errors.New("no complete client or server configuration found")
	}
	return "", fmt.Errorf("no complete client or server configuration found; missing %s", strings.Join(missing, ", "))
}

func (cfg *Config) ApplyDefaults(role Role) {
	switch role {
	case RoleClient:
		if cfg.Client.WriteTimeout == 0 {
			cfg.Client.WriteTimeout = 10
		}
		cfg.Client.UDPBatch.ApplyDefaults()
		cfg.Client.Transfer.ApplyDefaults()
	case RoleServer:
		if cfg.Server.WriteTimeout == 0 {
			cfg.Server.WriteTimeout = 10
		}
		if cfg.Server.ClientTimeout == 0 {
			cfg.Server.ClientTimeout = 30
		}
		cfg.Server.ApplyDefaults()
		cfg.Server.UDPBatch.ApplyDefaults()
		cfg.Server.Transfer.ApplyDefaults()
	}
}

func (cfg Config) Validate(role Role) error {
	switch role {
	case RoleClient:
		return cfg.Client.Transfer.Validate("client")
	case RoleServer:
		if err := cfg.Server.Transfer.Validate("server"); err != nil {
			return err
		}
		if cfg.Server.MaxClients == nil {
			return errors.New("server.maxClients must be set")
		}
		if *cfg.Server.MaxClients < 0 {
			return errors.New("server.maxClients must not be negative")
		}
		return validateAllowedClients(cfg.Server.AllowedClients)
	}
	return nil
}

func validateAllowedClients(values []string) error {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return errors.New("server.allowedClients contains an empty entry")
		}
		if ip := net.ParseIP(value); ip != nil {
			continue
		}
		if _, _, err := net.ParseCIDR(value); err == nil {
			continue
		}
		return fmt.Errorf("server.allowedClients contains invalid IP/CIDR %q", value)
	}
	return nil
}

func (cfg Config) clientPresent() bool {
	return cfg.Client.Description != "" || cfg.Client.ListenAddr != "" || cfg.Client.DstAddr != "" || cfg.Client.WriteTimeout != 0 || len(cfg.Client.ExcludedInterfaces) > 0 || len(cfg.Client.InterfaceLabels) > 0 || len(cfg.Client.DstOverrides) > 0 || cfg.Client.UDPBatch.present() || cfg.Client.Transfer.present() || webPresent(cfg.Client.WebManager)
}

func (cfg Config) serverPresent() bool {
	return cfg.Server.Description != "" || cfg.Server.ListenAddr != "" || cfg.Server.DstAddr != "" || cfg.Server.WriteTimeout != 0 || cfg.Server.ClientTimeout != 0 || len(cfg.Server.AllowedClients) > 0 || cfg.Server.MaxClients != nil || cfg.Server.UDPBatch.present() || cfg.Server.Transfer.present() || webPresent(cfg.Server.WebManager)
}

func webPresent(web WebManager) bool {
	return web.ListenAddr != "" || web.Username != "" || web.Password != ""
}

func missingClientFields(client Client) []string {
	missing := make([]string, 0, 2)
	if client.ListenAddr == "" {
		missing = append(missing, "client.listenAddr")
	}
	if client.DstAddr == "" {
		missing = append(missing, "client.dstAddr")
	}
	return missing
}

func missingServerFields(server Server) []string {
	missing := make([]string, 0, 2)
	if server.ListenAddr == "" {
		missing = append(missing, "server.listenAddr")
	}
	if server.DstAddr == "" {
		missing = append(missing, "server.dstAddr")
	}
	return missing
}
