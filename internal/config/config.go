package config

import (
	"errors"
	"fmt"
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

type Client struct {
	Description        string            `yaml:"description"`
	ListenAddr         string            `yaml:"listenAddr"`
	DstAddr            string            `yaml:"dstAddr"`
	WriteTimeout       int64             `yaml:"writeTimeout"`
	ExcludedInterfaces []string          `yaml:"excludedInterfaces"`
	InterfaceLabels    map[string]string `yaml:"interfaceLabels"`
	DstOverrides       []DstOverride     `yaml:"dstOverrides"`
	UDPBatch           UDPBatch          `yaml:"udpBatch"`
	WebManager         WebManager        `yaml:"webManager"`
}

type DstOverride struct {
	IfName  string `yaml:"ifName"`
	DstAddr string `yaml:"dstAddr"`
}

type Server struct {
	Description   string     `yaml:"description"`
	ListenAddr    string     `yaml:"listenAddr"`
	DstAddr       string     `yaml:"dstAddr"`
	WriteTimeout  int64      `yaml:"writeTimeout"`
	ClientTimeout int64      `yaml:"clientTimeout"`
	UDPBatch      UDPBatch   `yaml:"udpBatch"`
	WebManager    WebManager `yaml:"webManager"`
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
	case RoleServer:
		if cfg.Server.WriteTimeout == 0 {
			cfg.Server.WriteTimeout = 10
		}
		if cfg.Server.ClientTimeout == 0 {
			cfg.Server.ClientTimeout = 30
		}
		cfg.Server.UDPBatch.ApplyDefaults()
	}
}

func (cfg Config) clientPresent() bool {
	return cfg.Client.Description != "" || cfg.Client.ListenAddr != "" || cfg.Client.DstAddr != "" || cfg.Client.WriteTimeout != 0 || len(cfg.Client.ExcludedInterfaces) > 0 || len(cfg.Client.InterfaceLabels) > 0 || len(cfg.Client.DstOverrides) > 0 || cfg.Client.UDPBatch.present() || webPresent(cfg.Client.WebManager)
}

func (cfg Config) serverPresent() bool {
	return cfg.Server.Description != "" || cfg.Server.ListenAddr != "" || cfg.Server.DstAddr != "" || cfg.Server.WriteTimeout != 0 || cfg.Server.ClientTimeout != 0 || cfg.Server.UDPBatch.present() || webPresent(cfg.Server.WebManager)
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
