package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path"
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

	clientSectionPresent bool
	serverSectionPresent bool
}

const (
	DefaultTransferKeepaliveIntervalMillis int64 = 1000
	DefaultTransferKeepaliveTimeoutMillis  int64 = 5000
	DefaultTCPChunkSize                          = 16 * 1024
	DefaultTCPCarrierQueueBytes                  = 1024 * 1024
	DefaultTCPReorderWindowBytes                 = 4 * 1024 * 1024
	DefaultTCPDialTimeoutMillis            int64 = 5000
	DefaultTCPOpenTimeoutMillis            int64 = 5000
	DefaultTCPWriteTimeoutMillis           int64 = 10000
	MaxTCPSessionBufferBytes                     = 1<<31 - 1
)

type WebManager struct {
	ListenAddr string `yaml:"listenAddr"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
}

type Transfer struct {
	KeepaliveIntervalMillis int64       `yaml:"keepaliveIntervalMillis"`
	KeepaliveTimeoutMillis  int64       `yaml:"keepaliveTimeoutMillis"`
	TCP                     TCPTransfer `yaml:"tcp"`
}

type TCPTransfer struct {
	ChunkSize             int   `yaml:"chunkSize"`
	CarrierQueueBytes     int   `yaml:"carrierQueueBytes"`
	ReorderWindowBytes    int   `yaml:"reorderWindowBytes"`
	DialTimeoutMillis     int64 `yaml:"dialTimeoutMillis"`
	OpenTimeoutMillis     int64 `yaml:"openTimeoutMillis"`
	WriteTimeoutMillis    int64 `yaml:"writeTimeoutMillis"`
	MaxStreams            int   `yaml:"maxStreams"`
	MaxCarriersPerStream  int   `yaml:"maxCarriersPerStream"`
	MaxPendingConnections int   `yaml:"maxPendingConnections"`
	MaxPendingStreams     int   `yaml:"maxPendingStreams"`
	MaxSessions           int   `yaml:"maxSessions"`
}

func (transfer *Transfer) ApplyDefaults() {
	if transfer.KeepaliveIntervalMillis == 0 {
		transfer.KeepaliveIntervalMillis = DefaultTransferKeepaliveIntervalMillis
	}
	if transfer.KeepaliveTimeoutMillis == 0 {
		transfer.KeepaliveTimeoutMillis = DefaultTransferKeepaliveTimeoutMillis
	}
	transfer.TCP.ApplyDefaults()
}

func (tcp *TCPTransfer) ApplyDefaults() {
	if tcp.ChunkSize == 0 {
		tcp.ChunkSize = DefaultTCPChunkSize
	}
	if tcp.CarrierQueueBytes == 0 {
		tcp.CarrierQueueBytes = DefaultTCPCarrierQueueBytes
	}
	if tcp.ReorderWindowBytes == 0 {
		tcp.ReorderWindowBytes = DefaultTCPReorderWindowBytes
	}
	if tcp.DialTimeoutMillis == 0 {
		tcp.DialTimeoutMillis = DefaultTCPDialTimeoutMillis
	}
	if tcp.OpenTimeoutMillis == 0 {
		tcp.OpenTimeoutMillis = DefaultTCPOpenTimeoutMillis
	}
	if tcp.WriteTimeoutMillis == 0 {
		tcp.WriteTimeoutMillis = DefaultTCPWriteTimeoutMillis
	}
}

func (transfer Transfer) Validate(prefix string) error {
	if transfer.KeepaliveIntervalMillis <= 0 || transfer.KeepaliveTimeoutMillis <= transfer.KeepaliveIntervalMillis {
		return fmt.Errorf("%s.transfer requires positive keepaliveIntervalMillis and keepaliveTimeoutMillis greater than the interval", prefix)
	}
	return transfer.TCP.Validate(prefix)
}

func (tcp TCPTransfer) Validate(prefix string) error {
	if tcp.ChunkSize <= 0 || tcp.ChunkSize > 64*1024 {
		return fmt.Errorf("%s.transfer.tcp.chunkSize must be between 1 and 65536", prefix)
	}
	if tcp.CarrierQueueBytes <= 0 || tcp.CarrierQueueBytes > MaxTCPSessionBufferBytes {
		return fmt.Errorf("%s.transfer.tcp.carrierQueueBytes must be between 1 and %d", prefix, MaxTCPSessionBufferBytes)
	}
	if tcp.ReorderWindowBytes <= 0 || tcp.ReorderWindowBytes > MaxTCPSessionBufferBytes {
		return fmt.Errorf("%s.transfer.tcp.reorderWindowBytes must be between 1 and %d", prefix, MaxTCPSessionBufferBytes)
	}
	if tcp.DialTimeoutMillis <= 0 {
		return fmt.Errorf("%s.transfer.tcp.dialTimeoutMillis must be positive", prefix)
	}
	if tcp.OpenTimeoutMillis <= 0 {
		return fmt.Errorf("%s.transfer.tcp.openTimeoutMillis must be positive", prefix)
	}
	if tcp.WriteTimeoutMillis <= 0 {
		return fmt.Errorf("%s.transfer.tcp.writeTimeoutMillis must be positive", prefix)
	}
	if tcp.MaxStreams < 0 {
		return fmt.Errorf("%s.transfer.tcp.maxStreams must not be negative", prefix)
	}
	if tcp.MaxCarriersPerStream < 0 {
		return fmt.Errorf("%s.transfer.tcp.maxCarriersPerStream must not be negative", prefix)
	}
	if tcp.MaxPendingConnections < 0 {
		return fmt.Errorf("%s.transfer.tcp.maxPendingConnections must not be negative", prefix)
	}
	if tcp.MaxPendingStreams < 0 {
		return fmt.Errorf("%s.transfer.tcp.maxPendingStreams must not be negative", prefix)
	}
	if tcp.MaxSessions < 0 {
		return fmt.Errorf("%s.transfer.tcp.maxSessions must not be negative", prefix)
	}
	return nil
}

func (transfer Transfer) present() bool {
	return transfer.KeepaliveIntervalMillis != 0 || transfer.KeepaliveTimeoutMillis != 0 || transfer.TCP.present()
}

func (tcp TCPTransfer) present() bool {
	return tcp.ChunkSize != 0 || tcp.CarrierQueueBytes != 0 || tcp.ReorderWindowBytes != 0 || tcp.DialTimeoutMillis != 0 || tcp.OpenTimeoutMillis != 0 || tcp.WriteTimeoutMillis != 0 || tcp.MaxStreams != 0 || tcp.MaxCarriersPerStream != 0 || tcp.MaxPendingConnections != 0 || tcp.MaxPendingStreams != 0 || tcp.MaxSessions != 0
}

type Client struct {
	Description         string            `yaml:"description"`
	ListenAddr          string            `yaml:"listenAddr"`
	DstAddr             string            `yaml:"dstAddr"`
	SOCKS5Auth          *Credentials      `yaml:"socks5Auth"`
	PeerAuth            *Credentials      `yaml:"peerAuth"`
	AllowUnsafeFrontend bool              `yaml:"allowUnsafeFrontend"`
	IncludeInterfaces   []string          `yaml:"includeInterfaces"`
	ExcludedInterfaces  []string          `yaml:"excludedInterfaces"`
	InterfaceLabels     map[string]string `yaml:"interfaceLabels"`
	DstOverrides        []DstOverride     `yaml:"dstOverrides"`
	Transfer            Transfer          `yaml:"transfer"`
	WebManager          WebManager        `yaml:"webManager"`
}

type Credentials struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type ServerPeerAuth struct {
	Users map[string]string `yaml:"users"`
}

type DstOverride struct {
	IfName  string `yaml:"ifName"`
	DstAddr string `yaml:"dstAddr"`
}

type Server struct {
	Description                   string          `yaml:"description"`
	ListenAddr                    string          `yaml:"listenAddr"`
	AllowUnsafeDynamicDestination bool            `yaml:"allowUnsafeDynamicDestination"`
	AllowedClients                []string        `yaml:"allowedClients"`
	PeerAuth                      *ServerPeerAuth `yaml:"peerAuth"`
	Transfer                      Transfer        `yaml:"transfer"`
	WebManager                    WebManager      `yaml:"webManager"`
}

func (client Client) SOCKS5AuthEnabled() bool {
	return client.SOCKS5Auth != nil
}

func (client Client) PeerAuthEnabled() bool {
	return client.PeerAuth != nil
}

func (server Server) PeerAuthEnabled() bool {
	return server.PeerAuth != nil
}

func Load(filename string) (*Config, Role, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, "", fmt.Errorf("read config %q: %w", filename, err)
	}

	var cfg Config
	if err := yaml.UnmarshalStrict(content, &cfg); err != nil {
		return nil, "", fmt.Errorf("parse config %q: %w", filename, err)
	}
	var sections struct {
		Client map[interface{}]interface{} `yaml:"client"`
		Server map[interface{}]interface{} `yaml:"server"`
	}
	if err := yaml.Unmarshal(content, &sections); err != nil {
		return nil, "", fmt.Errorf("parse config %q: %w", filename, err)
	}
	cfg.clientSectionPresent = len(sections.Client) > 0
	cfg.serverSectionPresent = len(sections.Server) > 0

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
	serverComplete := cfg.Server.ListenAddr != ""

	switch {
	case clientComplete && serverComplete:
		return "", errors.New("both client and server configurations are complete; keep only one role in the config file")
	case clientComplete && cfg.serverPresent():
		return "", errors.New("client configuration is complete but server configuration is also present; keep only one role in the config file")
	case serverComplete && cfg.clientPresent():
		return "", errors.New("server configuration is complete but client configuration is also present; keep only one role in the config file")
	case clientComplete:
		return RoleClient, nil
	case serverComplete:
		return RoleServer, nil
	}

	missing := make([]string, 0, 3)
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
		cfg.Client.Transfer.ApplyDefaults()
	case RoleServer:
		cfg.Server.Transfer.ApplyDefaults()
	}
}

func (cfg Config) Validate(role Role) error {
	switch role {
	case RoleClient:
		if err := cfg.Client.Transfer.Validate("client"); err != nil {
			return err
		}
		if err := validateWebManager("client.webManager", cfg.Client.WebManager); err != nil {
			return err
		}
		if cfg.Client.SOCKS5Auth != nil {
			if err := validateCredentials("client.socks5Auth", *cfg.Client.SOCKS5Auth, 255); err != nil {
				return err
			}
		}
		if cfg.Client.PeerAuth != nil {
			if err := validateCredentials("client.peerAuth", *cfg.Client.PeerAuth, 1024); err != nil {
				return err
			}
		}
		if err := validateInterfacePatterns("client.includeInterfaces", cfg.Client.IncludeInterfaces); err != nil {
			return err
		}
		if err := validateInterfacePatterns("client.excludedInterfaces", cfg.Client.ExcludedInterfaces); err != nil {
			return err
		}
		if !cfg.Client.AllowUnsafeFrontend && !isLoopbackListenAddress(cfg.Client.ListenAddr) {
			return errors.New("client SOCKS5 frontend requires a loopback listenAddr unless allowUnsafeFrontend is true")
		}
		return nil
	case RoleServer:
		if err := cfg.Server.Transfer.Validate("server"); err != nil {
			return err
		}
		if err := validateWebManager("server.webManager", cfg.Server.WebManager); err != nil {
			return err
		}
		if err := validateAllowedClients(cfg.Server.AllowedClients); err != nil {
			return err
		}
		if cfg.Server.PeerAuth != nil {
			if err := validateServerPeerAuth(*cfg.Server.PeerAuth); err != nil {
				return err
			}
		}
		if len(cfg.Server.AllowedClients) == 0 && cfg.Server.PeerAuth == nil && !cfg.Server.AllowUnsafeDynamicDestination {
			return errors.New("server requires allowedClients, peerAuth, or allowUnsafeDynamicDestination")
		}
		return nil
	}
	return nil
}

func validateWebManager(prefix string, web WebManager) error {
	if web.ListenAddr == "" && (web.Username != "" || web.Password != "") {
		return fmt.Errorf("%s.listenAddr is required when credentials are configured", prefix)
	}
	if (web.Username == "") != (web.Password == "") {
		return fmt.Errorf("%s.username and %s.password must be configured together", prefix, prefix)
	}
	return nil
}

func validateCredentials(prefix string, credentials Credentials, maxPasswordBytes int) error {
	usernameBytes := len([]byte(credentials.Username))
	if usernameBytes == 0 || usernameBytes > 255 {
		return fmt.Errorf("%s.username must contain between 1 and 255 bytes", prefix)
	}
	passwordBytes := len([]byte(credentials.Password))
	if passwordBytes == 0 || passwordBytes > maxPasswordBytes {
		return fmt.Errorf("%s.password must contain between 1 and %d bytes", prefix, maxPasswordBytes)
	}
	return nil
}

func validateServerPeerAuth(peerAuth ServerPeerAuth) error {
	if len(peerAuth.Users) == 0 {
		return errors.New("server.peerAuth.users must contain at least one user")
	}
	for username, password := range peerAuth.Users {
		if err := validateCredentials("server.peerAuth.users", Credentials{Username: username, Password: password}, 1024); err != nil {
			return err
		}
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

func validateInterfacePatterns(field string, patterns []string) error {
	for _, pattern := range patterns {
		if pattern == "" {
			return fmt.Errorf("%s contains invalid pattern %q: pattern must not be empty", field, pattern)
		}
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("%s contains invalid pattern %q: %w", field, pattern, err)
		}
	}
	return nil
}

func isLoopbackListenAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (cfg Config) clientPresent() bool {
	return cfg.clientSectionPresent || cfg.Client.Description != "" || cfg.Client.ListenAddr != "" || cfg.Client.DstAddr != "" || cfg.Client.SOCKS5Auth != nil || cfg.Client.PeerAuth != nil || cfg.Client.AllowUnsafeFrontend || len(cfg.Client.IncludeInterfaces) > 0 || len(cfg.Client.ExcludedInterfaces) > 0 || len(cfg.Client.InterfaceLabels) > 0 || len(cfg.Client.DstOverrides) > 0 || cfg.Client.Transfer.present() || webPresent(cfg.Client.WebManager)
}

func (cfg Config) serverPresent() bool {
	return cfg.serverSectionPresent || cfg.Server.Description != "" || cfg.Server.ListenAddr != "" || cfg.Server.AllowUnsafeDynamicDestination || len(cfg.Server.AllowedClients) > 0 || cfg.Server.PeerAuth != nil || cfg.Server.Transfer.present() || webPresent(cfg.Server.WebManager)
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
	if server.ListenAddr == "" {
		return []string{"server.listenAddr"}
	}
	return nil
}
