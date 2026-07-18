package serverrole

import (
	"context"
	"io/fs"
	"net"
	"strings"
	"sync"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	log "github.com/sirupsen/logrus"
)

var runControl = control.Run

type Server struct {
	cfg     config.Server
	version string
	webFS   fs.FS

	allowedClientIPs  []net.IP
	allowedClientNets []*net.IPNet

	tcpRuntimeMu sync.RWMutex
	tcpRuntime   *tcpServerRuntime
}

func New(cfg config.Server, version string, webFS fs.FS) *Server {
	cfg.Transfer.ApplyDefaults()
	allowedClientIPs, allowedClientNets := parseAllowedClients(cfg.AllowedClients)
	return &Server{
		cfg:               cfg,
		version:           version,
		webFS:             webFS,
		allowedClientIPs:  allowedClientIPs,
		allowedClientNets: allowedClientNets,
	}
}

func (server *Server) Run(ctx context.Context) error {
	if server.cfg.Description != "" {
		log.Info(server.cfg.Description)
	}
	return server.runTCP(ctx)
}

func (server *Server) Status() (any, error) {
	if runtime := server.getTCPRuntime(); runtime != nil {
		return runtime.status(), nil
	}
	return control.ServerStatus{
		Type:            "server",
		Version:         server.version,
		Description:     server.cfg.Description,
		ListenAddress:   server.cfg.ListenAddr,
		PeerAuthEnabled: server.cfg.PeerAuthEnabled(),
		Sockets:         []control.WebSocket{},
	}, nil
}

func (server *Server) setTCPRuntime(runtime *tcpServerRuntime) {
	server.tcpRuntimeMu.Lock()
	server.tcpRuntime = runtime
	server.tcpRuntimeMu.Unlock()
}

func (server *Server) getTCPRuntime() *tcpServerRuntime {
	server.tcpRuntimeMu.RLock()
	defer server.tcpRuntimeMu.RUnlock()
	return server.tcpRuntime
}

func parseAllowedClients(values []string) ([]net.IP, []*net.IPNet) {
	ips := make([]net.IP, 0, len(values))
	networks := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ip := net.ParseIP(value); ip != nil {
			ips = append(ips, ip)
			continue
		}
		if _, network, err := net.ParseCIDR(value); err == nil {
			networks = append(networks, network)
		}
	}
	return ips, networks
}

func (server *Server) clientIPAllowed(ip net.IP) bool {
	if len(server.allowedClientIPs) == 0 && len(server.allowedClientNets) == 0 {
		return true
	}
	for _, allowedIP := range server.allowedClientIPs {
		if allowedIP.Equal(ip) {
			return true
		}
	}
	for _, allowedNet := range server.allowedClientNets {
		if allowedNet.Contains(ip) {
			return true
		}
	}
	return false
}
