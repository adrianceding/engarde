package clientrole

import (
	"context"
	"io/fs"
	"net"
	"sync"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	pathmgr "github.com/adrianceding/engarde/internal/path"
	log "github.com/sirupsen/logrus"
)

var runControl = control.Run
var newRefreshTicker = defaultRefreshTicker

func defaultRefreshTicker() (<-chan time.Time, func()) {
	ticker := time.NewTicker(time.Second)
	return ticker.C, ticker.Stop
}

type Client struct {
	cfg     config.Client
	version string
	webFS   fs.FS
	paths   *pathmgr.Manager

	listInterfaces   func() ([]net.Interface, error)
	interfaceAddress func(net.Interface) string

	tcpRuntimeMu sync.RWMutex
	tcpRuntime   *tcpClientRuntime
}

func New(cfg config.Client, version string, webFS fs.FS) *Client {
	cfg.Transfer.ApplyDefaults()
	return &Client{
		cfg:              cfg,
		version:          version,
		webFS:            webFS,
		paths:            pathmgr.NewManager(cfg),
		listInterfaces:   net.Interfaces,
		interfaceAddress: pathmgr.AddressByInterface,
	}
}

func (client *Client) Run(ctx context.Context) error {
	if client.cfg.Description != "" {
		log.Info(client.cfg.Description)
	}
	return client.runTCP(ctx)
}

func (client *Client) Status() (any, error) {
	if runtime := client.getTCPRuntime(); runtime != nil {
		return runtime.status()
	}
	return control.ClientStatus{
		Type:                "client",
		Version:             client.version,
		Description:         client.cfg.Description,
		ListenAddress:       client.cfg.ListenAddr,
		FrontendAuthEnabled: client.cfg.SOCKS5AuthEnabled(),
		PeerAuthEnabled:     client.cfg.PeerAuthEnabled(),
		Interfaces:          []control.WebInterface{},
	}, nil
}

func (client *Client) setTCPRuntime(runtime *tcpClientRuntime) {
	client.tcpRuntimeMu.Lock()
	client.tcpRuntime = runtime
	client.tcpRuntimeMu.Unlock()
}

func (client *Client) getTCPRuntime() *tcpClientRuntime {
	client.tcpRuntimeMu.RLock()
	defer client.tcpRuntimeMu.RUnlock()
	return client.tcpRuntime
}

func (client *Client) ToggleOverride(ifName string) string {
	return client.paths.ToggleExclusion(ifName)
}

func (client *Client) Include(ifName string) string {
	return client.paths.Include(ifName)
}

func (client *Client) Exclude(ifName string) string {
	return client.paths.Exclude(ifName)
}

func (client *Client) ResetExclusions() string {
	return client.paths.ResetExclusions()
}
