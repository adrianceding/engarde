package clientrole

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/control"
	pathmgr "github.com/adrianceding/engarde/internal/path"
	"github.com/adrianceding/engarde/internal/relay"
	"github.com/adrianceding/engarde/internal/udp"
	log "github.com/sirupsen/logrus"
)

var runControl = control.Run
var resolveUDPAddr = net.ResolveUDPAddr
var listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
	return net.ListenUDP(network, laddr)
}
var newRefreshTicker = defaultRefreshTicker

type udpSocket interface {
	relay.UDPWriter
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	Close() error
}

func defaultRefreshTicker() (<-chan time.Time, func()) {
	ticker := time.NewTicker(time.Second)
	return ticker.C, ticker.Stop
}

type Client struct {
	cfg     config.Client
	version string
	webFS   fs.FS
	paths   *pathmgr.Manager

	listInterfaces     func() ([]net.Interface, error)
	interfaceAddress   func(net.Interface) string
	openUDPOnInterface func(*net.UDPAddr, string) (udpSocket, error)

	wgSocket udpSocket

	wgAddrMu sync.RWMutex
	wgAddr   *net.UDPAddr

	routesMu sync.RWMutex
	routes   map[string]*sendRoute

	dispatcher *relay.Dispatcher
}

type sendRoute struct {
	ifName  string
	srcAddr string
	dstAddr *net.UDPAddr
	socket  udpSocket
	lastRec atomic.Int64
	closing atomic.Bool
}

func New(cfg config.Client, version string, webFS fs.FS) *Client {
	client := &Client{
		cfg:              cfg,
		version:          version,
		webFS:            webFS,
		paths:            pathmgr.NewManager(cfg),
		routes:           make(map[string]*sendRoute),
		listInterfaces:   net.Interfaces,
		interfaceAddress: pathmgr.AddressByInterface,
		openUDPOnInterface: func(addr *net.UDPAddr, ifName string) (udpSocket, error) {
			return udp.OpenUDPOnInterface(addr, ifName)
		},
	}
	client.dispatcher = relay.NewDispatcher(cfg.WriteTimeout, relay.DefaultQueueSize, func(result relay.Result) {
		log.WithError(result.Err).Warn("Error writing to '" + result.ID + "', re-creating socket")
		client.removeRoute(result.ID)
	})
	return client
}

func (client *Client) Run(ctx context.Context) error {
	if client.cfg.Description != "" {
		log.Info(client.cfg.Description)
	}

	listenAddr, err := resolveUDPAddr("udp4", client.cfg.ListenAddr)
	if err != nil {
		return err
	}
	wgSocket, err := listenUDP("udp", listenAddr)
	if err != nil {
		return err
	}
	client.wgSocket = wgSocket
	client.updateWireGuardWriteBuffer()
	log.Info("Listening on " + client.cfg.ListenAddr)

	go client.closeOnCancel(ctx)
	if client.cfg.WebManager.ListenAddr != "" {
		go func() {
			if err := runControl(ctx, client.cfg.WebManager.ListenAddr, client.cfg.WebManager.Username, client.cfg.WebManager.Password, client.webFS, client, client); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	go client.updateAvailableInterfaces(ctx)
	return client.receiveFromWireGuard(ctx)
}

func (client *Client) Status() (any, error) {
	interfaces, err := client.listInterfaces()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	routes := client.routeSnapshot()
	webInterfaces := make([]control.WebInterface, 0, len(interfaces))
	for _, iface := range interfaces {
		ifName := iface.Name
		address := client.interfaceAddress(iface)
		webInterface := control.WebInterface{
			Name:          ifName,
			Label:         client.paths.Label(ifName),
			SenderAddress: address,
		}
		if client.paths.IsExcluded(ifName) {
			webInterface.Status = "excluded"
			webInterfaces = append(webInterfaces, webInterface)
			continue
		}
		webInterface.DstAddress = client.paths.Destination(ifName)
		if route, ok := routes[ifName]; ok {
			webInterface.Status = "active"
			lastRec := route.lastRec.Load()
			if lastRec > 0 {
				last := now - lastRec
				webInterface.Last = &last
			}
		} else {
			webInterface.Status = "idle"
		}
		webInterfaces = append(webInterfaces, webInterface)
	}
	return control.ClientStatus{
		Type:          "client",
		Version:       client.version,
		Description:   client.cfg.Description,
		ListenAddress: client.cfg.ListenAddr,
		Interfaces:    webInterfaces,
	}, nil
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

func (client *Client) closeOnCancel(ctx context.Context) {
	<-ctx.Done()
	if client.wgSocket != nil {
		client.wgSocket.Close()
	}
	client.closeAllRoutes()
}

func (client *Client) updateAvailableInterfaces(ctx context.Context) {
	ticks, stopTicker := newRefreshTicker()
	defer stopTicker()
	for {
		client.refreshInterfaces()
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}
	}
}

func (client *Client) refreshInterfaces() {
	interfaces, err := client.listInterfaces()
	if err != nil {
		return
	}
	known := make(map[string]net.Interface, len(interfaces))
	for _, iface := range interfaces {
		known[iface.Name] = iface
	}

	for ifName, route := range client.routeSnapshot() {
		iface, ok := known[ifName]
		if !ok {
			log.Info("Interface '" + ifName + "' no longer exists, deleting it")
			client.removeRoute(ifName)
			continue
		}
		if client.paths.IsExcluded(ifName) {
			log.Info("Interface '" + ifName + "' is now excluded, deleting it")
			client.removeRoute(ifName)
			continue
		}
		if address := client.interfaceAddress(iface); address != route.srcAddr {
			log.Info("Interface '" + ifName + "' changed address, re-creating socket")
			client.removeRoute(ifName)
		}
	}

	for _, iface := range interfaces {
		ifName := iface.Name
		if client.paths.IsExcluded(ifName) || client.hasRoute(ifName) {
			continue
		}
		address := client.interfaceAddress(iface)
		if address == "" {
			continue
		}
		log.Info("New interface '" + ifName + "' with IP '" + address + "', adding it")
		client.createSendRoute(ifName, address)
	}
}

func (client *Client) createSendRoute(ifName, sourceAddr string) {
	dst := client.paths.Destination(ifName)
	dstAddr, err := resolveUDPAddr("udp4", dst)
	if err != nil {
		log.WithError(err).Error("Can't resolve destination address '" + dst + "' for interface '" + ifName + "', not using it")
		return
	}
	srcAddr, err := resolveUDPAddr("udp4", sourceAddr+":0")
	if err != nil {
		log.WithError(err).Error("Can't resolve source address '" + sourceAddr + "' for interface '" + ifName + "', not using it")
		return
	}
	socket, err := client.openUDPOnInterface(srcAddr, ifName)
	if err != nil {
		log.WithError(err).Error("Can't create socket for address '" + sourceAddr + "' on interface '" + ifName + "', not using it")
		return
	}

	route := &sendRoute{ifName: ifName, srcAddr: sourceAddr, dstAddr: dstAddr, socket: socket}
	client.routesMu.Lock()
	if _, exists := client.routes[ifName]; exists {
		client.routesMu.Unlock()
		socket.Close()
		return
	}
	client.routes[ifName] = route
	client.routesMu.Unlock()
	client.updateWireGuardWriteBuffer()
	go client.writeBack(route)
}

func (client *Client) writeBack(route *sendRoute) {
	buffer := make([]byte, 1500)
	for {
		n, _, err := route.socket.ReadFromUDP(buffer)
		if route.closing.Load() || errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.WithError(err).Warn("Error reading from '" + route.ifName + "', re-creating socket")
			client.removeRoute(route.ifName)
			return
		}
		route.lastRec.Store(time.Now().Unix())
		wgAddr := client.getWireGuardAddr()
		if wgAddr == nil {
			continue
		}
		if _, err := client.wgSocket.WriteToUDP(buffer[:n], wgAddr); err != nil {
			log.WithError(err).Warn("Error writing to WireGuard")
		}
	}
}

func (client *Client) receiveFromWireGuard(ctx context.Context) error {
	buffer := make([]byte, 1500)
	for {
		n, srcAddr, err := client.wgSocket.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from WireGuard")
			continue
		}
		client.setWireGuardAddr(srcAddr)
		client.dispatcher.Fanout(buffer[:n], client.routeTargets())
	}
}

func (client *Client) setWireGuardAddr(addr *net.UDPAddr) {
	client.wgAddrMu.Lock()
	client.wgAddr = addr
	client.wgAddrMu.Unlock()
}

func (client *Client) getWireGuardAddr() *net.UDPAddr {
	client.wgAddrMu.RLock()
	defer client.wgAddrMu.RUnlock()
	return client.wgAddr
}

func (client *Client) routeTargets() []relay.Target {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	targets := make([]relay.Target, 0, len(client.routes))
	for ifName, route := range client.routes {
		targets = append(targets, relay.Target{ID: ifName, Conn: route.socket, Addr: route.dstAddr})
	}
	return targets
}

func (client *Client) routeSnapshot() map[string]*sendRoute {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	snapshot := make(map[string]*sendRoute, len(client.routes))
	for ifName, route := range client.routes {
		snapshot[ifName] = route
	}
	return snapshot
}

func (client *Client) hasRoute(ifName string) bool {
	client.routesMu.RLock()
	defer client.routesMu.RUnlock()
	_, ok := client.routes[ifName]
	return ok
}

func (client *Client) removeRoute(ifName string) {
	client.routesMu.Lock()
	route, ok := client.routes[ifName]
	if ok {
		delete(client.routes, ifName)
	}
	client.routesMu.Unlock()
	if ok {
		client.dispatcher.Remove(ifName)
		route.closing.Store(true)
		route.socket.Close()
		client.updateWireGuardWriteBuffer()
	}
}

func (client *Client) closeAllRoutes() {
	client.routesMu.Lock()
	routes := client.routes
	client.routes = make(map[string]*sendRoute)
	client.routesMu.Unlock()
	client.dispatcher.Close()
	for _, route := range routes {
		route.closing.Store(true)
		route.socket.Close()
	}
}

func (client *Client) updateWireGuardWriteBuffer() {
	client.routesMu.RLock()
	targetCount := len(client.routes)
	client.routesMu.RUnlock()
	if err := relay.SetWriteBufferForTargets(client.wgSocket, targetCount); err != nil {
		log.WithError(err).Warn("Error setting WireGuard write buffer")
	}
}
