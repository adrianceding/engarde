package serverrole

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
	"github.com/adrianceding/engarde/internal/relay"
	"github.com/adrianceding/engarde/internal/udp"
	log "github.com/sirupsen/logrus"
)

var runControl = control.Run
var resolveUDPAddr = net.ResolveUDPAddr
var listenUDP = func(network string, laddr *net.UDPAddr) (udpSocket, error) {
	conn, err := net.ListenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	return udp.Wrap(conn), nil
}

type udpSocket interface {
	relay.UDPWriter
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	Close() error
}

type Server struct {
	cfg     config.Server
	version string
	webFS   fs.FS

	wgSocket     udpSocket
	clientSocket udpSocket
	wgAddr       *net.UDPAddr

	clientsMu sync.RWMutex
	clients   map[string]*connectedClient

	dispatcher *relay.Dispatcher
}

type connectedClient struct {
	addr *net.UDPAddr
	last atomic.Int64
}

func New(cfg config.Server, version string, webFS fs.FS) *Server {
	server := &Server{
		cfg:     cfg,
		version: version,
		webFS:   webFS,
		clients: make(map[string]*connectedClient),
	}
	server.dispatcher = relay.NewDispatcherWithBatch(cfg.WriteTimeout, relay.DefaultQueueSize, cfg.UDPBatch.IsEnabled(), cfg.UDPBatch.EffectiveWriteSize(), func(result relay.Result) {
		log.WithError(result.Err).Warn("Error writing to client '" + result.ID + "', terminating it")
		server.removeClient(result.ID)
	})
	return server
}

func (server *Server) Run(ctx context.Context) error {
	if server.cfg.Description != "" {
		log.Info(server.cfg.Description)
	}

	wgAddr, err := resolveUDPAddr("udp4", server.cfg.DstAddr)
	if err != nil {
		return err
	}
	wgSource, err := resolveUDPAddr("udp4", "0.0.0.0:0")
	if err != nil {
		return err
	}
	wgSocket, err := listenUDP("udp", wgSource)
	if err != nil {
		return err
	}
	server.wgAddr = wgAddr
	server.wgSocket = wgSocket
	server.updateWireGuardWriteBuffer()

	listenAddr, err := resolveUDPAddr("udp4", server.cfg.ListenAddr)
	if err != nil {
		wgSocket.Close()
		return err
	}
	clientSocket, err := listenUDP("udp", listenAddr)
	if err != nil {
		wgSocket.Close()
		return err
	}
	server.clientSocket = clientSocket
	log.Info("Listening on " + server.cfg.ListenAddr)

	go server.closeOnCancel(ctx)
	if server.cfg.WebManager.ListenAddr != "" {
		go func() {
			if err := runControl(ctx, server.cfg.WebManager.ListenAddr, server.cfg.WebManager.Username, server.cfg.WebManager.Password, server.webFS, server, nil); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	go server.receiveFromWireGuard(ctx)
	return server.receiveFromClient(ctx)
}

func (server *Server) Status() (any, error) {
	now := time.Now().Unix()
	sockets := make([]control.WebSocket, 0)
	server.clientsMu.RLock()
	for address, client := range server.clients {
		lastValue := client.last.Load()
		webSocket := control.WebSocket{Address: address}
		if lastValue > 0 {
			last := now - lastValue
			webSocket.Last = &last
		}
		sockets = append(sockets, webSocket)
	}
	server.clientsMu.RUnlock()

	return control.ServerStatus{
		Type:          "server",
		Version:       server.version,
		Description:   server.cfg.Description,
		ListenAddress: server.cfg.ListenAddr,
		DstAddress:    server.cfg.DstAddr,
		Sockets:       sockets,
	}, nil
}

func (server *Server) closeOnCancel(ctx context.Context) {
	<-ctx.Done()
	server.dispatcher.Close()
	if server.wgSocket != nil {
		server.wgSocket.Close()
	}
	if server.clientSocket != nil {
		server.clientSocket.Close()
	}
}

func (server *Server) receiveFromClient(ctx context.Context) error {
	readBatch := udp.NewReadBatch(server.cfg.UDPBatch.EffectiveReadSize())
	writeBatch := make([]udp.Packet, 0, server.cfg.UDPBatch.EffectiveWriteSize())
	learnedAddrs := make([]*net.UDPAddr, 0, server.cfg.UDPBatch.EffectiveReadSize())
	for {
		n, err := udp.ReadBatch(server.clientSocket, readBatch, server.cfg.UDPBatch.IsEnabled())
		if err != nil && n == 0 {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from client")
			continue
		}
		writeBatch = writeBatch[:0]
		learnedAddrs = learnedAddrs[:0]
		now := time.Now().Unix()
		for i := 0; i < n; i++ {
			addr := readBatch[i].Addr
			if addr == nil {
				continue
			}
			if !containsUDPAddr(learnedAddrs, addr) {
				server.learnClientAt(addr, now)
				learnedAddrs = append(learnedAddrs, addr)
			}
			writeBatch = append(writeBatch, udp.Packet{Payload: readBatch[i].Payload, Addr: server.wgAddr})
		}
		if _, err := udp.WriteBatchChunks(server.wgSocket, writeBatch, server.cfg.UDPBatch.IsEnabled(), server.cfg.UDPBatch.EffectiveWriteSize()); err != nil {
			log.WithError(err).Warn("Error writing to WireGuard")
		}
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.WithError(err).Warn("Error reading from client")
		}
	}
}

func (server *Server) receiveFromWireGuard(ctx context.Context) {
	readBatch := udp.NewReadBatch(server.cfg.UDPBatch.EffectiveReadSize())
	payloads := make([][]byte, 0, server.cfg.UDPBatch.EffectiveReadSize())
	for {
		n, err := udp.ReadBatch(server.wgSocket, readBatch, server.cfg.UDPBatch.IsEnabled())
		if err != nil && n == 0 {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.WithError(err).Warn("Error reading from WireGuard")
			continue
		}
		payloads = payloads[:0]
		for i := 0; i < n; i++ {
			payloads = append(payloads, readBatch[i].Payload)
		}
		if len(payloads) > 0 {
			server.dispatcher.FanoutBatch(payloads, server.clientTargets(time.Now().Unix()))
		}
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.WithError(err).Warn("Error reading from WireGuard")
		}
	}
}

func (server *Server) learnClient(addr *net.UDPAddr) {
	server.learnClientAt(addr, time.Now().Unix())
}

func (server *Server) learnClientAt(addr *net.UDPAddr, now int64) {
	key := addr.String()
	server.clientsMu.RLock()
	client, ok := server.clients[key]
	server.clientsMu.RUnlock()
	if ok {
		client.last.Store(now)
		return
	}

	log.Info("New client connected: '" + key + "'")
	client = &connectedClient{addr: addr}
	client.last.Store(now)
	server.clientsMu.Lock()
	server.clients[key] = client
	server.clientsMu.Unlock()
	server.updateWireGuardWriteBuffer()
}

func containsUDPAddr(addrs []*net.UDPAddr, addr *net.UDPAddr) bool {
	for _, existing := range addrs {
		if sameUDPAddr(existing, addr) {
			return true
		}
	}
	return false
}

func sameUDPAddr(first, second *net.UDPAddr) bool {
	if first == nil || second == nil {
		return first == second
	}
	return first.Port == second.Port && first.Zone == second.Zone && first.IP.Equal(second.IP)
}

func (server *Server) clientTargets(now int64) []relay.Target {
	targets := make([]relay.Target, 0)
	expired := make([]string, 0)
	cutoff := now - server.cfg.ClientTimeout
	server.clientsMu.RLock()
	for address, client := range server.clients {
		if client.last.Load() > cutoff {
			targets = append(targets, relay.Target{ID: address, Conn: server.clientSocket, Addr: client.addr})
			continue
		}
		expired = append(expired, address)
	}
	server.clientsMu.RUnlock()

	for _, address := range expired {
		log.Info("Client '" + address + "' timed out")
		server.removeExpiredClient(address, cutoff)
	}
	return targets
}

func (server *Server) removeExpiredClient(address string, cutoff int64) {
	server.clientsMu.Lock()
	client, ok := server.clients[address]
	if !ok {
		server.clientsMu.Unlock()
		return
	}
	removed := false
	if client.last.Load() <= cutoff {
		delete(server.clients, address)
		removed = true
	}
	server.clientsMu.Unlock()
	if removed {
		server.dispatcher.Remove(address)
		server.updateWireGuardWriteBuffer()
	}
}

func (server *Server) removeClient(address string) {
	server.clientsMu.Lock()
	_, removed := server.clients[address]
	if removed {
		delete(server.clients, address)
	}
	server.clientsMu.Unlock()
	if removed {
		server.dispatcher.Remove(address)
		server.updateWireGuardWriteBuffer()
	}
}

func (server *Server) updateWireGuardWriteBuffer() {
	server.clientsMu.RLock()
	targetCount := len(server.clients)
	server.clientsMu.RUnlock()
	if err := relay.SetWriteBufferForTargets(server.wgSocket, targetCount); err != nil {
		log.WithError(err).Warn("Error setting WireGuard write buffer")
	}
}
