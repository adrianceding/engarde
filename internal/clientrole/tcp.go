package clientrole

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/engarde/internal/control"
	"github.com/adrianceding/engarde/internal/socks5"
	"github.com/adrianceding/engarde/internal/stats"
	"github.com/adrianceding/engarde/internal/tcpbind"
	"github.com/adrianceding/engarde/internal/tcpstream"
	log "github.com/sirupsen/logrus"
)

var listenTCP = func(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

var dialTCPOnInterface = tcpbind.DialContext

// Netlink/DHCP updates can briefly hide an address that is still usable by an established Session.
const tcpPathAddressMissGraceRefreshes = 2

type tcpClientRuntime struct {
	client   *Client
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener

	mu            sync.Mutex
	closing       bool
	flows         map[tcpstream.StreamID]*tcpstream.Flow
	paths         map[string]tcpClientPath
	carriers      map[tcpstream.StreamID]map[string]*tcpstream.Carrier
	traffic       map[string]*stats.Traffic
	lastReceived  map[string]*atomic.Int64
	sessions      map[string]*tcpPathSession
	groups        map[*tcpCarrierGroup]struct{}
	addressMisses map[string]int
	active        *tcpActiveCoordinator
	accepted      map[*tcpAcceptedConn]struct{}
	acceptWG      sync.WaitGroup
	groupWG       sync.WaitGroup
	backgroundWG  sync.WaitGroup
	shutdownOnce  sync.Once
	recoveryMu    sync.Mutex
	nextFlowOrder uint64
}

type tcpAcceptedConn struct {
	conn net.Conn
}

type tcpClientPath struct {
	index       int
	address     string
	destination string
}

type tcpClientInterfaceStatusSource struct {
	activeCarriers int
	traffic        *stats.Traffic
	lastReceived   *atomic.Int64
}

func (client *Client) runTCP(ctx context.Context) error {
	listener, err := listenTCP("tcp", client.cfg.ListenAddr)
	if err != nil {
		return err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(ctx)
	runtime := &tcpClientRuntime{
		client:        client,
		ctx:           runtimeCtx,
		cancel:        cancelRuntime,
		listener:      listener,
		flows:         make(map[tcpstream.StreamID]*tcpstream.Flow),
		paths:         make(map[string]tcpClientPath),
		carriers:      make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier),
		traffic:       make(map[string]*stats.Traffic),
		lastReceived:  make(map[string]*atomic.Int64),
		sessions:      make(map[string]*tcpPathSession),
		groups:        make(map[*tcpCarrierGroup]struct{}),
		addressMisses: make(map[string]int),
		accepted:      make(map[*tcpAcceptedConn]struct{}),
	}
	defer runtime.shutdown()
	client.setTCPRuntime(runtime)
	runtime.startActiveCoordinator()
	runtime.refresh()
	log.Info("Listening on " + client.cfg.ListenAddr + " over TCP")
	runtime.backgroundWG.Add(1)
	go func() {
		defer runtime.backgroundWG.Done()
		runtime.closeOnCancel()
	}()
	if client.cfg.WebManager.ListenAddr != "" {
		runtime.backgroundWG.Add(1)
		go func() {
			defer runtime.backgroundWG.Done()
			if err := runControl(runtimeCtx, client.cfg.WebManager.ListenAddr, client.cfg.WebManager.Username, client.cfg.WebManager.Password, client.webFS, client, client); err != nil {
				log.WithError(err).Error("Management webserver stopped")
			}
		}()
	}
	runtime.backgroundWG.Add(1)
	go func() {
		defer runtime.backgroundWG.Done()
		runtime.refreshLoop()
	}()
	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			if runtimeCtx.Err() != nil || errors.Is(acceptErr, net.ErrClosed) {
				return nil
			}
			return acceptErr
		}
		runtime.startAccept(conn)
	}
}

func (runtime *tcpClientRuntime) startAccept(conn net.Conn) bool {
	if runtime.client.cfg.Transfer.TCP.ActiveStandby() && !runtime.recoveryCapacityAvailable() {
		_ = conn.Close()
		return false
	}
	accepted := &tcpAcceptedConn{conn: conn}
	runtime.mu.Lock()
	maxStreams := runtime.client.cfg.Transfer.TCP.MaxStreams
	ctxClosed := runtime.ctx != nil && runtime.ctx.Err() != nil
	if runtime.closing || ctxClosed || (maxStreams > 0 && len(runtime.accepted) >= maxStreams) {
		runtime.mu.Unlock()
		_ = conn.Close()
		return false
	}
	if runtime.accepted == nil {
		runtime.accepted = make(map[*tcpAcceptedConn]struct{})
	}
	runtime.accepted[accepted] = struct{}{}
	runtime.acceptWG.Add(1)
	runtime.mu.Unlock()
	go func() {
		defer runtime.finishAccept(accepted)
		defer conn.Close()
		runtime.accept(conn)
	}()
	return true
}

func (runtime *tcpClientRuntime) finishAccept(accepted *tcpAcceptedConn) {
	runtime.mu.Lock()
	delete(runtime.accepted, accepted)
	runtime.mu.Unlock()
	runtime.acceptWG.Done()
}

func (runtime *tcpClientRuntime) startGroupWorker(worker func()) bool {
	runtime.mu.Lock()
	ctxClosed := runtime.ctx != nil && runtime.ctx.Err() != nil
	if runtime.closing || ctxClosed {
		runtime.mu.Unlock()
		return false
	}
	runtime.groupWG.Add(1)
	runtime.mu.Unlock()
	go func() {
		defer runtime.groupWG.Done()
		worker()
	}()
	return true
}

func (runtime *tcpClientRuntime) accept(conn net.Conn) {
	destination, err := runtime.readDestination(conn)
	if err != nil {
		conn.Close()
		return
	}
	streamID, err := tcpstream.NewStreamID()
	if err != nil {
		conn.Close()
		return
	}
	flow := tcpstream.NewFlow(streamID, conn, tcpstream.DirectionClientToServer, runtime.flowConfig())
	group, err := runtime.assignCarrierGroup(flow, destination, func() error {
		return socks5.WriteReply(conn, socks5.ReplySucceeded, time.Duration(runtime.client.cfg.Transfer.TCP.WriteTimeoutMillis)*time.Millisecond)
	}, func(openErr error) {
		_ = socks5.WriteReply(conn, socks5.ReplyForError(openErr), time.Duration(runtime.client.cfg.Transfer.TCP.WriteTimeoutMillis)*time.Millisecond)
	})
	if err != nil {
		_ = socks5.WriteReply(conn, socks5.ReplyForError(err), time.Duration(runtime.client.cfg.Transfer.TCP.WriteTimeoutMillis)*time.Millisecond)
		flow.Reset(tcpstream.ErrNoCarriers)
		return
	}
	<-flow.Done()
	runtime.releaseCarrierGroup(group)
}

func (runtime *tcpClientRuntime) readDestination(conn net.Conn) (tcpstream.Destination, error) {
	var credentials *socks5.Credentials
	if configured := runtime.client.cfg.SOCKS5Auth; configured != nil {
		credentials = &socks5.Credentials{Username: configured.Username, Password: configured.Password}
	}
	return socks5.ReadConnectWithAuth(conn, time.Duration(runtime.client.cfg.Transfer.TCP.OpenTimeoutMillis)*time.Millisecond, credentials)
}

func (runtime *tcpClientRuntime) refreshLoop() {
	ticks, stopTicker := newRefreshTicker()
	defer stopTicker()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case <-ticks:
			runtime.refresh()
		}
	}
}

func (runtime *tcpClientRuntime) refresh() {
	interfaces, err := runtime.client.listInterfaces()
	if err != nil {
		return
	}
	current := make(map[string]tcpClientPath)
	eligible := make(map[string]struct{})
	unresolved := make(map[string]struct{})
	for _, iface := range interfaces {
		if runtime.client.paths.IsExcluded(iface.Name) {
			continue
		}
		eligible[iface.Name] = struct{}{}
		address := runtime.client.interfaceAddress(iface)
		if address == "" {
			unresolved[iface.Name] = struct{}{}
			continue
		}
		current[iface.Name] = tcpClientPath{
			index:       iface.Index,
			address:     address,
			destination: runtime.client.paths.Destination(iface.Name),
		}
	}
	runtime.retainPathsWithTransientAddressMisses(current, eligible, unresolved)
	runtime.refreshCarrierGroups(current)
}

func (runtime *tcpClientRuntime) retainPathsWithTransientAddressMisses(current map[string]tcpClientPath, eligible, unresolved map[string]struct{}) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.addressMisses == nil {
		runtime.addressMisses = make(map[string]int)
	}
	for interfaceName := range runtime.addressMisses {
		if _, exists := eligible[interfaceName]; !exists {
			delete(runtime.addressMisses, interfaceName)
		}
	}
	for interfaceName := range current {
		delete(runtime.addressMisses, interfaceName)
	}
	for interfaceName := range unresolved {
		path, exists := runtime.paths[interfaceName]
		if !exists {
			delete(runtime.addressMisses, interfaceName)
			continue
		}
		runtime.addressMisses[interfaceName]++
		if runtime.addressMisses[interfaceName] <= tcpPathAddressMissGraceRefreshes {
			current[interfaceName] = path
		}
	}
}

func (runtime *tcpClientRuntime) status() (control.ClientStatus, error) {
	interfaces, err := runtime.client.listInterfaces()
	if err != nil {
		return control.ClientStatus{}, err
	}
	now := time.Now().Unix()
	sessionQuality, sessionCount := runtime.pathSessionStatus()
	activeStandby := runtime.client.cfg.Transfer.TCP.ActiveStandby()
	sources := make(map[string]tcpClientInterfaceStatusSource, len(interfaces))
	runtime.mu.Lock()
	carrierCount := 0
	for _, carriers := range runtime.carriers {
		for interfaceName, carrier := range carriers {
			if carrier != nil {
				carrierCount++
				source := sources[interfaceName]
				source.activeCarriers++
				sources[interfaceName] = source
			}
		}
	}
	for _, iface := range interfaces {
		interfaceName := iface.Name
		source := sources[interfaceName]
		source.traffic = runtime.traffic[interfaceName]
		source.lastReceived = runtime.lastReceived[interfaceName]
		sources[interfaceName] = source
	}
	streamCount := len(runtime.flows)
	flowSources := make([]*tcpstream.Flow, 0, streamCount)
	for _, flow := range runtime.flows {
		flowSources = append(flowSources, flow)
	}
	runtime.mu.Unlock()
	recovering := 0
	for _, flow := range flowSources {
		if flow != nil && flow.State() == tcpstream.FlowStateRecovering {
			recovering++
		}
	}

	webInterfaces := make([]control.WebInterface, 0, len(interfaces))
	for _, iface := range interfaces {
		interfaceName := iface.Name
		source := sources[interfaceName]
		quality, hasQuality := sessionQuality[interfaceName]
		senderAddress := runtime.client.interfaceAddress(iface)
		excluded := runtime.client.paths.IsExcluded(interfaceName)
		if activeStandby && !excluded && !hasQuality {
			quality.state = "connecting"
			if senderAddress == "" {
				quality.state = "unavailable"
			}
		}
		webInterface := control.WebInterface{
			Name:                 interfaceName,
			Label:                runtime.client.paths.Label(interfaceName),
			SenderAddress:        senderAddress,
			Traffic:              source.traffic.Snapshot(),
			QualityState:         quality.state,
			RTTMillis:            quality.rttMillis,
			JitterMillis:         quality.jitterMillis,
			ScoreMillis:          quality.scoreMillis,
			FailurePenaltyMillis: quality.failurePenaltyMillis,
			ActiveFlows:          quality.activeFlows,
			ServerInstanceID:     quality.serverInstanceID,
		}
		if lastReceived := source.lastReceived; lastReceived != nil {
			if last := lastReceived.Load(); last > 0 {
				age := now - last
				if age < 0 {
					age = 0
				}
				webInterface.Last = &age
			}
		}
		if excluded {
			webInterface.Status = "excluded"
		} else {
			webInterface.DstAddress = runtime.client.paths.Destination(interfaceName)
			if quality.active || source.activeCarriers > 0 {
				webInterface.Status = "active"
			} else {
				webInterface.Status = "idle"
			}
		}
		webInterfaces = append(webInterfaces, webInterface)
	}
	return control.ClientStatus{
		Type:                "client",
		Version:             runtime.client.version,
		Description:         runtime.client.cfg.Description,
		ListenAddress:       runtime.client.cfg.ListenAddr,
		FrontendAuthEnabled: runtime.client.cfg.SOCKS5AuthEnabled(),
		PeerAuthEnabled:     runtime.client.cfg.PeerAuthEnabled(),
		Interfaces:          webInterfaces,
		Streams:             streamCount,
		Carriers:            carrierCount,
		Sessions:            sessionCount,
		CarrierMode:         string(runtime.client.cfg.Transfer.TCP.CarrierMode),
		Recovering:          recovering,
	}, nil
}

func (runtime *tcpClientRuntime) trafficForPath(interfaceName string, path tcpClientPath) *stats.Traffic {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if current, ok := runtime.paths[interfaceName]; runtime.closing || !ok || current != path {
		return &stats.Traffic{}
	}
	if runtime.traffic == nil {
		runtime.traffic = make(map[string]*stats.Traffic)
	}
	traffic := runtime.traffic[interfaceName]
	if traffic == nil {
		traffic = &stats.Traffic{}
		runtime.traffic[interfaceName] = traffic
	}
	return traffic
}

func (runtime *tcpClientRuntime) lastReceivedForPath(interfaceName string, path tcpClientPath) *atomic.Int64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if current, ok := runtime.paths[interfaceName]; runtime.closing || !ok || current != path {
		return &atomic.Int64{}
	}
	if runtime.lastReceived == nil {
		runtime.lastReceived = make(map[string]*atomic.Int64)
	}
	lastReceived := runtime.lastReceived[interfaceName]
	if lastReceived == nil {
		lastReceived = &atomic.Int64{}
		runtime.lastReceived[interfaceName] = lastReceived
	}
	return lastReceived
}

type tcpLastReceivedConn struct {
	net.Conn
	lastReceived *atomic.Int64
}

func (conn *tcpLastReceivedConn) Read(payload []byte) (int, error) {
	read, err := conn.Conn.Read(payload)
	if read > 0 {
		conn.lastReceived.Store(time.Now().Unix())
	}
	return read, err
}

func tcpCarrierObserver(traffic *stats.Traffic) tcpstream.CarrierObserver {
	return tcpstream.CarrierObserver{
		Read: func(frame tcpstream.Frame) {
			recordTCPFrameRX(traffic, frame)
		},
		Write: func(frame tcpstream.Frame) {
			recordTCPFrameTX(traffic, frame)
		},
		Drop: func(frame tcpstream.Frame) {
			if frame.Type == tcpstream.FrameData {
				traffic.Data.RecordDrop(len(frame.Payload))
			} else {
				traffic.Control.RecordDrop(tcpstream.HeaderSize + len(frame.Payload))
			}
		},
		Skip: func(frame tcpstream.Frame) {
			if frame.Type == tcpstream.FrameData {
				traffic.Data.RecordSkip(len(frame.Payload))
			} else {
				traffic.Control.RecordSkip(tcpstream.HeaderSize + len(frame.Payload))
			}
		},
	}
}

func recordTCPFrameRX(traffic *stats.Traffic, frame tcpstream.Frame) {
	if frame.Type == tcpstream.FrameData {
		traffic.Data.RecordRX(len(frame.Payload))
	} else {
		traffic.Control.RecordRX(tcpstream.HeaderSize + len(frame.Payload))
	}
}

func recordTCPFrameTX(traffic *stats.Traffic, frame tcpstream.Frame) {
	if frame.Type == tcpstream.FrameData {
		traffic.Data.RecordTX(len(frame.Payload))
	} else {
		traffic.Control.RecordTX(tcpstream.HeaderSize + len(frame.Payload))
	}
}

func (runtime *tcpClientRuntime) flowConfig() tcpstream.FlowConfig {
	tcpConfig := runtime.client.cfg.Transfer.TCP
	return tcpstream.FlowConfig{
		ChunkSize:          tcpConfig.ChunkSize,
		CarrierQueueBytes:  tcpConfig.CarrierQueueBytes,
		ReorderWindowBytes: tcpConfig.ReorderWindowBytes,
		WriteTimeout:       time.Duration(tcpConfig.WriteTimeoutMillis) * time.Millisecond,
		RecoveryTimeout:    time.Duration(tcpConfig.ClientRecoveryTimeoutMillis) * time.Millisecond,
		SingleCarrier:      tcpConfig.ActiveStandby(),
	}
}

func (runtime *tcpClientRuntime) closeOnCancel() {
	<-runtime.ctx.Done()
	if runtime.listener != nil {
		_ = runtime.listener.Close()
	}
}

func (runtime *tcpClientRuntime) shutdown() {
	runtime.shutdownOnce.Do(func() {
		runtime.mu.Lock()
		runtime.closing = true
		accepted := make([]*tcpAcceptedConn, 0, len(runtime.accepted))
		for conn := range runtime.accepted {
			accepted = append(accepted, conn)
		}
		flows := make([]*tcpstream.Flow, 0, len(runtime.flows))
		for _, flow := range runtime.flows {
			flows = append(flows, flow)
		}
		groups := make([]*tcpCarrierGroup, 0, len(runtime.groups))
		for group := range runtime.groups {
			groups = append(groups, group)
		}
		sessions := make([]*tcpPathSession, 0, len(runtime.sessions))
		for _, pathSession := range runtime.sessions {
			sessions = append(sessions, pathSession)
		}
		runtime.mu.Unlock()

		if runtime.cancel != nil {
			runtime.cancel()
		}
		if runtime.listener != nil {
			_ = runtime.listener.Close()
		}
		for _, pathSession := range sessions {
			pathSession.close()
		}
		for _, accepted := range accepted {
			_ = accepted.conn.Close()
		}
		for _, flow := range flows {
			_ = flow.Close()
		}
		for _, group := range groups {
			group.close()
		}
		runtime.acceptWG.Wait()
		runtime.groupWG.Wait()
		runtime.backgroundWG.Wait()

		runtime.mu.Lock()
		runtime.flows = make(map[tcpstream.StreamID]*tcpstream.Flow)
		runtime.paths = make(map[string]tcpClientPath)
		runtime.carriers = make(map[tcpstream.StreamID]map[string]*tcpstream.Carrier)
		runtime.traffic = make(map[string]*stats.Traffic)
		runtime.lastReceived = make(map[string]*atomic.Int64)
		runtime.sessions = make(map[string]*tcpPathSession)
		runtime.groups = make(map[*tcpCarrierGroup]struct{})
		runtime.addressMisses = make(map[string]int)
		runtime.accepted = make(map[*tcpAcceptedConn]struct{})
		runtime.mu.Unlock()
	})
}
