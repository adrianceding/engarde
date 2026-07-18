package serverrole

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

type tcpConnWithRemoteAddr struct {
	net.Conn
}

func (conn *tcpConnWithRemoteAddr) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func dialTCPServerTestSession(t *testing.T, runtime *tcpServerRuntime, credentials *tcpstream.PeerCredentials) (*tcpstream.Session, <-chan struct{}) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	finished := make(chan struct{})
	go func() {
		runtime.accept(&tcpConnWithRemoteAddr{Conn: serverConn})
		close(finished)
	}()
	session, err := tcpstream.DialSessionWithAuth(clientConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), credentials)
	if err != nil {
		_ = clientConn.Close()
		_ = serverConn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("server session did not stop")
		}
	})
	return session, finished
}

func TestTCPServerDestinationForOpen(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := New(config.Server{Transfer: transfer}, "test", nil)
	runtime := &tcpServerRuntime{server: server}
	destination, err := tcpstream.ParseDestination("ss.example.com:8388")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtime.destinationForOpen(tcpstream.Frame{Type: tcpstream.FrameOpen, Payload: payload})
	if err != nil || got != destination.String() {
		t.Fatalf("destination = %q/%v, want %q", got, err, destination.String())
	}
}

func TestTCPServerDestinationForOpenRejectsInvalidPayload(t *testing.T) {
	runtime := &tcpServerRuntime{server: New(config.Server{}, "test", nil)}
	if _, err := runtime.destinationForOpen(tcpstream.Frame{}); err == nil {
		t.Fatal("empty destination payload was accepted")
	}
}

func TestOpenResultForDNSNotFoundIsHostUnreachable(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &net.DNSError{Err: "no such host", Name: "missing.example", IsNotFound: true},
	}
	if got := openResultForError(err); got != tcpstream.OpenResultHostUnreachable {
		t.Fatalf("open result = %d, want host unreachable", got)
	}
}

func TestTCPServerPeerAuthenticationFailureDoesNotDialOrCreateState(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := New(config.Server{
		PeerAuth: &config.ServerPeerAuth{Users: map[string]string{"client-a": "correct-secret"}},
		Transfer: transfer,
	}, "test", nil)
	runtime := &tcpServerRuntime{
		server:   server,
		ctx:      context.Background(),
		streams:  make(map[tcpstream.StreamID]*tcpServerStream),
		closed:   make(map[tcpstream.StreamID]time.Time),
		sessions: make(map[*tcpstream.Session]struct{}),
		traffic:  make(map[string]*tcpServerTraffic),
	}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	previousDial := dialTCPDestination
	dialCount := 0
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		dialCount++
		return nil, errors.New("unexpected destination dial")
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	accepted := make(chan struct{})
	go func() {
		runtime.accept(&tcpConnWithRemoteAddr{Conn: serverConn})
		close(accepted)
	}()
	_, err := tcpstream.DialSessionWithAuth(clientConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig(), &tcpstream.PeerCredentials{
		Username: "client-a",
		Password: "wrong-secret",
	})
	if !errors.Is(err, tcpstream.ErrPeerAuthenticationFailed) {
		t.Fatalf("DialSessionWithAuth error = %v, want authentication failure", err)
	}
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not finish rejecting peer authentication")
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if dialCount != 0 || len(runtime.streams) != 0 || len(runtime.sessions) != 0 || len(runtime.traffic) != 0 {
		t.Fatalf("dials/streams/sessions/traffic = %d/%d/%d/%d, want 0/0/0/0", dialCount, len(runtime.streams), len(runtime.sessions), len(runtime.traffic))
	}
}

func TestTCPServerRejectsZeroStreamIDBeforeDestinationDial(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := New(config.Server{Transfer: transfer}, "test", nil)
	runtime := &tcpServerRuntime{
		server:   server,
		ctx:      context.Background(),
		streams:  make(map[tcpstream.StreamID]*tcpServerStream),
		closed:   make(map[tcpstream.StreamID]time.Time),
		sessions: make(map[*tcpstream.Session]struct{}),
		traffic:  make(map[string]*tcpServerTraffic),
	}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	deadline := time.Now().Add(time.Second)
	_ = clientConn.SetDeadline(deadline)
	_ = serverConn.SetDeadline(deadline)

	previousDial := dialTCPDestination
	dialCount := 0
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		dialCount++
		return nil, errors.New("unexpected destination dial")
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	handled := make(chan struct{})
	go func() {
		runtime.acceptStream(serverConn, tcpstream.MaxPayloadSize, "", &tcpServerTraffic{})
		close(handled)
	}()
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	rawOpen := make([]byte, tcpstream.HeaderSize+len(payload))
	rawOpen[0] = byte(tcpstream.FrameOpen)
	rawOpen[1] = byte(tcpstream.DirectionClientToServer)
	binary.BigEndian.PutUint16(rawOpen[2:4], tcpstream.HeaderSize)
	binary.BigEndian.PutUint32(rawOpen[4:8], uint32(len(payload)))
	copy(rawOpen[tcpstream.HeaderSize:], payload)
	if _, err := clientConn.Write(rawOpen); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("server did not reject zero stream ID")
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if dialCount != 0 || len(runtime.streams) != 0 || len(runtime.sessions) != 0 {
		t.Fatalf("dials/streams/sessions = %d/%d/%d, want 0/0/0", dialCount, len(runtime.streams), len(runtime.sessions))
	}
}

func TestTCPServerGetOrCreateReusesMatchingDestination(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := New(config.Server{Transfer: transfer}, "test", nil)
	runtime := &tcpServerRuntime{
		server:  server,
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
	}
	streamID := tcpstream.StreamID{1}
	runtime.closed[streamID] = time.Now().Add(-time.Second)
	clientEndpoint, serverEndpoint := net.Pipe()
	defer clientEndpoint.Close()
	defer serverEndpoint.Close()
	previousDial := dialTCPDestination
	dialCount := 0
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		dialCount++
		return serverEndpoint, nil
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	first, err := runtime.getOrCreate(streamID, tcpstream.Version, "example.com:443", "client-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.getOrCreate(streamID, tcpstream.Version, "example.com:443", "client-a")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || dialCount != 1 {
		t.Fatalf("streams equal/dials = %v/%d, want true/1", first == second, dialCount)
	}
	runtime.mu.Lock()
	_, expiredTombstoneRetained := runtime.closed[streamID]
	runtime.mu.Unlock()
	if expiredTombstoneRetained {
		t.Fatal("expired stream tombstone was not removed lazily")
	}
	blockedID := tcpstream.StreamID{2}
	runtime.mu.Lock()
	runtime.rememberClosedLocked(blockedID, time.Now())
	runtime.mu.Unlock()
	if _, err := runtime.getOrCreate(blockedID, tcpstream.Version, "example.com:443", "client-a"); err == nil {
		t.Fatal("unexpired stream tombstone did not reject admission")
	}
	if dialCount != 1 {
		t.Fatalf("unexpired tombstone triggered destination dial; dials = %d", dialCount)
	}
	if _, err := runtime.getOrCreate(streamID, tcpstream.Version, "other.example.com:443", "client-a"); err == nil {
		t.Fatal("getOrCreate accepted a mismatched destination")
	}
	if _, err := runtime.getOrCreate(streamID, tcpstream.Version, "example.com:443", "client-b"); err == nil {
		t.Fatal("getOrCreate accepted a mismatched principal")
	}
	first.flow.Close()
}

func TestTCPServerUnlimitedStreamsDoNotRejectAdmission(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 0
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
	}
	previousDial := dialTCPDestination
	peers := make([]net.Conn, 0, 3)
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		endpoint, peer := net.Pipe()
		peers = append(peers, peer)
		return endpoint, nil
	}
	t.Cleanup(func() {
		dialTCPDestination = previousDial
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	flows := make([]*tcpstream.Flow, 0, 3)
	for index := range 3 {
		stream, err := runtime.getOrCreate(tcpstream.StreamID{byte(index + 1)}, tcpstream.Version, "127.0.0.1:1", "")
		if err != nil {
			t.Fatalf("getOrCreate stream %d: %v", index, err)
		}
		flows = append(flows, stream.flow)
	}
	runtime.mu.Lock()
	streamCount := len(runtime.streams)
	runtime.mu.Unlock()
	if streamCount != len(flows) {
		t.Fatalf("streams = %d, want %d with maxStreams=0", streamCount, len(flows))
	}
	for _, flow := range flows {
		_ = flow.Close()
	}
	runtime.flowWG.Wait()
}

func TestTCPServerPositiveMaxStreamsRejectsExcess(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 1
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
	}
	previousDial := dialTCPDestination
	dialCount := 0
	endpoint, peer := net.Pipe()
	defer peer.Close()
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		dialCount++
		return endpoint, nil
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	first, err := runtime.getOrCreate(tcpstream.StreamID{1}, tcpstream.Version, "127.0.0.1:1", "")
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}
	if _, err := runtime.getOrCreate(tcpstream.StreamID{2}, tcpstream.Version, "127.0.0.1:1", ""); err == nil {
		t.Fatal("positive maxStreams accepted an excess stream")
	}
	if dialCount != 1 {
		t.Fatalf("destination dials = %d, want 1", dialCount)
	}
	runtime.mu.Lock()
	streamCount := len(runtime.streams)
	runtime.mu.Unlock()
	if streamCount != 1 {
		t.Fatalf("streams = %d, want 1", streamCount)
	}
	_ = first.flow.Close()
	runtime.flowWG.Wait()
}

func TestTCPServerUnlimitedCarriersDoNotRejectAdmission(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxCarriersPerStream = 0
	runtime := &tcpServerRuntime{
		server:   New(config.Server{Transfer: transfer}, "test", nil),
		ctx:      context.Background(),
		streams:  make(map[tcpstream.StreamID]*tcpServerStream),
		closed:   make(map[tcpstream.StreamID]time.Time),
		sessions: make(map[*tcpstream.Session]struct{}),
		traffic:  make(map[string]*tcpServerTraffic),
	}
	destination, destinationPeer := net.Pipe()
	defer destinationPeer.Close()
	previousDial := dialTCPDestination
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		return destination, nil
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })
	requestedDestination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}

	streamID := tcpstream.StreamID{1}
	clients := make([]net.Conn, 0, 3)
	for index := range 3 {
		session, _ := dialTCPServerTestSession(t, runtime, nil)
		claimed, _, err := session.OpenDestination(streamID, requestedDestination, time.Second)
		if err != nil {
			t.Fatalf("carrier %d open: %v", index, err)
		}
		clients = append(clients, claimed)
	}
	defer func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	}()
	var stream *tcpServerStream
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		stream = runtime.streams[streamID]
		runtime.mu.Unlock()
		return stream != nil && stream.flow.CarrierCount() == len(clients)
	}, "all virtual carriers to attach")
	_ = stream.flow.Close()
	runtime.flowWG.Wait()
}

func TestTCPServerPositiveMaxCarriersPerStreamRejectsExcess(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxCarriersPerStream = 1
	runtime := &tcpServerRuntime{
		server:   New(config.Server{Transfer: transfer}, "test", nil),
		ctx:      context.Background(),
		streams:  make(map[tcpstream.StreamID]*tcpServerStream),
		closed:   make(map[tcpstream.StreamID]time.Time),
		sessions: make(map[*tcpstream.Session]struct{}),
		traffic:  make(map[string]*tcpServerTraffic),
	}
	destinationConn, destinationPeer := net.Pipe()
	defer destinationPeer.Close()
	previousDial := dialTCPDestination
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		return destinationConn, nil
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	streamID := tcpstream.StreamID{1}

	firstSession, _ := dialTCPServerTestSession(t, runtime, nil)
	firstCarrier, _, err := firstSession.OpenDestination(streamID, destination, time.Second)
	if err != nil {
		t.Fatalf("first carrier open: %v", err)
	}
	defer firstCarrier.Close()
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		stream := runtime.streams[streamID]
		runtime.mu.Unlock()
		return stream != nil && stream.flow.CarrierCount() == 1
	}, "first virtual carrier to attach")

	secondSession, _ := dialTCPServerTestSession(t, runtime, nil)
	if _, _, err := secondSession.OpenDestination(streamID, destination, time.Second); err == nil {
		t.Fatal("positive maxCarriersPerStream accepted an excess carrier")
	} else {
		var openErr *tcpstream.OpenError
		if !errors.As(err, &openErr) || openErr.Result != tcpstream.OpenResultGeneralFailure {
			t.Fatalf("excess carrier error = %v, want general OPEN failure", err)
		}
	}
	runtime.mu.Lock()
	stream := runtime.streams[streamID]
	sessionCount := len(runtime.sessions)
	runtime.mu.Unlock()
	if stream == nil || stream.flow.CarrierCount() != 1 {
		t.Fatal("excess carrier changed the active carrier count")
	}
	if sessionCount != 2 {
		t.Fatalf("physical sessions = %d, want 2 after rejecting one virtual carrier", sessionCount)
	}
	_ = stream.flow.Close()
	runtime.flowWG.Wait()
}

func TestTCPServerSessionCarriesMultipleStreams(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:   New(config.Server{Transfer: transfer}, "test", nil),
		ctx:      context.Background(),
		streams:  make(map[tcpstream.StreamID]*tcpServerStream),
		closed:   make(map[tcpstream.StreamID]time.Time),
		sessions: make(map[*tcpstream.Session]struct{}),
		traffic:  make(map[string]*tcpServerTraffic),
	}
	previousDial := dialTCPDestination
	peers := make([]net.Conn, 0, 2)
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		endpoint, peer := net.Pipe()
		peers = append(peers, peer)
		return endpoint, nil
	}
	t.Cleanup(func() {
		dialTCPDestination = previousDial
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := dialTCPServerTestSession(t, runtime, nil)
	clientStreams := make([]net.Conn, 0, 2)
	for index := range 2 {
		clientStream, _, err := session.OpenDestination(tcpstream.StreamID{byte(index + 1)}, destination, time.Second)
		if err != nil {
			t.Fatalf("open stream %d: %v", index, err)
		}
		clientStreams = append(clientStreams, clientStream)
	}
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		sessionCount := len(runtime.sessions)
		streams := make([]*tcpServerStream, 0, len(runtime.streams))
		for _, stream := range runtime.streams {
			streams = append(streams, stream)
		}
		runtime.mu.Unlock()
		if sessionCount != 1 || len(streams) != 2 {
			return false
		}
		for _, stream := range streams {
			if stream.flow == nil || stream.flow.CarrierCount() != 1 {
				return false
			}
		}
		return true
	}, "one physical session to carry two logical streams")

	runtime.mu.Lock()
	flows := make([]*tcpstream.Flow, 0, len(runtime.streams))
	for _, stream := range runtime.streams {
		flows = append(flows, stream.flow)
	}
	runtime.mu.Unlock()
	for _, flow := range flows {
		_ = flow.Close()
	}
	for _, clientStream := range clientStreams {
		_ = clientStream.Close()
	}
	runtime.flowWG.Wait()
	runtime.mu.Lock()
	sessionCount := len(runtime.sessions)
	runtime.mu.Unlock()
	if sessionCount != 1 {
		t.Fatalf("physical sessions after logical streams closed = %d, want 1", sessionCount)
	}
}

func TestTCPServerPendingAdmissionCanBeUnlimited(t *testing.T) {
	unlimited := &tcpServerRuntime{}
	for index := range 3 {
		if !unlimited.tryAcquirePending() {
			t.Fatalf("unlimited pending admission rejected connection %d", index)
		}
		unlimited.releasePending()
	}

	limited := &tcpServerRuntime{pending: make(chan struct{}, 1)}
	if !limited.tryAcquirePending() {
		t.Fatal("limited pending admission rejected its first connection")
	}
	if limited.tryAcquirePending() {
		t.Fatal("limited pending admission accepted a connection above its limit")
	}
	limited.releasePending()
	if !limited.tryAcquirePending() {
		t.Fatal("limited pending admission did not release its slot")
	}
	limited.releasePending()
}

func TestTCPServerPendingAdmissionReleasedAfterSessionHandshake(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:      New(config.Server{Transfer: transfer}, "test", nil),
		ctx:         context.Background(),
		streams:     make(map[tcpstream.StreamID]*tcpServerStream),
		closed:      make(map[tcpstream.StreamID]time.Time),
		sessions:    make(map[*tcpstream.Session]struct{}),
		connections: make(map[*tcpServerConn]struct{}),
		traffic:     make(map[string]*tcpServerTraffic),
		pending:     make(chan struct{}, 1),
	}
	clientConn, serverConn := net.Pipe()
	if !runtime.tryAcquirePending() {
		t.Fatal("failed to reserve initial handshake slot")
	}
	runtime.startAccept(&tcpConnWithRemoteAddr{Conn: serverConn})
	session, err := tcpstream.DialSession(clientConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		sessionCount := len(runtime.sessions)
		runtime.mu.Unlock()
		return len(runtime.pending) == 0 && sessionCount == 1
	}, "handshake slot release while session remains active")
	if !runtime.tryAcquirePending() {
		t.Fatal("active session retained the handshake admission slot")
	}
	runtime.releasePending()
	_ = session.Close()
	runtime.acceptWG.Wait()
}

func TestTCPServerMaxSessionsRejectsExcessWithoutHoldingHandshakeAdmission(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxSessions = 1
	runtime := &tcpServerRuntime{
		server:      New(config.Server{Transfer: transfer}, "test", nil),
		ctx:         context.Background(),
		streams:     make(map[tcpstream.StreamID]*tcpServerStream),
		closed:      make(map[tcpstream.StreamID]time.Time),
		sessions:    make(map[*tcpstream.Session]struct{}),
		connections: make(map[*tcpServerConn]struct{}),
		traffic:     make(map[string]*tcpServerTraffic),
		pending:     make(chan struct{}, 1),
	}
	sessions := make([]*tcpstream.Session, 0, 2)
	t.Cleanup(func() {
		for _, session := range sessions {
			_ = session.Close()
		}
		runtime.acceptWG.Wait()
	})

	firstClient, firstServer := net.Pipe()
	if !runtime.tryAcquirePending() {
		t.Fatal("failed to reserve first handshake slot")
	}
	runtime.startAccept(&tcpConnWithRemoteAddr{Conn: firstServer})
	firstSession, err := tcpstream.DialSession(firstClient, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig())
	if err != nil {
		t.Fatal(err)
	}
	sessions = append(sessions, firstSession)
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		sessionCount := len(runtime.sessions)
		runtime.mu.Unlock()
		return len(runtime.pending) == 0 && sessionCount == 1
	}, "first session admission")

	secondClient, secondServer := net.Pipe()
	if !runtime.tryAcquirePending() {
		t.Fatal("active session incorrectly retained the handshake admission slot")
	}
	runtime.startAccept(&tcpConnWithRemoteAddr{Conn: secondServer})
	secondSession, secondErr := tcpstream.DialSession(secondClient, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig())
	if secondErr != nil && !tcpstream.IsSessionClosed(secondErr) {
		t.Fatal(secondErr)
	}
	if secondSession != nil {
		sessions = append(sessions, secondSession)
	}
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		sessionCount := len(runtime.sessions)
		connectionCount := len(runtime.connections)
		runtime.mu.Unlock()
		return len(runtime.pending) == 0 && sessionCount == 1 && connectionCount == 1
	}, "excess session rejection and handshake slot release")
	if secondSession != nil {
		destination, err := tcpstream.ParseDestination("example.com:443")
		if err != nil {
			t.Fatal(err)
		}
		if stream, _, err := secondSession.OpenDestination(tcpstream.StreamID{2}, destination, time.Second); err == nil {
			_ = stream.Close()
			t.Fatal("excess session accepted a virtual stream")
		}
	}
	if firstSession.IsClosed() {
		t.Fatal("rejecting the excess session closed the admitted session")
	}
}

func TestTCPServerMaxPendingStreamsRejectsExcessAndRecoversCapacity(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxPendingStreams = 1
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
		traffic: make(map[string]*tcpServerTraffic),
	}
	traffic := &tcpServerTraffic{}
	clientConns := make([]net.Conn, 0, 3)
	t.Cleanup(func() {
		for _, conn := range clientConns {
			_ = conn.Close()
		}
		runtime.streamWG.Wait()
		runtime.mu.Lock()
		flows := make([]*tcpstream.Flow, 0, len(runtime.streams))
		for _, stream := range runtime.streams {
			if stream.flow != nil {
				flows = append(flows, stream.flow)
			}
		}
		runtime.mu.Unlock()
		for _, flow := range flows {
			_ = flow.Close()
		}
		runtime.flowWG.Wait()
	})

	firstClient, firstServer := net.Pipe()
	clientConns = append(clientConns, firstClient)
	if !runtime.startSessionStream(firstServer, tcpstream.MaxPayloadSize, "", traffic) {
		t.Fatal("first virtual stream stopped the physical session")
	}
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		pending := runtime.pendingStreams
		runtime.mu.Unlock()
		return pending == 1
	}, "first pending virtual stream admission")

	secondClient, secondServer := net.Pipe()
	clientConns = append(clientConns, secondClient)
	if !runtime.startSessionStream(secondServer, tcpstream.MaxPayloadSize, "", traffic) {
		t.Fatal("pending stream limit stopped the physical session")
	}
	_ = secondClient.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := secondClient.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess virtual stream remained open")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("excess virtual stream was not closed promptly")
	}
	runtime.mu.Lock()
	pending := runtime.pendingStreams
	runtime.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending virtual streams = %d after rejection, want 1", pending)
	}
	_ = secondClient.Close()

	_ = firstClient.Close()
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		pending := runtime.pendingStreams
		runtime.mu.Unlock()
		return pending == 0
	}, "pending virtual stream capacity release")

	destinationEndpoint, destinationPeer := net.Pipe()
	previousDial := dialTCPDestination
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		return destinationEndpoint, nil
	}
	t.Cleanup(func() {
		dialTCPDestination = previousDial
		_ = destinationPeer.Close()
	})
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	thirdClient, thirdServer := net.Pipe()
	clientConns = append(clientConns, thirdClient)
	if !runtime.startSessionStream(thirdServer, tcpstream.MaxPayloadSize, "", traffic) {
		t.Fatal("released pending capacity did not admit the next virtual stream")
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	streamID := tcpstream.StreamID{3}
	if err := tcpstream.WriteFrame(thirdClient, tcpstream.Frame{
		Type:      tcpstream.FrameOpen,
		Direction: tcpstream.DirectionClientToServer,
		StreamID:  streamID,
		Payload:   payload,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := tcpstream.ReadFrame(thirdClient, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != tcpstream.FrameOpenResult || result.StreamID != streamID || len(result.Payload) != 1 || tcpstream.OpenResult(result.Payload[0]) != tcpstream.OpenResultSuccess {
		t.Fatalf("OPEN_RESULT = %#v, want success for the admitted virtual stream", result)
	}
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		pending := runtime.pendingStreams
		stream := runtime.streams[streamID]
		runtime.mu.Unlock()
		return pending == 0 && stream != nil && stream.flow.CarrierCount() == 1
	}, "third virtual stream attach and admission release")

	runtime.mu.Lock()
	stream := runtime.streams[streamID]
	runtime.mu.Unlock()
	_ = stream.flow.Close()
	_ = thirdClient.Close()
	runtime.streamWG.Wait()
	runtime.flowWG.Wait()
}

func TestTCPServerFailedInitialOpenDoesNotPoisonRedundantPath(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.OpenTimeoutMillis = 1000
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
		traffic: make(map[string]*tcpServerTraffic),
	}
	destinationEndpoint, destinationPeer := net.Pipe()
	previousDial := dialTCPDestination
	dialCount := 0
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		dialCount++
		return destinationEndpoint, nil
	}
	t.Cleanup(func() {
		dialTCPDestination = previousDial
		_ = destinationPeer.Close()
	})
	destination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := destination.Encode()
	if err != nil {
		t.Fatal(err)
	}
	streamID := tcpstream.StreamID{4}
	openFrame := tcpstream.Frame{
		Type:      tcpstream.FrameOpen,
		Direction: tcpstream.DirectionClientToServer,
		StreamID:  streamID,
		Payload:   payload,
	}
	traffic := &tcpServerTraffic{}

	badClient, badServer := net.Pipe()
	badDone := make(chan struct{})
	go func() {
		runtime.acceptStream(badServer, tcpstream.MaxPayloadSize, "", traffic)
		close(badDone)
	}()
	if err := tcpstream.WriteFrame(badClient, openFrame); err != nil {
		t.Fatal(err)
	}
	_ = badClient.Close()
	select {
	case <-badDone:
	case <-time.After(time.Second):
		t.Fatal("failed initial OPEN path did not exit")
	}
	runtime.mu.Lock()
	initial := runtime.streams[streamID]
	runtime.mu.Unlock()
	if initial == nil || initial.flow == nil {
		t.Fatal("failed initial OPEN path removed the shared flow")
	}
	select {
	case <-initial.flow.Done():
		t.Fatal("failed initial OPEN path reset the shared flow")
	default:
	}

	healthyClient, healthyServer := net.Pipe()
	t.Cleanup(func() { _ = healthyClient.Close() })
	healthyDone := make(chan struct{})
	go func() {
		runtime.acceptStream(healthyServer, tcpstream.MaxPayloadSize, "", traffic)
		close(healthyDone)
	}()
	if err := tcpstream.WriteFrame(healthyClient, openFrame); err != nil {
		t.Fatal(err)
	}
	result, err := tcpstream.ReadFrame(healthyClient, tcpstream.MaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != tcpstream.FrameOpenResult || result.StreamID != streamID || len(result.Payload) != 1 || tcpstream.OpenResult(result.Payload[0]) != tcpstream.OpenResultSuccess {
		t.Fatalf("healthy OPEN_RESULT = %#v, want success", result)
	}
	select {
	case <-healthyDone:
	case <-time.After(time.Second):
		t.Fatal("healthy redundant OPEN path did not attach")
	}
	runtime.mu.Lock()
	current := runtime.streams[streamID]
	runtime.mu.Unlock()
	if current != initial {
		t.Fatal("healthy redundant OPEN path did not reuse the original flow")
	}
	if dialCount != 1 {
		t.Fatalf("destination dial count = %d, want 1", dialCount)
	}
	if current.flow.CarrierCount() != 1 {
		t.Fatalf("healthy carrier count = %d, want 1", current.flow.CarrierCount())
	}
	current.attachMu.Lock()
	started := current.started
	timer := current.openTimer
	current.attachMu.Unlock()
	if !started || timer != nil {
		t.Fatalf("healthy flow started/timer = %t/%v, want true/nil", started, timer)
	}
	_ = current.flow.Close()
	runtime.flowWG.Wait()
}

func TestTCPServerUnattachedFlowExpiresAfterOpenTimeout(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.OpenTimeoutMillis = 20
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     context.Background(),
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
		traffic: make(map[string]*tcpServerTraffic),
	}
	destinationEndpoint, destinationPeer := net.Pipe()
	previousDial := dialTCPDestination
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		return destinationEndpoint, nil
	}
	t.Cleanup(func() {
		dialTCPDestination = previousDial
		_ = destinationPeer.Close()
	})
	streamID := tcpstream.StreamID{5}
	stream, err := runtime.getOrCreate(streamID, tcpstream.Version, "example.com:443", "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.flow.Done():
		t.Fatal("unattached flow expired before openTimeout")
	default:
	}
	select {
	case <-stream.flow.Done():
	case <-time.After(time.Second):
		t.Fatal("unattached flow did not expire after openTimeout")
	}
	if err := stream.flow.Err(); !errors.Is(err, errTCPStreamOpenTimeout) {
		t.Fatalf("unattached flow error = %v, want OPEN timeout", err)
	}
	runtime.flowWG.Wait()
	runtime.mu.Lock()
	_, active := runtime.streams[streamID]
	_, closed := runtime.closed[streamID]
	runtime.mu.Unlock()
	if active || !closed {
		t.Fatalf("unattached flow active/closed = %t/%t, want false/true", active, closed)
	}
}

func TestTCPServerUnlimitedAdmissionKeepsAuxiliaryCachesBounded(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		closed:  make(map[tcpstream.StreamID]time.Time, tcpServerClosedCacheSafetyLimit),
		traffic: make(map[string]*tcpServerTraffic),
	}
	base := time.Unix(1_700_000_000, 0)
	for index := range tcpServerClosedCacheSafetyLimit {
		var streamID tcpstream.StreamID
		binary.BigEndian.PutUint64(streamID[:8], uint64(index+1))
		runtime.closed[streamID] = base.Add(time.Hour)
	}
	var newest tcpstream.StreamID
	binary.BigEndian.PutUint64(newest[:8], uint64(tcpServerClosedCacheSafetyLimit+1))
	runtime.rememberClosedLocked(newest, base)
	if got := len(runtime.closed); got != tcpServerClosedCacheSafetyLimit {
		t.Fatalf("unlimited closed cache entries = %d, want %d", got, tcpServerClosedCacheSafetyLimit)
	}
	if _, exists := runtime.closed[newest]; !exists {
		t.Fatal("unlimited closed cache did not retain the newest tombstone")
	}
	if got := runtime.trafficLimit(); got != tcpServerTrafficCacheSafetyLimit {
		t.Fatalf("unlimited traffic cache limit = %d, want %d", got, tcpServerTrafficCacheSafetyLimit)
	}
}

func TestTCPServerClosedCacheEvictsOldestTombstone(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 2
	runtime := &tcpServerRuntime{
		server: New(config.Server{Transfer: transfer}, "test", nil),
		closed: make(map[tcpstream.StreamID]time.Time),
	}
	base := time.Unix(1_700_000_000, 0)
	first := tcpstream.StreamID{1}
	second := tcpstream.StreamID{2}
	third := tcpstream.StreamID{3}
	runtime.rememberClosedLocked(first, base)
	runtime.rememberClosedLocked(second, base.Add(time.Second))
	runtime.rememberClosedLocked(third, base.Add(2*time.Second))
	if len(runtime.closed) != 2 {
		t.Fatalf("closed cache entries = %d, want 2", len(runtime.closed))
	}
	if _, exists := runtime.closed[first]; exists {
		t.Fatal("closed cache retained its oldest tombstone")
	}
	if _, exists := runtime.closed[second]; !exists {
		t.Fatal("closed cache evicted a newer tombstone")
	}
	if _, exists := runtime.closed[third]; !exists {
		t.Fatal("closed cache did not retain the newest tombstone")
	}
}

func TestTCPServerFailedDialTombstonesStayBoundedAndExpireWithoutRequests(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		ctx:     ctx,
		streams: make(map[tcpstream.StreamID]*tcpServerStream),
		closed:  make(map[tcpstream.StreamID]time.Time),
	}
	previousDial := dialTCPDestination
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		return nil, errors.New("destination unavailable")
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	for index := range 20 {
		streamID := tcpstream.StreamID{byte(index + 1)}
		if _, err := runtime.getOrCreate(streamID, tcpstream.Version, "127.0.0.1:1", ""); err == nil {
			t.Fatalf("dial %d unexpectedly succeeded", index)
		}
	}
	runtime.mu.Lock()
	if got := len(runtime.closed); got != transfer.TCP.MaxStreams {
		runtime.mu.Unlock()
		t.Fatalf("closed tombstones = %d, want %d", got, transfer.TCP.MaxStreams)
	}
	for streamID := range runtime.closed {
		runtime.closed[streamID] = time.Now().Add(-time.Second)
	}
	runtime.mu.Unlock()

	ticks := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		runtime.maintain(ticks)
		close(done)
	}()
	ticks <- time.Now()
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.closed) == 0
	}, "expired closed tombstones to be pruned")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance loop did not stop")
	}
}

func TestTCPServerTrafficIsBoundedAndInactiveEntriesExpire(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 2
	transfer.TCP.MaxCarriersPerStream = 2
	transfer.TCP.MaxPendingConnections = 1
	runtime := &tcpServerRuntime{
		server:  New(config.Server{Transfer: transfer}, "test", nil),
		closed:  make(map[tcpstream.StreamID]time.Time),
		traffic: make(map[string]*tcpServerTraffic),
	}
	base := time.Unix(1_700_000_000, 0)
	limit := runtime.trafficLimit()
	newestAddress := "192.0.2.latest"
	for index := range limit + 10 {
		runtime.trafficForAddressAt(fmt.Sprintf("192.0.2.%d", index), base.Add(time.Duration(index)*time.Second))
	}
	runtime.trafficForAddressAt(newestAddress, base.Add(time.Duration(limit+10)*time.Second))
	runtime.mu.Lock()
	if got := len(runtime.traffic); got != limit {
		runtime.mu.Unlock()
		t.Fatalf("traffic entries = %d, want hard limit %d", got, limit)
	}
	active := runtime.traffic[newestAddress]
	if active == nil {
		runtime.mu.Unlock()
		t.Fatalf("expected newest traffic entry %q to remain", newestAddress)
	}
	for _, traffic := range runtime.traffic {
		traffic.active = 0
	}
	active.active = 1
	runtime.mu.Unlock()

	runtime.pruneState(base.Add(time.Duration(limit+11)*time.Second + tcpServerTrafficTTL))
	runtime.mu.Lock()
	if len(runtime.traffic) != 1 || runtime.traffic[newestAddress] != active {
		runtime.mu.Unlock()
		t.Fatalf("active traffic entry was evicted: %#v", runtime.traffic)
	}
	active.active = 0
	runtime.mu.Unlock()
	runtime.pruneState(base.Add(time.Duration(limit+12)*time.Second + tcpServerTrafficTTL))
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.traffic) != 0 {
		t.Fatalf("inactive expired traffic entries = %d, want 0", len(runtime.traffic))
	}
}

func TestTCPServerCarrierObserverDoesNotRetimestampActiveTraffic(t *testing.T) {
	traffic := &tcpServerTraffic{active: 1}
	lastUsed := time.Unix(1_700_000_000, 0)
	traffic.touch(lastUsed)
	observer := tcpCarrierObserver(traffic)

	observer.Read(tcpstream.Frame{Type: tcpstream.FrameData, Payload: []byte("read")})
	observer.Write(tcpstream.Frame{Type: tcpstream.FrameFIN})
	observer.Drop(tcpstream.Frame{Type: tcpstream.FrameData, Payload: []byte("drop")})
	observer.Skip(tcpstream.Frame{Type: tcpstream.FrameAck, Payload: []byte{1}})

	if got := traffic.usedAt(); !got.Equal(lastUsed) {
		t.Fatalf("active traffic timestamp = %v, want %v", got, lastUsed)
	}
	snapshot := traffic.Snapshot()
	if snapshot.Data.RXPackets != 1 || snapshot.Data.RXBytes != 4 || snapshot.Data.DropPackets != 1 || snapshot.Data.DropBytes != 4 {
		t.Fatalf("data traffic = %#v", snapshot.Data)
	}
	if snapshot.Control.TXPackets != 1 || snapshot.Control.TXBytes != tcpstream.HeaderSize || snapshot.Control.SkippedPackets != 1 || snapshot.Control.SkippedBytes != tcpstream.HeaderSize+1 {
		t.Fatalf("control traffic = %#v", snapshot.Control)
	}
}

func TestTCPServerTrafficRetainsActiveEntriesAboveCacheLimit(t *testing.T) {
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	transfer.TCP.MaxStreams = 1
	transfer.TCP.MaxCarriersPerStream = 1
	transfer.TCP.MaxPendingConnections = 1
	runtime := &tcpServerRuntime{
		server:      New(config.Server{Transfer: transfer}, "test", nil),
		connections: make(map[*tcpServerConn]struct{}),
		traffic:     make(map[string]*tcpServerTraffic),
	}

	type trackedPeer struct {
		conn    *tcpServerConn
		peer    net.Conn
		traffic *tcpServerTraffic
		address string
	}
	newTrackedPeer := func(address string) trackedPeer {
		serverConn, peer := net.Pipe()
		tracked := &tcpServerConn{Conn: serverConn, runtime: runtime}
		runtime.mu.Lock()
		runtime.connections[tracked] = struct{}{}
		runtime.mu.Unlock()
		return trackedPeer{conn: tracked, peer: peer, traffic: runtime.trafficForConnection(tracked, address), address: address}
	}

	peers := []trackedPeer{
		newTrackedPeer("192.0.2.1"),
		newTrackedPeer("192.0.2.2"),
		newTrackedPeer("192.0.2.3"),
	}
	defer func() {
		for _, item := range peers {
			_ = item.conn.Close()
			_ = item.peer.Close()
		}
	}()
	limit := runtime.trafficLimit()
	runtime.mu.Lock()
	if len(runtime.traffic) != len(peers) || len(runtime.traffic) <= limit {
		runtime.mu.Unlock()
		t.Fatalf("active traffic entries/limit = %d/%d, want %d active entries retained", len(runtime.traffic), limit, len(peers))
	}
	for _, item := range peers {
		if runtime.traffic[item.address] != item.traffic || item.traffic.active != 1 {
			runtime.mu.Unlock()
			t.Fatalf("active traffic %q was not retained", item.address)
		}
	}
	runtime.mu.Unlock()

	first := peers[0]
	if err := first.conn.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	if len(runtime.traffic) != limit || runtime.traffic[first.address] != nil || first.traffic.active != 0 {
		runtime.mu.Unlock()
		t.Fatalf("inactive traffic did not shrink cache: entries/active = %d/%d", len(runtime.traffic), first.traffic.active)
	}
	for _, item := range peers[1:] {
		if runtime.traffic[item.address] != item.traffic {
			runtime.mu.Unlock()
			t.Fatalf("shrinking cache evicted active traffic %q", item.address)
		}
	}
	runtime.mu.Unlock()
}

func TestTCPServerCancellationWaitsForAcceptedHandshake(t *testing.T) {
	listener := newScriptedTCPListener()
	server, cancel, runResult := startScriptedTCPServer(t, listener)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	listener.results <- tcpAcceptResult{conn: &tcpConnWithRemoteAddr{Conn: serverConn}}
	runtime := waitForTCPServerRuntime(t, server)
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.connections) == 1
	}, "accepted connection to be tracked")

	cancel()
	if err := receiveTCPServerRunResult(t, runResult); err != nil {
		t.Fatalf("runTCP cancellation error = %v", err)
	}
	assertTCPServerRuntimeClean(t, runtime)
}

func TestTCPServerCancellationRejectsDialSuccessAfterCancel(t *testing.T) {
	listener := newScriptedTCPListener()
	previousDial := dialTCPDestination
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	destinationConn, destinationPeer := net.Pipe()
	defer destinationPeer.Close()
	dialTCPDestination = func(context.Context, string, time.Duration) (net.Conn, error) {
		close(dialStarted)
		<-releaseDial
		return destinationConn, nil
	}
	t.Cleanup(func() { dialTCPDestination = previousDial })

	server, cancel, runResult := startScriptedTCPServer(t, listener)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	listener.results <- tcpAcceptResult{conn: &tcpConnWithRemoteAddr{Conn: serverConn}}
	runtime := waitForTCPServerRuntime(t, server)
	session, err := tcpstream.DialSession(clientConn, tcpstream.MaxPayloadSize, time.Second, runtime.sessionConfig())
	if err != nil {
		t.Fatal(err)
	}
	requestedDestination, err := tcpstream.ParseDestination("example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	claimResult := make(chan error, 1)
	go func() {
		_, _, claimErr := session.OpenDestination(tcpstream.StreamID{1}, requestedDestination, time.Second)
		claimResult <- claimErr
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("destination dial did not start")
	}

	cancel()
	close(releaseDial)
	if err := receiveTCPServerRunResult(t, runResult); err != nil {
		t.Fatalf("runTCP cancellation error = %v", err)
	}
	select {
	case claimErr := <-claimResult:
		if claimErr == nil {
			t.Fatal("destination OPEN succeeded after server cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("destination OPEN did not finish after server cancellation")
	}
	assertTCPServerRuntimeClean(t, runtime)
	_ = destinationPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := destinationPeer.Read(make([]byte, 1)); err == nil {
		t.Fatal("destination connection created during shutdown remained open")
	}
}

func TestTCPServerAcceptErrorCleansAcceptedConnections(t *testing.T) {
	listener := newScriptedTCPListener()
	server, cancel, runResult := startScriptedTCPServer(t, listener)
	defer cancel()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	listener.results <- tcpAcceptResult{conn: &tcpConnWithRemoteAddr{Conn: serverConn}}
	runtime := waitForTCPServerRuntime(t, server)
	waitForTCPServerCondition(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return len(runtime.connections) == 1
	}, "accepted connection to be tracked")
	wantErr := errors.New("accept failed")
	listener.results <- tcpAcceptResult{err: wantErr}
	if err := receiveTCPServerRunResult(t, runResult); !errors.Is(err, wantErr) {
		t.Fatalf("runTCP error = %v, want %v", err, wantErr)
	}
	assertTCPServerRuntimeClean(t, runtime)
}

type tcpAcceptResult struct {
	conn net.Conn
	err  error
}

type scriptedTCPListener struct {
	results   chan tcpAcceptResult
	closed    chan struct{}
	closeOnce sync.Once
}

func newScriptedTCPListener() *scriptedTCPListener {
	return &scriptedTCPListener{results: make(chan tcpAcceptResult, 4), closed: make(chan struct{})}
}

func (listener *scriptedTCPListener) Accept() (net.Conn, error) {
	select {
	case result := <-listener.results:
		return result.conn, result.err
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *scriptedTCPListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *scriptedTCPListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero}
}

func startScriptedTCPServer(t *testing.T, listener *scriptedTCPListener) (*Server, context.CancelFunc, <-chan error) {
	t.Helper()
	transfer := config.Transfer{}
	transfer.ApplyDefaults()
	server := New(config.Server{ListenAddr: "127.0.0.1:0", AllowUnsafeDynamicDestination: true, Transfer: transfer}, "test", nil)
	previousListen := listenTCP
	listenTCP = func(string, string) (net.Listener, error) { return listener, nil }
	t.Cleanup(func() { listenTCP = previousListen })
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.runTCP(ctx) }()
	return server, cancel, result
}

func waitForTCPServerRuntime(t *testing.T, server *Server) *tcpServerRuntime {
	t.Helper()
	var runtime *tcpServerRuntime
	waitForTCPServerCondition(t, func() bool {
		runtime = server.getTCPRuntime()
		return runtime != nil
	}, "TCP server runtime to start")
	return runtime
}

func receiveTCPServerRunResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("TCP server did not stop")
		return nil
	}
}

func assertTCPServerRuntimeClean(t *testing.T, runtime *tcpServerRuntime) {
	t.Helper()
	runtime.mu.Lock()
	streams := len(runtime.streams)
	closed := len(runtime.closed)
	sessions := len(runtime.sessions)
	connections := len(runtime.connections)
	traffic := len(runtime.traffic)
	pendingStreams := runtime.pendingStreams
	runtime.mu.Unlock()
	if streams != 0 || closed != 0 || sessions != 0 || connections != 0 || traffic != 0 || len(runtime.pending) != 0 || pendingStreams != 0 {
		t.Fatalf("streams/closed/sessions/connections/traffic/pending/pendingStreams = %d/%d/%d/%d/%d/%d/%d, want all zero", streams, closed, sessions, connections, traffic, len(runtime.pending), pendingStreams)
	}
}

func waitForTCPServerCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
