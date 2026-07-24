package tcpstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

const sessionTestTimeout = 3 * time.Second

type sessionAcceptResult struct {
	session   *Session
	principal string
	err       error
}

type sessionOpenResult struct {
	conn        net.Conn
	maxPayload  uint32
	destination Destination
	err         error
}

type sessionRecoverableOpenResult struct {
	conn       net.Conn
	maxPayload uint32
	result     RecoverableOpenResult
	err        error
}

type sessionWriteFailureConn struct {
	net.Conn
	err error
}

type sessionManualTimer struct {
	delay time.Duration
	ticks chan time.Time

	mu      sync.Mutex
	stopped bool
}

func (timer *sessionManualTimer) stop() {
	timer.mu.Lock()
	timer.stopped = true
	timer.mu.Unlock()
}

func (timer *sessionManualTimer) isStopped() bool {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	return timer.stopped
}

type sessionManualTimerFactory struct {
	requests chan *sessionManualTimer
}

func installSessionManualOpenTimer(t *testing.T) *sessionManualTimerFactory {
	t.Helper()
	factory := &sessionManualTimerFactory{requests: make(chan *sessionManualTimer, 4)}
	previous := newSessionOpenTimer
	newSessionOpenTimer = func(delay time.Duration) (<-chan time.Time, func()) {
		timer := &sessionManualTimer{delay: delay, ticks: make(chan time.Time, 1)}
		factory.requests <- timer
		return timer.ticks, timer.stop
	}
	t.Cleanup(func() { newSessionOpenTimer = previous })
	return factory
}

func (factory *sessionManualTimerFactory) next(t *testing.T) *sessionManualTimer {
	t.Helper()
	select {
	case timer := <-factory.requests:
		return timer
	case <-time.After(sessionTestTimeout):
		t.Fatal("session open did not request a timer")
		return nil
	}
}

func (factory *sessionManualTimerFactory) assertNoPending(t *testing.T) {
	t.Helper()
	select {
	case timer := <-factory.requests:
		t.Fatalf("unexpected additional session open timer with delay %v", timer.delay)
	default:
	}
}

func (conn *sessionWriteFailureConn) Write([]byte) (int, error) {
	return 0, conn.err
}

func TestSessionConfigDefaults(t *testing.T) {
	config := smuxSessionConfig(SessionConfig{})
	defaults := smux.DefaultConfig()
	if config.Version != smuxProtocolVersion {
		t.Fatalf("smux version = %d, want %d", config.Version, smuxProtocolVersion)
	}
	if config.KeepAliveInterval != defaults.KeepAliveInterval {
		t.Fatalf("keepalive interval = %s, want %s", config.KeepAliveInterval, defaults.KeepAliveInterval)
	}
	if config.KeepAliveTimeout != defaults.KeepAliveTimeout {
		t.Fatalf("keepalive timeout = %s, want %s", config.KeepAliveTimeout, defaults.KeepAliveTimeout)
	}
	if config.MaxReceiveBuffer != 4*1024*1024 {
		t.Fatalf("receive buffer = %d, want %d", config.MaxReceiveBuffer, 4*1024*1024)
	}
	if config.MaxStreamBuffer != 1024*1024 {
		t.Fatalf("stream buffer = %d, want %d", config.MaxStreamBuffer, 1024*1024)
	}
	if err := smux.VerifyConfig(config); err != nil {
		t.Fatalf("default session config is invalid: %v", err)
	}
}

func TestClientSessionDoneWhenPeerConnectionCloses(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	config := SessionConfig{
		KeepaliveInterval: time.Hour,
		KeepaliveTimeout:  2 * time.Hour,
	}
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, config, nil)
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSession(clientConn, MaxPayloadSize, time.Second, config)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()
	server := waitSessionAccept(t, serverResult)
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer server.session.Close()

	if err := serverConn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-clientSession.Done():
	case <-time.After(time.Second):
		t.Fatal("client Session.Done did not close after the peer connection closed")
	}
	if !clientSession.IsClosed() {
		t.Fatal("client session is not closed after the peer connection closed")
	}
	select {
	case <-clientSession.clientAcceptDone:
	case <-time.After(time.Second):
		t.Fatal("client AcceptStream monitor did not stop after the peer connection closed")
	}
}

func TestClientSessionLocalCloseStopsAcceptMonitor(t *testing.T) {
	clientSession, serverSession, _ := newSessionTestPair(t, nil, nil)
	defer serverSession.Close()

	if err := clientSession.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-clientSession.clientAcceptDone:
	default:
		t.Fatal("client AcceptStream monitor is still running after Session.Close returned")
	}
}

func TestClientSessionRejectsServerInitiatedStream(t *testing.T) {
	clientSession, serverSession, _ := newSessionTestPair(t, nil, nil)
	defer clientSession.Close()
	defer serverSession.Close()

	unexpected, _ := serverSession.mux.OpenStream()
	if unexpected != nil {
		defer unexpected.Close()
	}
	select {
	case <-clientSession.Done():
	case <-time.After(time.Second):
		t.Fatal("client session did not close after a server-initiated stream")
	}
	if !clientSession.IsClosed() {
		t.Fatal("client session accepted a server-initiated stream")
	}
}

func TestOpenDestinationClosesSessionOnOpenStreamFailure(t *testing.T) {
	injected := errors.New("injected connection write failure")
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	mux, err := smux.Client(&sessionWriteFailureConn{Conn: clientConn, err: injected}, smuxSessionConfig(SessionConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{mux: mux, maxPayload: MaxPayloadSize}

	conn, _, err := session.OpenDestination(sessionTestStreamID(9), sessionTestDestination(t, "failure.example:443"), time.Second)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("failed OpenStream returned a connection")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("OpenDestination error = %v, want %v", err, injected)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("OpenDestination did not close the session after OpenStream failed")
	}
}

func TestRecoverableOpenClosesSessionOnOpenStreamFailure(t *testing.T) {
	injected := errors.New("injected connection write failure")
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	mux, err := smux.Client(&sessionWriteFailureConn{Conn: clientConn, err: injected}, smuxSessionConfig(SessionConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{mux: mux, maxPayload: MaxPayloadSize, activeStandby: true}

	conn, _, _, err := session.OpenRecoverableDestination(
		sessionTestStreamID(10),
		sessionTestDestination(t, "failure.example:443"),
		ResumeToken{1},
		3*time.Second,
		time.Second,
	)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("failed OpenStream returned a connection")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("OpenRecoverableDestination error = %v, want %v", err, injected)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("OpenRecoverableDestination did not close the session after OpenStream failed")
	}
}

func TestOpenDestinationTimeoutIncludesOpeningSMUXStream(t *testing.T) {
	timers := installSessionManualOpenTimer(t)
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	mux, err := smux.Client(clientConn, smuxSessionConfig(SessionConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{mux: mux, maxPayload: MaxPayloadSize}

	result := make(chan sessionOpenResult, 1)
	go func() {
		conn, maxPayload, err := session.OpenDestination(sessionTestStreamID(10), sessionTestDestination(t, "timeout.example:443"), 25*time.Millisecond)
		result <- sessionOpenResult{conn: conn, maxPayload: maxPayload, err: err}
	}()
	timer := timers.next(t)
	if timer.delay != 25*time.Millisecond {
		t.Fatalf("open timer delay = %v, want 25ms", timer.delay)
	}
	select {
	case <-session.Done():
		t.Fatal("session closed before the open timer fired")
	default:
	}
	timer.ticks <- time.Time{}
	var opened sessionOpenResult
	select {
	case opened = <-result:
	case <-time.After(sessionTestTimeout):
		t.Fatal("OpenDestination did not return after the open timer fired")
	}
	if !timer.isStopped() {
		t.Fatal("session open timer was not stopped after timeout")
	}
	timers.assertNoPending(t)
	conn, err := opened.conn, opened.err
	if conn != nil {
		_ = conn.Close()
		t.Fatal("timed out OpenStream returned a connection")
	}
	if !errors.Is(err, smux.ErrTimeout) {
		t.Fatalf("OpenDestination error = %v, want %v", err, smux.ErrTimeout)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("OpenDestination did not close the blocked session")
	}
}

func TestSessionMultiplexesStreamsIndependently(t *testing.T) {
	clientSession, serverSession, _ := newSessionTestPair(t, nil, nil)
	defer clientSession.Close()
	defer serverSession.Close()

	firstID := sessionTestStreamID(1)
	firstDestination := sessionTestDestination(t, "one.example:443")
	firstAccepted := acceptSessionOpen(serverSession, OpenResultSuccess)
	firstClient, maxPayload, err := clientSession.OpenDestination(firstID, firstDestination, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer firstClient.Close()
	if maxPayload != MaxPayloadSize {
		t.Fatalf("first max payload = %d, want %d", maxPayload, MaxPayloadSize)
	}
	firstServer := waitSessionOpen(t, firstAccepted)
	if firstServer.err != nil {
		t.Fatal(firstServer.err)
	}
	defer firstServer.conn.Close()
	if firstServer.destination.String() != firstDestination.String() {
		t.Fatalf("first destination = %s, want %s", firstServer.destination.String(), firstDestination.String())
	}

	secondID := sessionTestStreamID(2)
	secondDestination := sessionTestDestination(t, "two.example:8443")
	secondAccepted := acceptSessionOpen(serverSession, OpenResultSuccess)
	secondClient, maxPayload, err := clientSession.OpenDestination(secondID, secondDestination, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer secondClient.Close()
	if maxPayload != MaxPayloadSize {
		t.Fatalf("second max payload = %d, want %d", maxPayload, MaxPayloadSize)
	}
	secondServer := waitSessionOpen(t, secondAccepted)
	if secondServer.err != nil {
		t.Fatal(secondServer.err)
	}
	defer secondServer.conn.Close()
	if secondServer.destination.String() != secondDestination.String() {
		t.Fatalf("second destination = %s, want %s", secondServer.destination.String(), secondDestination.String())
	}
	if got := clientSession.StreamCount(); got != 2 {
		t.Fatalf("client stream count = %d, want 2", got)
	}
	if got := serverSession.StreamCount(); got != 2 {
		t.Fatalf("server stream count = %d, want 2", got)
	}

	if err := firstClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstServer.conn.Close(); err != nil {
		t.Fatal(err)
	}
	assertSessionOpen(t, clientSession, "client")
	assertSessionOpen(t, serverSession, "server")

	setSessionTestDeadline(t, secondClient)
	setSessionTestDeadline(t, secondServer.conn)
	clientFrame := Frame{
		Type:      FrameData,
		Direction: DirectionClientToServer,
		StreamID:  secondID,
		Payload:   []byte("client payload"),
	}
	if err := WriteFrame(secondClient, clientFrame); err != nil {
		t.Fatal(err)
	}
	gotClientFrame, err := ReadFrame(secondServer.conn, secondServer.maxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotClientFrame.Payload) != string(clientFrame.Payload) || gotClientFrame.StreamID != secondID {
		t.Fatalf("server frame = %+v, want stream %x payload %q", gotClientFrame, secondID, clientFrame.Payload)
	}

	serverFrame := Frame{
		Type:      FrameData,
		Direction: DirectionServerToClient,
		StreamID:  secondID,
		Payload:   []byte("server payload"),
	}
	if err := WriteFrame(secondServer.conn, serverFrame); err != nil {
		t.Fatal(err)
	}
	gotServerFrame, err := ReadFrame(secondClient, maxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotServerFrame.Payload) != string(serverFrame.Payload) || gotServerFrame.StreamID != secondID {
		t.Fatalf("client frame = %+v, want stream %x payload %q", gotServerFrame, secondID, serverFrame.Payload)
	}
}

func TestSessionOpenRejectionDoesNotCloseSession(t *testing.T) {
	clientSession, serverSession, _ := newSessionTestPair(t, nil, nil)
	defer clientSession.Close()
	defer serverSession.Close()

	rejectedID := sessionTestStreamID(3)
	rejected := acceptSessionOpen(serverSession, OpenResultPolicyDenied)
	conn, _, err := clientSession.OpenDestination(rejectedID, sessionTestDestination(t, "denied.example:443"), time.Second)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("rejected OPEN returned a connection")
	}
	var openError *OpenError
	if !errors.As(err, &openError) || openError.Result != OpenResultPolicyDenied {
		t.Fatalf("OPEN error = %v, want policy denied", err)
	}
	rejectedServer := waitSessionOpen(t, rejected)
	if rejectedServer.err != nil {
		t.Fatal(rejectedServer.err)
	}
	assertSessionOpen(t, clientSession, "client after rejected OPEN")
	assertSessionOpen(t, serverSession, "server after rejected OPEN")

	acceptedID := sessionTestStreamID(4)
	accepted := acceptSessionOpen(serverSession, OpenResultSuccess)
	acceptedClient, _, err := clientSession.OpenDestination(acceptedID, sessionTestDestination(t, "allowed.example:443"), time.Second)
	if err != nil {
		t.Fatalf("second OPEN failed after rejection: %v", err)
	}
	defer acceptedClient.Close()
	acceptedServer := waitSessionOpen(t, accepted)
	if acceptedServer.err != nil {
		t.Fatal(acceptedServer.err)
	}
	defer acceptedServer.conn.Close()

	setSessionTestDeadline(t, acceptedClient)
	setSessionTestDeadline(t, acceptedServer.conn)
	want := Frame{Type: FrameData, Direction: DirectionClientToServer, StreamID: acceptedID, Payload: []byte("still alive")}
	if err := WriteFrame(acceptedClient, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(acceptedServer.conn, acceptedServer.maxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != string(want.Payload) || got.StreamID != acceptedID {
		t.Fatalf("frame after rejected OPEN = %+v", got)
	}
}

func TestSessionOpenResultDeadlineClosesSession(t *testing.T) {
	timers := installSessionManualOpenTimer(t)
	previousNow := sessionOpenNow
	sessionOpenNow = func() time.Time { return time.Unix(1, 0) }
	t.Cleanup(func() { sessionOpenNow = previousNow })
	clientSession, serverSession, _ := newSessionTestPair(t, nil, nil)
	defer clientSession.Close()
	defer serverSession.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		stream, _, err := serverSession.AcceptStream()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- stream
	}()

	conn, _, err := clientSession.OpenDestination(sessionTestStreamID(5), sessionTestDestination(t, "silent.example:443"), 25*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("timed out OPEN_RESULT returned a connection")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("OpenDestination error = %v, want timeout", err)
	}
	select {
	case <-clientSession.Done():
	default:
		t.Fatal("OPEN_RESULT timeout did not close the physical session")
	}
	select {
	case stream := <-accepted:
		if stream != nil {
			_ = stream.Close()
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not accept the timed out virtual stream")
	}
	timer := timers.next(t)
	if timer.delay != 25*time.Millisecond {
		t.Fatalf("open timer delay = %v, want 25ms", timer.delay)
	}
	if !timer.isStopped() {
		t.Fatal("session open timer was not stopped after stream opened")
	}
	timers.assertNoPending(t)
}

func TestSessionPeerAuthentication(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		credentials := &PeerCredentials{Username: "client-a", Password: "peer-secret"}
		clientSession, serverSession, principal := newSessionTestPair(t, credentials, map[string]string{"client-a": "peer-secret"})
		defer clientSession.Close()
		defer serverSession.Close()
		if principal != "client-a" {
			t.Fatalf("principal = %q, want client-a", principal)
		}
		assertSessionOpen(t, clientSession, "authenticated client")
		assertSessionOpen(t, serverSession, "authenticated server")
	})

	t.Run("failure", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		serverResult := make(chan sessionAcceptResult, 1)
		go func() {
			session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, SessionConfig{}, map[string]string{"client-a": "peer-secret"})
			serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
		}()

		clientSession, err := DialSessionWithAuth(clientConn, MaxPayloadSize, time.Second, SessionConfig{}, &PeerCredentials{Username: "client-a", Password: "wrong-secret"})
		if clientSession != nil {
			_ = clientSession.Close()
			t.Fatal("failed authentication returned a client session")
		}
		if !errors.Is(err, ErrPeerAuthenticationFailed) {
			t.Fatalf("client authentication error = %v, want %v", err, ErrPeerAuthenticationFailed)
		}
		server := waitSessionAccept(t, serverResult)
		if server.session != nil {
			_ = server.session.Close()
			t.Fatal("failed authentication returned a server session")
		}
		if !errors.Is(server.err, ErrPeerAuthenticationFailed) {
			t.Fatalf("server authentication error = %v, want %v", server.err, ErrPeerAuthenticationFailed)
		}
		if server.principal != "" {
			t.Fatalf("failed authentication principal = %q, want empty", server.principal)
		}
	})
}

func TestSessionActiveStandbyCapability(t *testing.T) {
	instanceID := ServerInstanceID{1, 2, 3}
	clientConfig := SessionConfig{ActiveStandby: true}
	serverConfig := SessionConfig{ActiveStandby: true, ServerInstanceID: instanceID, OrphanRetention: 9 * time.Second}
	clientConn, serverConn := net.Pipe()
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, serverConfig, map[string]string{"client-a": "peer-secret"})
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSessionWithAuth(clientConn, MaxPayloadSize, time.Second, clientConfig, &PeerCredentials{Username: "client-a", Password: "peer-secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()
	server := waitSessionAccept(t, serverResult)
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer server.session.Close()
	if server.principal != "client-a" {
		t.Fatalf("principal = %q, want client-a", server.principal)
	}
	for name, session := range map[string]*Session{"client": clientSession, "server": server.session} {
		if !session.ActiveStandby() || session.ServerInstanceID() != instanceID || session.ServerOrphanRetention() != 9*time.Second {
			t.Fatalf("%s capability = active %v instance %x retention %v", name, session.ActiveStandby(), session.ServerInstanceID(), session.ServerOrphanRetention())
		}
	}
}

func TestSessionActiveStandbyRequiresPeerSupport(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, SessionConfig{}, nil)
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSession(clientConn, MaxPayloadSize, time.Second, SessionConfig{ActiveStandby: true})
	if clientSession != nil {
		_ = clientSession.Close()
		t.Fatal("capability mismatch returned a client session")
	}
	if !errors.Is(err, ErrActiveStandbyRequired) {
		t.Fatalf("client capability error = %v, want %v", err, ErrActiveStandbyRequired)
	}
	server := waitSessionAccept(t, serverResult)
	if server.session != nil {
		_ = server.session.Close()
		t.Fatal("capability mismatch returned a server session")
	}
	if !errors.Is(server.err, ErrActiveStandbyRequired) {
		t.Fatalf("server capability error = %v, want %v", server.err, ErrActiveStandbyRequired)
	}
}

func TestSessionRecoverableOpenAndResume(t *testing.T) {
	instanceID := ServerInstanceID{1}
	clientConfig := SessionConfig{ActiveStandby: true}
	serverConfig := SessionConfig{ActiveStandby: true, ServerInstanceID: instanceID, OrphanRetention: 9 * time.Second}
	clientConn, serverConn := net.Pipe()
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, serverConfig, nil)
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSession(clientConn, MaxPayloadSize, time.Second, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()
	server := waitSessionAccept(t, serverResult)
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer server.session.Close()

	streamID := sessionTestStreamID(20)
	token := ResumeToken{7, 8, 9}
	destination := sessionTestDestination(t, "recoverable.example:443")
	serverDone := make(chan error, 1)
	go func() {
		openStream, maxPayload, err := server.session.AcceptStream()
		if err != nil {
			serverDone <- err
			return
		}
		defer openStream.Close()
		openFrame, err := ReadFrame(openStream, maxPayload)
		if err != nil {
			serverDone <- err
			return
		}
		open, err := DecodeRecoverableOpen(openFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if open.Destination != destination || open.ResumeToken != token || open.RecoveryTimeoutMillis != 3000 {
			serverDone <- fmt.Errorf("recoverable OPEN = %#v", open)
			return
		}
		if err := WriteFrame(openStream, (RecoverableOpenResult{Result: OpenResultSuccess, Generation: 1, ServerOrphanRetentionMillis: 9000}).Frame(streamID)); err != nil {
			serverDone <- err
			return
		}

		resumeStream, maxPayload, err := server.session.AcceptStream()
		if err != nil {
			serverDone <- err
			return
		}
		defer resumeStream.Close()
		resume, err := ReadFrame(resumeStream, maxPayload)
		if err != nil {
			serverDone <- err
			return
		}
		resumeToken, err := ResumeTokenFromFrame(resume)
		if err != nil || resumeToken != token || resume.Offset != 2 {
			serverDone <- fmt.Errorf("RESUME = token %x generation %d error %v", resumeToken, resume.Offset, err)
			return
		}
		serverDone <- WriteFrame(resumeStream, NewResumeResultFrame(streamID, 2, ResumeResultSuccess))
	}()

	openConn, maxPayload, result, err := clientSession.OpenRecoverableDestination(streamID, destination, token, 3*time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer openConn.Close()
	if maxPayload != MaxPayloadSize || result.Generation != 1 || result.ServerOrphanRetentionMillis != 9000 {
		t.Fatalf("recoverable OPEN result = payload %d result %#v", maxPayload, result)
	}
	resumeConn, maxPayload, err := clientSession.Resume(streamID, token, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer resumeConn.Close()
	if maxPayload != MaxPayloadSize {
		t.Fatalf("RESUME max payload = %d, want %d", maxPayload, MaxPayloadSize)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not finish recoverable OPEN and RESUME")
	}
}

func TestRecoverableOpenReportsUncertainWrittenRequest(t *testing.T) {
	clientSession, serverSession := newActiveSessionTestPair(t)
	defer clientSession.Close()
	defer serverSession.Close()
	requestRead := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err == nil {
			_, err = ReadFrame(stream, maxPayload)
			_ = stream.Close()
		}
		requestRead <- err
	}()
	_, _, _, err := clientSession.OpenRecoverableDestination(sessionTestStreamID(30), sessionTestDestination(t, "uncertain.example:443"), ResumeToken{1}, 3*time.Second, time.Second)
	if err == nil || !StreamRequestWasWritten(err) {
		t.Fatalf("recoverable OPEN error = %v, want uncertain written request", err)
	}
	select {
	case err := <-requestRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not read uncertain recoverable OPEN")
	}
	assertSessionOpen(t, clientSession, "client after uncertain recoverable OPEN")
	assertSessionOpen(t, serverSession, "server after uncertain recoverable OPEN")

	reusedID := sessionTestStreamID(31)
	reusedDestination := sessionTestDestination(t, "reused.example:443")
	reusedToken := ResumeToken{2}
	serverDone := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err != nil {
			serverDone <- err
			return
		}
		defer stream.Close()
		request, err := ReadFrame(stream, maxPayload)
		if err != nil {
			serverDone <- err
			return
		}
		open, err := DecodeRecoverableOpen(request)
		if err != nil {
			serverDone <- err
			return
		}
		if open.Destination != reusedDestination || open.ResumeToken != reusedToken {
			serverDone <- fmt.Errorf("reused recoverable OPEN = %#v", open)
			return
		}
		serverDone <- WriteFrame(stream, (RecoverableOpenResult{
			Result:                      OpenResultSuccess,
			Generation:                  request.Offset,
			ServerOrphanRetentionMillis: 9000,
		}).Frame(request.StreamID))
	}()
	reusedConn, maxPayload, result, err := clientSession.OpenRecoverableDestination(reusedID, reusedDestination, reusedToken, 3*time.Second, time.Second)
	if err != nil {
		t.Fatalf("recoverable OPEN reuse failed: %v", err)
	}
	defer reusedConn.Close()
	if maxPayload != MaxPayloadSize || result.Generation != 1 || result.ServerOrphanRetentionMillis != 9000 {
		t.Fatalf("reused recoverable OPEN result = payload %d result %#v", maxPayload, result)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not finish reused recoverable OPEN")
	}
	assertSessionOpen(t, clientSession, "client after reused recoverable OPEN")
	assertSessionOpen(t, serverSession, "server after reused recoverable OPEN")
}

func TestRecoverableOpenResultDeadlineKeepsSession(t *testing.T) {
	clientSession, serverSession := newActiveSessionTestPair(t)
	defer clientSession.Close()
	defer serverSession.Close()

	accepted := make(chan sessionOpenResult, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err == nil {
			_, err = ReadFrame(stream, maxPayload)
		}
		accepted <- sessionOpenResult{conn: stream, err: err}
	}()
	result := make(chan sessionRecoverableOpenResult, 1)
	go func() {
		conn, maxPayload, openResult, err := clientSession.OpenRecoverableDestination(
			sessionTestStreamID(32),
			sessionTestDestination(t, "silent-recoverable.example:443"),
			ResumeToken{3},
			3*time.Second,
			250*time.Millisecond,
		)
		result <- sessionRecoverableOpenResult{conn: conn, maxPayload: maxPayload, result: openResult, err: err}
	}()
	serverStream := waitSessionOpen(t, accepted)
	if serverStream.err != nil {
		t.Fatal(serverStream.err)
	}
	defer serverStream.conn.Close()

	var opened sessionRecoverableOpenResult
	select {
	case opened = <-result:
	case <-time.After(sessionTestTimeout):
		t.Fatal("recoverable OPEN did not return after its response deadline")
	}
	if opened.conn != nil {
		_ = opened.conn.Close()
		t.Fatal("timed out recoverable OPEN returned a connection")
	}
	var netErr net.Error
	if !errors.As(opened.err, &netErr) || !netErr.Timeout() || !StreamRequestWasWritten(opened.err) {
		t.Fatalf("recoverable OPEN error = %v, want timeout for a written request", opened.err)
	}
	assertSessionOpen(t, clientSession, "client after recoverable OPEN timeout")
	assertSessionOpen(t, serverSession, "server after recoverable OPEN timeout")
}

func TestResumeEOFKeepsSessionAndAllowsRetry(t *testing.T) {
	clientSession, serverSession := newActiveSessionTestPair(t)
	defer clientSession.Close()
	defer serverSession.Close()

	streamID := sessionTestStreamID(33)
	token := ResumeToken{4}
	firstRead := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err == nil {
			_, err = ReadFrame(stream, maxPayload)
			_ = stream.Close()
		}
		firstRead <- err
	}()
	_, _, err := clientSession.Resume(streamID, token, 2, time.Second)
	if err == nil || !StreamRequestWasWritten(err) {
		t.Fatalf("first RESUME error = %v, want failed written request", err)
	}
	select {
	case err := <-firstRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not read first RESUME")
	}
	assertSessionOpen(t, clientSession, "client after failed RESUME")
	assertSessionOpen(t, serverSession, "server after failed RESUME")

	serverDone := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err != nil {
			serverDone <- err
			return
		}
		defer stream.Close()
		request, err := ReadFrame(stream, maxPayload)
		if err != nil {
			serverDone <- err
			return
		}
		resumeToken, err := ResumeTokenFromFrame(request)
		if err != nil || resumeToken != token || request.Offset != 3 {
			serverDone <- fmt.Errorf("retry RESUME = token %x generation %d error %v", resumeToken, request.Offset, err)
			return
		}
		serverDone <- WriteFrame(stream, NewResumeResultFrame(request.StreamID, request.Offset, ResumeResultSuccess))
	}()
	retried, maxPayload, err := clientSession.Resume(streamID, token, 3, time.Second)
	if err != nil {
		t.Fatalf("retry RESUME failed: %v", err)
	}
	defer retried.Close()
	if maxPayload != MaxPayloadSize {
		t.Fatalf("retry RESUME max payload = %d, want %d", maxPayload, MaxPayloadSize)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not finish retry RESUME")
	}
	assertSessionOpen(t, clientSession, "client after retry RESUME")
	assertSessionOpen(t, serverSession, "server after retry RESUME")
}

func TestRecoverableOpenProtocolErrorsCloseSession(t *testing.T) {
	tests := []struct {
		name    string
		wantErr error
		respond func(net.Conn, Frame) error
	}{
		{
			name:    "invalid header",
			wantErr: ErrInvalidFrame,
			respond: func(stream net.Conn, request Frame) error {
				return writeMutatedSessionTestFrame(stream, (RecoverableOpenResult{
					Result:                      OpenResultSuccess,
					Generation:                  request.Offset,
					ServerOrphanRetentionMillis: 9000,
				}).Frame(request.StreamID), func(encoded []byte) {
					binary.BigEndian.PutUint16(encoded[2:4], HeaderSize-1)
				})
			},
		},
		{
			name:    "oversized payload",
			wantErr: ErrPayloadLength,
			respond: func(stream net.Conn, request Frame) error {
				return writeMutatedSessionTestFrame(stream, (RecoverableOpenResult{
					Result:                      OpenResultSuccess,
					Generation:                  request.Offset,
					ServerOrphanRetentionMillis: 9000,
				}).Frame(request.StreamID), func(encoded []byte) {
					binary.BigEndian.PutUint32(encoded[4:8], MaxPayloadSize+1)
				})
			},
		},
		{
			name:    "wrong response type",
			wantErr: ErrInvalidFrame,
			respond: func(stream net.Conn, request Frame) error {
				return WriteFrame(stream, NewResumeResultFrame(request.StreamID, 2, ResumeResultSuccess))
			},
		},
		{
			name:    "wrong stream ID",
			wantErr: ErrInvalidFrame,
			respond: func(stream net.Conn, request Frame) error {
				return WriteFrame(stream, (RecoverableOpenResult{
					Result:                      OpenResultSuccess,
					Generation:                  request.Offset,
					ServerOrphanRetentionMillis: 9000,
				}).Frame(sessionTestStreamID(99)))
			},
		},
		{
			name:    "wrong generation",
			wantErr: ErrInvalidFrame,
			respond: func(stream net.Conn, request Frame) error {
				return WriteFrame(stream, (RecoverableOpenResult{
					Result:                      OpenResultSuccess,
					Generation:                  request.Offset + 1,
					ServerOrphanRetentionMillis: 9000,
				}).Frame(request.StreamID))
			},
		},
		{
			name:    "zero retention",
			wantErr: ErrInvalidFrame,
			respond: func(stream net.Conn, request Frame) error {
				return writeMutatedSessionTestFrame(stream, (RecoverableOpenResult{
					Result:                      OpenResultSuccess,
					Generation:                  request.Offset,
					ServerOrphanRetentionMillis: 9000,
				}).Frame(request.StreamID), func(encoded []byte) {
					binary.BigEndian.PutUint32(encoded[HeaderSize+1:HeaderSize+5], 0)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientSession, serverSession := newActiveSessionTestPair(t)
			defer clientSession.Close()
			defer serverSession.Close()
			serverDone := make(chan error, 1)
			go func() {
				stream, maxPayload, err := serverSession.AcceptStream()
				if err != nil {
					serverDone <- err
					return
				}
				request, err := ReadFrame(stream, maxPayload)
				if err == nil {
					err = test.respond(stream, request)
				}
				serverDone <- err
				_ = stream.Close()
			}()

			conn, _, _, err := clientSession.OpenRecoverableDestination(
				sessionTestStreamID(34),
				sessionTestDestination(t, "protocol-error.example:443"),
				ResumeToken{5},
				3*time.Second,
				time.Second,
			)
			if conn != nil {
				_ = conn.Close()
				t.Fatal("protocol-invalid response returned a connection")
			}
			if !errors.Is(err, test.wantErr) || !StreamRequestWasWritten(err) {
				t.Fatalf("recoverable OPEN error = %v, want written request error %v", err, test.wantErr)
			}
			select {
			case <-clientSession.Done():
			default:
				t.Fatal("protocol-invalid recoverable OPEN did not close the physical session")
			}
			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(sessionTestTimeout):
				t.Fatal("server did not finish protocol-invalid recoverable OPEN")
			}
		})
	}
}

func TestResumeGenerationMismatchClosesSession(t *testing.T) {
	clientSession, serverSession := newActiveSessionTestPair(t)
	defer clientSession.Close()
	defer serverSession.Close()

	serverDone := make(chan error, 1)
	go func() {
		stream, maxPayload, err := serverSession.AcceptStream()
		if err != nil {
			serverDone <- err
			return
		}
		request, err := ReadFrame(stream, maxPayload)
		if err == nil {
			err = WriteFrame(stream, NewResumeResultFrame(request.StreamID, request.Offset+1, ResumeResultSuccess))
		}
		serverDone <- err
		_ = stream.Close()
	}()
	conn, _, err := clientSession.Resume(sessionTestStreamID(35), ResumeToken{6}, 2, time.Second)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("generation-mismatched RESUME returned a connection")
	}
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("RESUME error = %v, want %v", err, ErrInvalidFrame)
	}
	select {
	case <-clientSession.Done():
	default:
		t.Fatal("generation-mismatched RESUME did not close the physical session")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sessionTestTimeout):
		t.Fatal("server did not finish generation-mismatched RESUME")
	}
}

func newSessionTestPair(t *testing.T, credentials *PeerCredentials, users map[string]string) (*Session, *Session, string) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, SessionConfig{}, users)
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSessionWithAuth(clientConn, MaxPayloadSize, time.Second, SessionConfig{}, credentials)
	if err != nil {
		t.Fatal(err)
	}
	server := waitSessionAccept(t, serverResult)
	if server.err != nil {
		_ = clientSession.Close()
		t.Fatal(server.err)
	}
	return clientSession, server.session, server.principal
}

func newActiveSessionTestPair(t *testing.T) (*Session, *Session) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	serverResult := make(chan sessionAcceptResult, 1)
	go func() {
		session, principal, err := AcceptSession(serverConn, MaxPayloadSize, time.Second, SessionConfig{
			ActiveStandby:    true,
			ServerInstanceID: ServerInstanceID{1},
			OrphanRetention:  9 * time.Second,
		}, nil)
		serverResult <- sessionAcceptResult{session: session, principal: principal, err: err}
	}()
	clientSession, err := DialSession(clientConn, MaxPayloadSize, time.Second, SessionConfig{ActiveStandby: true})
	if err != nil {
		t.Fatal(err)
	}
	server := waitSessionAccept(t, serverResult)
	if server.err != nil {
		_ = clientSession.Close()
		t.Fatal(server.err)
	}
	return clientSession, server.session
}

func writeMutatedSessionTestFrame(conn net.Conn, frame Frame, mutate func([]byte)) error {
	var encoded bytes.Buffer
	if err := WriteFrame(&encoded, frame); err != nil {
		return err
	}
	mutate(encoded.Bytes())
	return writeFull(conn, encoded.Bytes())
}

func acceptSessionOpen(session *Session, result OpenResult) <-chan sessionOpenResult {
	accepted := make(chan sessionOpenResult, 1)
	go func() {
		stream, maxPayload, err := session.AcceptStream()
		if err != nil {
			accepted <- sessionOpenResult{err: err}
			return
		}
		open, err := ReadFrame(stream, maxPayload)
		if err != nil {
			_ = stream.Close()
			accepted <- sessionOpenResult{err: err}
			return
		}
		destination, err := DecodeDestination(open.Payload)
		if err != nil {
			_ = stream.Close()
			accepted <- sessionOpenResult{err: err}
			return
		}
		if open.Type != FrameOpen {
			_ = stream.Close()
			accepted <- sessionOpenResult{err: fmt.Errorf("OPEN frame type = %d", open.Type)}
			return
		}
		err = WriteFrame(stream, Frame{
			Type:      FrameOpenResult,
			Direction: DirectionServerToClient,
			StreamID:  open.StreamID,
			Payload:   []byte{byte(result)},
		})
		if err != nil || result != OpenResultSuccess {
			_ = stream.Close()
		}
		accepted <- sessionOpenResult{conn: stream, maxPayload: maxPayload, destination: destination, err: err}
	}()
	return accepted
}

func waitSessionAccept(t *testing.T, result <-chan sessionAcceptResult) sessionAcceptResult {
	t.Helper()
	select {
	case accepted := <-result:
		return accepted
	case <-time.After(sessionTestTimeout):
		t.Fatal("timed out waiting for session handshake")
		return sessionAcceptResult{}
	}
}

func waitSessionOpen(t *testing.T, result <-chan sessionOpenResult) sessionOpenResult {
	t.Helper()
	select {
	case accepted := <-result:
		return accepted
	case <-time.After(sessionTestTimeout):
		t.Fatal("timed out waiting for virtual stream OPEN")
		return sessionOpenResult{}
	}
}

func sessionTestDestination(t *testing.T, address string) Destination {
	t.Helper()
	destination, err := ParseDestination(address)
	if err != nil {
		t.Fatal(err)
	}
	return destination
}

func sessionTestStreamID(lastByte byte) StreamID {
	var streamID StreamID
	streamID[len(streamID)-1] = lastByte
	return streamID
}

func setSessionTestDeadline(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(sessionTestTimeout)); err != nil {
		t.Fatal(err)
	}
}

func assertSessionOpen(t *testing.T, session *Session, name string) {
	t.Helper()
	if session.IsClosed() {
		t.Fatalf("%s session is closed", name)
	}
	select {
	case <-session.Done():
		t.Fatalf("%s session Done channel is closed", name)
	default:
	}
}
