package tcpstream

import (
	"errors"
	"fmt"
	"net"
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

type sessionWriteFailureConn struct {
	net.Conn
	err error
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

func TestOpenDestinationTimeoutIncludesOpeningSMUXStream(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	mux, err := smux.Client(clientConn, smuxSessionConfig(SessionConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{mux: mux, maxPayload: MaxPayloadSize}

	started := time.Now()
	conn, _, err := session.OpenDestination(sessionTestStreamID(10), sessionTestDestination(t, "timeout.example:443"), 25*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("timed out OpenStream returned a connection")
	}
	if !errors.Is(err, smux.ErrTimeout) {
		t.Fatalf("OpenDestination error = %v, want %v", err, smux.ErrTimeout)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("OpenDestination timeout took %s, want less than 1s", elapsed)
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

func TestSessionOpenResultTimeoutClosesSessionPromptly(t *testing.T) {
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

	started := time.Now()
	conn, _, err := clientSession.OpenDestination(sessionTestStreamID(5), sessionTestDestination(t, "silent.example:443"), 25*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
		t.Fatal("timed out OPEN_RESULT returned a connection")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("OpenDestination error = %v, want timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("OpenDestination timeout took %s, want less than 1s", elapsed)
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
