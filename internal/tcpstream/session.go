package tcpstream

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/xtaci/smux"
)

const smuxProtocolVersion = 2

type SessionConfig struct {
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
	ReceiveBuffer     int
	StreamBuffer      int
}

// Session multiplexes logical Engarde streams over one physical connection.
type Session struct {
	mux              *smux.Session
	maxPayload       uint32
	clientAcceptDone chan struct{}
}

func DialSession(conn net.Conn, maxPayload uint32, handshakeTimeout time.Duration, config SessionConfig) (*Session, error) {
	return DialSessionWithAuth(conn, maxPayload, handshakeTimeout, config, nil)
}

func DialSessionWithAuth(conn net.Conn, maxPayload uint32, handshakeTimeout time.Duration, config SessionConfig, credentials *PeerCredentials) (*Session, error) {
	if handshakeTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	clientPrefaceBytes, clientPreface, err := marshalPreface(Preface{MaxPayload: maxPayload})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := writeFull(conn, clientPrefaceBytes[:]); err != nil {
		_ = conn.Close()
		return nil, err
	}
	serverPreface, err := ReadPreface(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if serverPreface.Flags&PrefaceFlagAuthRequired != 0 {
		if credentials == nil {
			_ = conn.Close()
			return nil, ErrPeerAuthenticationRequired
		}
		if err := AuthenticatePeerClient(conn, clientPreface, serverPreface, *credentials); err != nil {
			_ = conn.Close()
			return nil, err
		}
	} else if credentials != nil {
		_ = conn.Close()
		return nil, ErrPeerAuthenticationDowngrade
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	mux, err := smux.Client(conn, smuxSessionConfig(config))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	session := &Session{
		mux:              mux,
		maxPayload:       serverPreface.MaxPayload,
		clientAcceptDone: make(chan struct{}),
	}
	go session.monitorClientAccept()
	return session, nil
}

func AcceptSession(conn net.Conn, maxPayload uint32, handshakeTimeout time.Duration, config SessionConfig, users map[string]string) (*Session, string, error) {
	if handshakeTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
			_ = conn.Close()
			return nil, "", err
		}
	}
	clientPreface, err := ReadPreface(conn)
	if err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	if clientPreface.Flags != 0 {
		_ = conn.Close()
		return nil, "", ErrInvalidPreface
	}
	serverPreface := Preface{MaxPayload: maxPayload}
	if users != nil {
		serverPreface.Flags = PrefaceFlagAuthRequired
	}
	if err := WritePreface(conn, serverPreface); err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	principal := ""
	if users != nil {
		principal, err = AuthenticatePeerServer(conn, clientPreface, serverPreface, users)
		if err != nil {
			_ = conn.Close()
			return nil, "", err
		}
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	mux, err := smux.Server(conn, smuxSessionConfig(config))
	if err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	return &Session{mux: mux, maxPayload: clientPreface.MaxPayload}, principal, nil
}

func smuxSessionConfig(config SessionConfig) *smux.Config {
	defaults := smux.DefaultConfig()
	keepaliveInterval := config.KeepaliveInterval
	if keepaliveInterval <= 0 {
		keepaliveInterval = defaults.KeepAliveInterval
	}
	keepaliveTimeout := config.KeepaliveTimeout
	if keepaliveTimeout <= 0 {
		keepaliveTimeout = defaults.KeepAliveTimeout
	}
	receiveBuffer := config.ReceiveBuffer
	if receiveBuffer <= 0 {
		receiveBuffer = 4 * 1024 * 1024
	}
	streamBuffer := config.StreamBuffer
	if streamBuffer <= 0 {
		streamBuffer = 1024 * 1024
	}
	if streamBuffer > receiveBuffer {
		receiveBuffer = streamBuffer
	}
	return &smux.Config{
		Version:           smuxProtocolVersion,
		KeepAliveInterval: keepaliveInterval,
		KeepAliveTimeout:  keepaliveTimeout,
		MaxFrameSize:      32 * 1024,
		MaxReceiveBuffer:  receiveBuffer,
		MaxStreamBuffer:   streamBuffer,
	}
}

func (session *Session) OpenDestination(streamID StreamID, destination Destination, timeout time.Duration) (net.Conn, uint32, error) {
	payload, err := destination.Encode()
	if err != nil {
		return nil, 0, err
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	stream, err := session.openStream(timeout)
	if err != nil {
		_ = session.Close()
		return nil, 0, err
	}
	succeeded := false
	keepSession := false
	defer func() {
		if succeeded {
			return
		}
		if keepSession {
			// smux Stream.Close has its own fixed 30s control-write timeout.
			// Do not make a completed OPEN rejection hold up the caller.
			go func() { _ = stream.Close() }()
			return
		}
		_ = session.Close()
		_ = stream.Close()
	}()
	if !deadline.IsZero() {
		if err := stream.SetDeadline(deadline); err != nil {
			return nil, 0, err
		}
	}
	if err := WriteFrame(stream, Frame{Type: FrameOpen, Direction: DirectionClientToServer, StreamID: streamID, Payload: payload}); err != nil {
		return nil, 0, err
	}
	frame, err := ReadFrame(stream, session.maxPayload)
	if err != nil {
		return nil, 0, err
	}
	if frame.Type != FrameOpenResult || frame.StreamID != streamID {
		return nil, 0, ErrInvalidFrame
	}
	result := OpenResult(frame.Payload[0])
	if result != OpenResultSuccess {
		keepSession = true
		return nil, 0, &OpenError{Result: result}
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		return nil, 0, err
	}
	succeeded = true
	return stream, session.maxPayload, nil
}

func (session *Session) openStream(timeout time.Duration) (*smux.Stream, error) {
	if timeout <= 0 {
		return session.mux.OpenStream()
	}
	type result struct {
		stream *smux.Stream
		err    error
	}
	opened := make(chan result, 1)
	go func() {
		stream, err := session.mux.OpenStream()
		opened <- result{stream: stream, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case opened := <-opened:
		return opened.stream, opened.err
	case <-timer.C:
		// A blocked smux control write makes the entire physical Session
		// unusable, so fail all virtual streams and let the path reconnect.
		_ = session.mux.Close()
		return nil, smux.ErrTimeout
	}
}

func (session *Session) AcceptStream() (net.Conn, uint32, error) {
	stream, err := session.mux.AcceptStream()
	if err != nil {
		return nil, 0, err
	}
	return stream, session.maxPayload, nil
}

func (session *Session) Done() <-chan struct{} {
	return session.mux.CloseChan()
}

func (session *Session) IsClosed() bool {
	return session.mux.IsClosed()
}

func (session *Session) StreamCount() int {
	return session.mux.NumStreams()
}

func (session *Session) Close() error {
	if session == nil || session.mux == nil {
		return nil
	}
	err := session.mux.Close()
	if session.clientAcceptDone != nil {
		<-session.clientAcceptDone
	}
	return err
}

func (session *Session) monitorClientAccept() {
	defer close(session.clientAcceptDone)

	// Engarde streams are client-initiated. AcceptStream is used here only to
	// surface socket/protocol errors and reject an unexpected server stream.
	_, _ = session.mux.AcceptStream()
	_ = session.mux.Close()
}

func IsSessionClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, smux.ErrGoAway)
}
