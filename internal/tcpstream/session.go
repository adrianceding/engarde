package tcpstream

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xtaci/smux"
)

const smuxProtocolVersion = 2

var newSessionOpenTimer = func(delay time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(delay)
	return timer.C, func() { timer.Stop() }
}

var sessionOpenNow = time.Now
var sessionProbeNow = time.Now

type SessionConfig struct {
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
	ReceiveBuffer     int
	StreamBuffer      int
	ActiveStandby     bool
	ServerInstanceID  ServerInstanceID
	OrphanRetention   time.Duration
}

// Session multiplexes logical Engarde streams over one physical connection.
type Session struct {
	mux              *smux.Session
	maxPayload       uint32
	activeStandby    bool
	serverInstanceID ServerInstanceID
	orphanRetention  time.Duration
	clientAcceptDone chan struct{}
}

type SessionProbe struct {
	conn       net.Conn
	maxPayload uint32
	mu         sync.Mutex
	nonce      uint64
}

type streamRequestMode uint8

const (
	streamRequestStrict streamRequestMode = iota
	streamRequestRecoverable
)

var (
	ErrActiveStandbyRequired    = errors.New("peer does not support active-standby TCP carriers")
	ErrInvalidSessionCapability = errors.New("invalid active-standby session capability")
)

type StreamRequestError struct {
	Err     error
	Written bool
}

func (err *StreamRequestError) Error() string {
	return err.Err.Error()
}

func (err *StreamRequestError) Unwrap() error {
	return err.Err
}

func StreamRequestWasWritten(err error) bool {
	var requestErr *StreamRequestError
	return errors.As(err, &requestErr) && requestErr.Written
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
	clientFlags := uint8(0)
	if config.ActiveStandby {
		clientFlags |= PrefaceFlagActiveStandby
	}
	clientPrefaceBytes, clientPreface, err := marshalPreface(Preface{Flags: clientFlags, MaxPayload: maxPayload})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := writeFull(conn, clientPrefaceBytes); err != nil {
		_ = conn.Close()
		return nil, err
	}
	serverPreface, err := ReadPreface(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	serverActiveStandby := serverPreface.Flags&PrefaceFlagActiveStandby != 0
	if config.ActiveStandby != serverActiveStandby {
		_ = conn.Close()
		return nil, ErrActiveStandbyRequired
	}
	if serverActiveStandby && (serverPreface.ServerInstanceID == (ServerInstanceID{}) || serverPreface.ServerOrphanRetentionMillis == 0) {
		_ = conn.Close()
		return nil, ErrInvalidSessionCapability
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
		activeStandby:    serverActiveStandby,
		serverInstanceID: serverPreface.ServerInstanceID,
		orphanRetention:  time.Duration(serverPreface.ServerOrphanRetentionMillis) * time.Millisecond,
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
	if clientPreface.Flags&PrefaceFlagAuthRequired != 0 {
		_ = conn.Close()
		return nil, "", ErrInvalidPreface
	}
	if clientPreface.ServerInstanceID != (ServerInstanceID{}) || clientPreface.ServerOrphanRetentionMillis != 0 {
		_ = conn.Close()
		return nil, "", ErrInvalidSessionCapability
	}
	serverPreface := Preface{MaxPayload: maxPayload}
	if users != nil {
		serverPreface.Flags |= PrefaceFlagAuthRequired
	}
	clientActiveStandby := clientPreface.Flags&PrefaceFlagActiveStandby != 0
	if clientActiveStandby && config.ActiveStandby {
		retentionMillis, ok := durationMillisUint32(config.OrphanRetention)
		if !ok || config.ServerInstanceID == (ServerInstanceID{}) {
			_ = conn.Close()
			return nil, "", ErrInvalidSessionCapability
		}
		serverPreface.Flags |= PrefaceFlagActiveStandby
		serverPreface.ServerInstanceID = config.ServerInstanceID
		serverPreface.ServerOrphanRetentionMillis = retentionMillis
	}
	if err := WritePreface(conn, serverPreface); err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	if clientActiveStandby && !config.ActiveStandby {
		_ = conn.Close()
		return nil, "", ErrActiveStandbyRequired
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
	return &Session{
		mux:              mux,
		maxPayload:       clientPreface.MaxPayload,
		activeStandby:    clientActiveStandby,
		serverInstanceID: serverPreface.ServerInstanceID,
		orphanRetention:  time.Duration(serverPreface.ServerOrphanRetentionMillis) * time.Millisecond,
	}, principal, nil
}

func durationMillisUint32(duration time.Duration) (uint32, bool) {
	if duration <= 0 || duration%time.Millisecond != 0 {
		return 0, false
	}
	millis := duration / time.Millisecond
	if millis > time.Duration(^uint32(0)) {
		return 0, false
	}
	return uint32(millis), true
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
	stream, response, err := session.exchangeStream(Frame{Type: FrameOpen, Direction: DirectionClientToServer, StreamID: streamID, Payload: payload}, FrameOpenResult, timeout, streamRequestStrict)
	if err != nil {
		return nil, 0, err
	}
	result := OpenResult(response.Payload[0])
	if result != OpenResultSuccess {
		closeRejectedStream(stream)
		return nil, 0, &OpenError{Result: result}
	}
	return stream, session.maxPayload, nil
}

func (session *Session) OpenRecoverableDestination(streamID StreamID, destination Destination, token ResumeToken, recoveryTimeout, timeout time.Duration) (net.Conn, uint32, RecoverableOpenResult, error) {
	if !session.activeStandby {
		return nil, 0, RecoverableOpenResult{}, ErrActiveStandbyRequired
	}
	recoveryMillis, ok := durationMillisUint32(recoveryTimeout)
	if !ok {
		return nil, 0, RecoverableOpenResult{}, ErrInvalidSessionCapability
	}
	request, err := (RecoverableOpen{Destination: destination, ResumeToken: token, Generation: 1, RecoveryTimeoutMillis: recoveryMillis}).Frame(streamID)
	if err != nil {
		return nil, 0, RecoverableOpenResult{}, err
	}
	stream, response, err := session.exchangeStream(request, FrameRecoverableOpenResult, timeout, streamRequestRecoverable)
	if err != nil {
		return nil, 0, RecoverableOpenResult{}, err
	}
	result, err := DecodeRecoverableOpenResult(response)
	if err != nil || result.Generation != request.Offset {
		session.closeProtocolStream(stream)
		return nil, 0, RecoverableOpenResult{}, &StreamRequestError{Err: ErrInvalidFrame, Written: true}
	}
	if result.Result != OpenResultSuccess {
		closeRejectedStream(stream)
		return nil, 0, result, &OpenError{Result: result.Result}
	}
	if result.ServerOrphanRetentionMillis == 0 {
		session.closeProtocolStream(stream)
		return nil, 0, RecoverableOpenResult{}, &StreamRequestError{Err: ErrInvalidFrame, Written: true}
	}
	return stream, session.maxPayload, result, nil
}

func (session *Session) Resume(streamID StreamID, token ResumeToken, generation uint64, timeout time.Duration) (net.Conn, uint32, error) {
	if !session.activeStandby {
		return nil, 0, ErrActiveStandbyRequired
	}
	request := NewResumeFrame(streamID, token, generation)
	stream, response, err := session.exchangeStream(request, FrameResumeResult, timeout, streamRequestRecoverable)
	if err != nil {
		return nil, 0, err
	}
	if response.Offset != generation {
		session.closeProtocolStream(stream)
		return nil, 0, ErrInvalidFrame
	}
	result := ResumeResult(response.Payload[0])
	if result != ResumeResultSuccess {
		closeRejectedStream(stream)
		return nil, 0, &ResumeError{Result: result}
	}
	return stream, session.maxPayload, nil
}

func (session *Session) OpenProbe(timeout time.Duration) (*SessionProbe, error) {
	if !session.activeStandby {
		return nil, ErrActiveStandbyRequired
	}
	stream, err := session.openStream(timeout)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	return &SessionProbe{conn: stream, maxPayload: session.maxPayload}, nil
}

func (probe *SessionProbe) Ping(timeout time.Duration) (time.Duration, error) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.nonce++
	if probe.nonce == 0 {
		probe.nonce = 1
	}
	startedAt := sessionProbeNow()
	if timeout > 0 {
		if err := probe.conn.SetDeadline(startedAt.Add(timeout)); err != nil {
			return 0, err
		}
	}
	request := Frame{Type: FramePing, Direction: DirectionClientToServer, Offset: probe.nonce}
	if err := WriteFrame(probe.conn, request); err != nil {
		return 0, err
	}
	response, err := ReadFrame(probe.conn, probe.maxPayload)
	if err != nil {
		return 0, err
	}
	if response.Type != FramePong || response.Offset != probe.nonce {
		return 0, ErrInvalidFrame
	}
	if err := probe.conn.SetDeadline(time.Time{}); err != nil {
		return 0, err
	}
	return sessionProbeNow().Sub(startedAt), nil
}

func (probe *SessionProbe) Close() error {
	if probe == nil || probe.conn == nil {
		return nil
	}
	return probe.conn.Close()
}

func (session *Session) exchangeStream(request Frame, responseType FrameType, timeout time.Duration, mode streamRequestMode) (net.Conn, Frame, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = sessionOpenNow().Add(timeout)
	}
	stream, err := session.openStream(timeout)
	if err != nil {
		_ = session.Close()
		return nil, Frame{}, err
	}
	succeeded := false
	requestWritten := false
	closeSessionOnFailure := mode != streamRequestRecoverable
	defer func() {
		if succeeded {
			return
		}
		if closeSessionOnFailure {
			_ = session.Close()
			_ = stream.Close()
			return
		}
		closeRejectedStream(stream)
	}()
	if !deadline.IsZero() {
		if err := stream.SetDeadline(deadline); err != nil {
			return nil, Frame{}, &StreamRequestError{Err: err}
		}
	}
	requestWritten = true
	if err := WriteFrame(stream, request); err != nil {
		// A write error may originate from the shared smux transport rather
		// than this child stream, so the physical Session is no longer safe.
		closeSessionOnFailure = true
		return nil, Frame{}, &StreamRequestError{Err: err, Written: requestWritten}
	}
	response, err := ReadFrame(stream, session.maxPayload)
	if err != nil {
		if errors.Is(err, ErrInvalidFrame) || errors.Is(err, ErrPayloadLength) {
			closeSessionOnFailure = true
		}
		return nil, Frame{}, &StreamRequestError{Err: err, Written: requestWritten}
	}
	if response.Type != responseType || response.StreamID != request.StreamID {
		closeSessionOnFailure = true
		return nil, Frame{}, &StreamRequestError{Err: ErrInvalidFrame, Written: requestWritten}
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		return nil, Frame{}, &StreamRequestError{Err: err, Written: requestWritten}
	}
	succeeded = true
	return stream, response, nil
}

func closeRejectedStream(stream net.Conn) {
	// smux Stream.Close has its own fixed 30s control-write timeout. A completed
	// application rejection must not hold up the caller.
	go func() { _ = stream.Close() }()
}

func (session *Session) closeProtocolStream(stream net.Conn) {
	_ = session.Close()
	_ = stream.Close()
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
	ticks, stopTimer := newSessionOpenTimer(timeout)
	defer stopTimer()
	select {
	case opened := <-opened:
		return opened.stream, opened.err
	case <-ticks:
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

func (session *Session) ActiveStandby() bool {
	return session.activeStandby
}

func (session *Session) ServerInstanceID() ServerInstanceID {
	return session.serverInstanceID
}

func (session *Session) ServerOrphanRetention() time.Duration {
	return session.orphanRetention
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
