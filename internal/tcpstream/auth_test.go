package tcpstream

import (
	"bytes"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

var errPeerAuthTestIO = errors.New("injected peer auth I/O failure")

type peerAuthTestReadWriter struct {
	reader io.Reader
	writer io.Writer
}

func (stream *peerAuthTestReadWriter) Read(payload []byte) (int, error) {
	return stream.reader.Read(payload)
}

func (stream *peerAuthTestReadWriter) Write(payload []byte) (int, error) {
	return stream.writer.Write(payload)
}

type peerAuthFragmentWriter struct {
	bytes.Buffer
	maximum int
	calls   int
}

func (writer *peerAuthFragmentWriter) Write(payload []byte) (int, error) {
	writer.calls++
	if len(payload) > writer.maximum {
		payload = payload[:writer.maximum]
	}
	return writer.Buffer.Write(payload)
}

type peerAuthErrorReader struct{}

func (peerAuthErrorReader) Read([]byte) (int, error) {
	return 0, errPeerAuthTestIO
}

type peerAuthFailAfterWriter struct {
	remaining int
}

func (writer *peerAuthFailAfterWriter) Write(payload []byte) (int, error) {
	if writer.remaining == 0 {
		return 0, errPeerAuthTestIO
	}
	if len(payload) > writer.remaining {
		payload = payload[:writer.remaining]
	}
	writer.remaining -= len(payload)
	return len(payload), nil
}

func TestPeerAuthenticationRoundTrip(t *testing.T) {
	clientConn, serverConn := peerAuthPipe(t)
	defer clientConn.Close()
	defer serverConn.Close()
	clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
	serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
	type serverResult struct {
		principal string
		err       error
	}
	serverResults := make(chan serverResult, 1)
	go func() {
		principal, err := AuthenticatePeerServer(serverConn, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
		serverResults <- serverResult{principal: principal, err: err}
	}()
	if err := AuthenticatePeerClient(clientConn, clientPreface, serverPreface, PeerCredentials{Username: "client-a", Password: "peer-secret"}); err != nil {
		t.Fatal(err)
	}
	result := <-serverResults
	if result.err != nil || result.principal != "client-a" {
		t.Fatalf("server result = %q/%v", result.principal, result.err)
	}
}

func TestPeerAuthenticationRejectsEveryTruncatedRecordPrefix(t *testing.T) {
	clientPreface, serverPreface := peerAuthTestPrefaces()
	challenge := peerAuthTestChallenge()
	result := peerAuthTestResult(peerAuthFailure)
	response := peerAuthTestResponse([]byte("client-a"))
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}

	t.Run("client challenge", func(t *testing.T) {
		for length := 0; length < len(challenge); length++ {
			stream := &peerAuthTestReadWriter{
				reader: bytes.NewReader(challenge[:length]),
				writer: io.Discard,
			}
			err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
			assertPeerAuthTruncationError(t, length, err)
		}
	})

	t.Run("client result", func(t *testing.T) {
		for length := 0; length < len(result); length++ {
			stream := &peerAuthTestReadWriter{
				reader: io.MultiReader(bytes.NewReader(challenge), bytes.NewReader(result[:length])),
				writer: io.Discard,
			}
			err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
			assertPeerAuthTruncationError(t, length, err)
		}
	})

	t.Run("server response", func(t *testing.T) {
		for length := 0; length < len(response); length++ {
			stream := &peerAuthTestReadWriter{
				reader: bytes.NewReader(response[:length]),
				writer: io.Discard,
			}
			_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
			assertPeerAuthTruncationError(t, length, err)
		}
	})
}

func TestPeerAuthenticationRejectsMalformedRecordHeaders(t *testing.T) {
	clientPreface, serverPreface := peerAuthTestPrefaces()
	challenge := peerAuthTestChallenge()
	result := peerAuthTestResult(peerAuthFailure)
	response := peerAuthTestResponse([]byte("client-a"))
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}

	clientTests := []struct {
		name      string
		challenge []byte
		result    []byte
	}{
		{name: "challenge magic", challenge: peerAuthMutate(challenge, 0, 0xff)},
		{name: "challenge version", challenge: peerAuthMutate(challenge, 4, peerAuthVersion+1)},
		{name: "challenge type", challenge: peerAuthMutate(challenge, 5, peerAuthResponseType)},
		{name: "challenge length", challenge: peerAuthMutate(challenge, 6, 1)},
		{name: "challenge reserved", challenge: peerAuthMutate(challenge, 7, 1)},
		{name: "result magic", challenge: challenge, result: peerAuthMutate(result, 0, 0xff)},
		{name: "result version", challenge: challenge, result: peerAuthMutate(result, 4, peerAuthVersion+1)},
		{name: "result type", challenge: challenge, result: peerAuthMutate(result, 5, peerAuthChallengeType)},
		{name: "result status", challenge: challenge, result: peerAuthMutate(result, 6, peerAuthFailure+1)},
		{name: "result reserved", challenge: challenge, result: peerAuthMutate(result, 7, 1)},
	}
	for _, test := range clientTests {
		t.Run("client "+test.name, func(t *testing.T) {
			input := test.challenge
			if test.result != nil {
				input = append(append([]byte(nil), test.challenge...), test.result...)
			}
			stream := &peerAuthTestReadWriter{reader: bytes.NewReader(input), writer: io.Discard}
			err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
			if !errors.Is(err, ErrInvalidPeerAuth) {
				t.Fatalf("AuthenticatePeerClient error = %v, want %v", err, ErrInvalidPeerAuth)
			}
		})
	}

	serverTests := []struct {
		name     string
		response []byte
		want     error
	}{
		{name: "magic", response: peerAuthMutate(response, 0, 0xff), want: ErrInvalidPeerAuth},
		{name: "version", response: peerAuthMutate(response, 4, peerAuthVersion+1), want: ErrInvalidPeerAuth},
		{name: "type", response: peerAuthMutate(response, 5, peerAuthResultType), want: ErrInvalidPeerAuth},
		{name: "zero username length", response: peerAuthMutate(response, 6, 0), want: ErrInvalidPeerAuth},
		{name: "reserved", response: peerAuthMutate(response, 7, 1), want: ErrInvalidPeerAuth},
		{name: "declared username too long", response: peerAuthMutate(response, 6, response[6]+1), want: io.ErrUnexpectedEOF},
	}
	for _, test := range serverTests {
		t.Run("server "+test.name, func(t *testing.T) {
			stream := &peerAuthTestReadWriter{reader: bytes.NewReader(test.response), writer: io.Discard}
			_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
			if !errors.Is(err, test.want) {
				t.Fatalf("AuthenticatePeerServer error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPeerAuthenticationHandlesFragmentedIO(t *testing.T) {
	clientPreface, serverPreface := peerAuthTestPrefaces()
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}

	t.Run("client", func(t *testing.T) {
		input := append(peerAuthTestChallenge(), peerAuthTestResult(peerAuthFailure)...)
		writer := &peerAuthFragmentWriter{maximum: 1}
		stream := &peerAuthTestReadWriter{
			reader: iotest.OneByteReader(bytes.NewReader(input)),
			writer: writer,
		}
		err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
		if !errors.Is(err, ErrPeerAuthenticationFailed) {
			t.Fatalf("AuthenticatePeerClient error = %v, want %v", err, ErrPeerAuthenticationFailed)
		}
		if writer.calls <= 1 {
			t.Fatalf("response writer calls = %d, want fragmented writes", writer.calls)
		}
		if _, _, _, err := readPeerAuthResponse(bytes.NewReader(writer.Bytes())); err != nil {
			t.Fatalf("fragmented client response: %v", err)
		}
	})

	t.Run("server", func(t *testing.T) {
		writer := &peerAuthFragmentWriter{maximum: 1}
		stream := &peerAuthTestReadWriter{
			reader: iotest.OneByteReader(bytes.NewReader(peerAuthTestResponse([]byte("client-a")))),
			writer: writer,
		}
		_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
		if !errors.Is(err, ErrPeerAuthenticationFailed) {
			t.Fatalf("AuthenticatePeerServer error = %v, want %v", err, ErrPeerAuthenticationFailed)
		}
		if writer.calls <= 2 {
			t.Fatalf("server writer calls = %d, want fragmented writes", writer.calls)
		}
		wire := writer.Bytes()
		if len(wire) != peerAuthChallengeSize+peerAuthResultSize {
			t.Fatalf("server output length = %d, want %d", len(wire), peerAuthChallengeSize+peerAuthResultSize)
		}
		if !validPeerAuthRecord(wire[:peerAuthChallengeSize], peerAuthChallengeType) {
			t.Fatalf("fragmented server challenge = %x", wire[:peerAuthChallengeSize])
		}
		if !validPeerAuthRecord(wire[peerAuthChallengeSize:], peerAuthResultType) {
			t.Fatalf("fragmented server result = %x", wire[peerAuthChallengeSize:])
		}
	})
}

func TestPeerAuthenticationPropagatesInjectedIOErrors(t *testing.T) {
	clientPreface, serverPreface := peerAuthTestPrefaces()
	challenge := peerAuthTestChallenge()
	response := peerAuthTestResponse([]byte("client-a"))
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}

	clientReadTests := []struct {
		name  string
		input []byte
	}{
		{name: "challenge", input: challenge[:7]},
		{name: "result", input: append(append([]byte(nil), challenge...), peerAuthTestResult(peerAuthFailure)[:7]...)},
	}
	for _, test := range clientReadTests {
		t.Run("client read "+test.name, func(t *testing.T) {
			stream := &peerAuthTestReadWriter{
				reader: io.MultiReader(bytes.NewReader(test.input), peerAuthErrorReader{}),
				writer: io.Discard,
			}
			err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
			if !errors.Is(err, errPeerAuthTestIO) {
				t.Fatalf("AuthenticatePeerClient error = %v, want injected error", err)
			}
		})
	}

	t.Run("client write response", func(t *testing.T) {
		stream := &peerAuthTestReadWriter{
			reader: bytes.NewReader(challenge),
			writer: &peerAuthFailAfterWriter{},
		}
		err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials)
		if !errors.Is(err, errPeerAuthTestIO) {
			t.Fatalf("AuthenticatePeerClient error = %v, want injected error", err)
		}
	})

	serverReadCuts := []struct {
		name string
		cut  int
	}{
		{name: "header", cut: 3},
		{name: "username", cut: peerAuthResponseHeaderSize + 3},
		{name: "nonce", cut: peerAuthResponseHeaderSize + len("client-a") + 3},
		{name: "MAC", cut: peerAuthResponseHeaderSize + len("client-a") + peerAuthNonceSize + 3},
	}
	for _, test := range serverReadCuts {
		t.Run("server read "+test.name, func(t *testing.T) {
			stream := &peerAuthTestReadWriter{
				reader: io.MultiReader(bytes.NewReader(response[:test.cut]), peerAuthErrorReader{}),
				writer: io.Discard,
			}
			_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
			if !errors.Is(err, errPeerAuthTestIO) {
				t.Fatalf("AuthenticatePeerServer error = %v, want injected error", err)
			}
		})
	}

	t.Run("server write challenge", func(t *testing.T) {
		stream := &peerAuthTestReadWriter{reader: bytes.NewReader(response), writer: &peerAuthFailAfterWriter{}}
		_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
		if !errors.Is(err, errPeerAuthTestIO) {
			t.Fatalf("AuthenticatePeerServer error = %v, want injected error", err)
		}
	})

	t.Run("server write result", func(t *testing.T) {
		stream := &peerAuthTestReadWriter{
			reader: bytes.NewReader(response),
			writer: &peerAuthFailAfterWriter{remaining: peerAuthChallengeSize},
		}
		_, err := AuthenticatePeerServer(stream, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
		if !errors.Is(err, errPeerAuthTestIO) {
			t.Fatalf("AuthenticatePeerServer error = %v, want injected error", err)
		}
	})
}

func TestPeerAuthenticationUsernameLengthBoundary(t *testing.T) {
	username := strings.Repeat("u", 255)
	clientConn, serverConn := peerAuthPipe(t)
	clientPreface, serverPreface := peerAuthTestPrefaces()
	serverResults := make(chan error, 1)
	go func() {
		principal, err := AuthenticatePeerServer(serverConn, clientPreface, serverPreface, map[string]string{username: "peer-secret"})
		if err == nil && principal != username {
			err = errors.New("server returned wrong principal")
		}
		serverResults <- err
	}()
	clientErr := AuthenticatePeerClient(clientConn, clientPreface, serverPreface, PeerCredentials{Username: username, Password: "peer-secret"})
	serverErr := <-serverResults
	_ = clientConn.Close()
	_ = serverConn.Close()
	if clientErr != nil || serverErr != nil {
		t.Fatalf("255-byte username errors = %v/%v", clientErr, serverErr)
	}

	for _, credentials := range []PeerCredentials{
		{Username: strings.Repeat("u", 256), Password: "peer-secret"},
		{Username: "client-a", Password: ""},
	} {
		stream := &peerAuthTestReadWriter{reader: peerAuthErrorReader{}, writer: &peerAuthFailAfterWriter{}}
		if err := AuthenticatePeerClient(stream, clientPreface, serverPreface, credentials); !errors.Is(err, ErrInvalidPeerAuth) {
			t.Fatalf("credentials with username length %d and password length %d: error = %v, want %v", len(credentials.Username), len(credentials.Password), err, ErrInvalidPeerAuth)
		}
	}
}

func TestPeerAuthenticationRejectsInvalidCredentials(t *testing.T) {
	tests := []PeerCredentials{
		{Username: "client-a", Password: "wrong"},
		{Username: "unknown", Password: "peer-secret"},
	}
	for _, credentials := range tests {
		clientConn, serverConn := peerAuthPipe(t)
		clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
		serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
		serverErrors := make(chan error, 1)
		go func() {
			_, err := AuthenticatePeerServer(serverConn, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
			serverErrors <- err
		}()
		clientErr := AuthenticatePeerClient(clientConn, clientPreface, serverPreface, credentials)
		serverErr := <-serverErrors
		_ = clientConn.Close()
		_ = serverConn.Close()
		if !errors.Is(clientErr, ErrPeerAuthenticationFailed) || !errors.Is(serverErr, ErrPeerAuthenticationFailed) {
			t.Fatalf("credentials %q errors = %v/%v", credentials.Username, clientErr, serverErr)
		}
	}
}

func TestPeerAuthenticationFailureDoesNotExposePasswordDerivedProof(t *testing.T) {
	firstProof, firstPredictable := capturePeerAuthenticationFailure(t)
	secondProof, secondPredictable := capturePeerAuthenticationFailure(t)
	if hmac.Equal(firstProof, firstPredictable) || hmac.Equal(secondProof, secondPredictable) {
		t.Fatal("peer auth failure exposed a password-derived proof")
	}
	if hmac.Equal(firstProof, secondProof) {
		t.Fatal("peer auth failure reused fixed proof padding")
	}
}

func capturePeerAuthenticationFailure(t *testing.T) ([]byte, []byte) {
	t.Helper()
	clientConn, serverConn := peerAuthPipe(t)
	defer clientConn.Close()
	defer serverConn.Close()
	clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
	serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
	serverErrors := make(chan error, 1)
	go func() {
		_, err := AuthenticatePeerServer(serverConn, clientPreface, serverPreface, map[string]string{"client-a": "peer-secret"})
		serverErrors <- err
	}()

	challenge := make([]byte, peerAuthChallengeSize)
	if _, err := io.ReadFull(clientConn, challenge); err != nil {
		t.Fatal(err)
	}
	username := []byte("client-a")
	clientNonce := make([]byte, peerAuthNonceSize)
	clientNonce[len(clientNonce)-1] = 1
	invalidMAC := make([]byte, peerAuthMACSize)
	response := make([]byte, peerAuthResponseHeaderSize+len(username)+peerAuthNonceSize+peerAuthMACSize)
	binary.BigEndian.PutUint32(response[0:4], peerAuthMagic)
	response[4] = peerAuthVersion
	response[5] = peerAuthResponseType
	response[6] = byte(len(username))
	copy(response[peerAuthResponseHeaderSize:], username)
	copy(response[peerAuthResponseHeaderSize+len(username):], clientNonce)
	copy(response[len(response)-peerAuthMACSize:], invalidMAC)
	if err := writeFull(clientConn, response); err != nil {
		t.Fatal(err)
	}
	result := make([]byte, peerAuthResultSize)
	if _, err := io.ReadFull(clientConn, result); err != nil {
		t.Fatal(err)
	}
	if !validPeerAuthRecord(result, peerAuthResultType) || result[6] != peerAuthFailure {
		t.Fatalf("peer auth failure result = %x", result)
	}
	predictable, err := peerAuthServerProof(clientPreface, serverPreface, challenge, clientNonce, username, invalidMAC, peerAuthFailure, []byte("peer-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverErrors; !errors.Is(err, ErrPeerAuthenticationFailed) {
		t.Fatalf("server error = %v, want authentication failure", err)
	}
	return append([]byte(nil), result[peerAuthResultHeaderSize:]...), append([]byte(nil), predictable[:]...)
}

func TestPeerAuthMACBindsTranscript(t *testing.T) {
	challenge := make([]byte, peerAuthChallengeSize)
	binary.BigEndian.PutUint32(challenge[0:4], peerAuthMagic)
	challenge[4] = peerAuthVersion
	challenge[5] = peerAuthChallengeType
	challenge[len(challenge)-1] = 1
	clientNonce := make([]byte, peerAuthNonceSize)
	clientNonce[len(clientNonce)-1] = 1
	clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
	serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
	first, err := peerAuthMAC(clientPreface, serverPreface, challenge, clientNonce, []byte("client-a"), []byte("peer-secret"))
	if err != nil {
		t.Fatal(err)
	}
	changedChallenge := append([]byte(nil), challenge...)
	changedChallenge[len(changedChallenge)-1] = 2
	second, err := peerAuthMAC(clientPreface, serverPreface, changedChallenge, clientNonce, []byte("client-a"), []byte("peer-secret"))
	if err != nil {
		t.Fatal(err)
	}
	changedNonce := append([]byte(nil), clientNonce...)
	changedNonce[len(changedNonce)-1] = 2
	third, err := peerAuthMAC(clientPreface, serverPreface, challenge, changedNonce, []byte("client-a"), []byte("peer-secret"))
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := peerAuthMAC(clientPreface, serverPreface, challenge, clientNonce, []byte("client-b"), []byte("peer-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if hmac.Equal(first[:], second[:]) || hmac.Equal(first[:], third[:]) || hmac.Equal(first[:], fourth[:]) {
		t.Fatal("peer auth MAC did not bind challenge, client nonce, and username")
	}
}

func TestPeerAuthenticationRejectsForgedServerSuccess(t *testing.T) {
	clientConn, serverConn := peerAuthPipe(t)
	defer clientConn.Close()
	defer serverConn.Close()
	clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
	serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
	go func() {
		challenge := make([]byte, peerAuthChallengeSize)
		binary.BigEndian.PutUint32(challenge[0:4], peerAuthMagic)
		challenge[4] = peerAuthVersion
		challenge[5] = peerAuthChallengeType
		challenge[len(challenge)-1] = 1
		_ = writeFull(serverConn, challenge)
		header := make([]byte, peerAuthResponseHeaderSize)
		_, _ = io.ReadFull(serverConn, header)
		username := make([]byte, int(header[6]))
		_, _ = io.ReadFull(serverConn, username)
		clientNonce := make([]byte, peerAuthNonceSize)
		_, _ = io.ReadFull(serverConn, clientNonce)
		clientMAC := make([]byte, peerAuthMACSize)
		_, _ = io.ReadFull(serverConn, clientMAC)
		_ = writePeerAuthResult(serverConn, peerAuthSuccess, [peerAuthMACSize]byte{})
	}()
	err := AuthenticatePeerClient(clientConn, clientPreface, serverPreface, PeerCredentials{Username: "client-a", Password: "peer-secret"})
	if !errors.Is(err, ErrPeerAuthenticationFailed) {
		t.Fatalf("forged server success error = %v", err)
	}
}

func TestPeerAuthenticationRejectsReplayedServerSuccess(t *testing.T) {
	clientPreface := Preface{Version: Version, MaxPayload: MaxPayloadSize}
	serverPreface := Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}
	challenge := make([]byte, peerAuthChallengeSize)
	binary.BigEndian.PutUint32(challenge[0:4], peerAuthMagic)
	challenge[4] = peerAuthVersion
	challenge[5] = peerAuthChallengeType
	challenge[len(challenge)-1] = 1

	firstClient, firstServer := peerAuthPipe(t)
	var recordedNonce []byte
	var recordedResult []byte
	firstServerErrors := make(chan error, 1)
	go func() {
		if err := writeFull(firstServer, challenge); err != nil {
			firstServerErrors <- err
			return
		}
		username, clientNonce, clientMAC, err := readPeerAuthResponse(firstServer)
		if err != nil {
			firstServerErrors <- err
			return
		}
		proof, err := peerAuthServerProof(clientPreface, serverPreface, challenge, clientNonce, username, clientMAC, peerAuthSuccess, []byte(credentials.Password))
		if err != nil {
			firstServerErrors <- err
			return
		}
		var result bytes.Buffer
		if err := writePeerAuthResult(&result, peerAuthSuccess, proof); err != nil {
			firstServerErrors <- err
			return
		}
		recordedNonce = append([]byte(nil), clientNonce...)
		recordedResult = append([]byte(nil), result.Bytes()...)
		firstServerErrors <- writeFull(firstServer, recordedResult)
	}()
	if err := AuthenticatePeerClient(firstClient, clientPreface, serverPreface, credentials); err != nil {
		t.Fatal(err)
	}
	if err := <-firstServerErrors; err != nil {
		t.Fatal(err)
	}
	_ = firstClient.Close()
	_ = firstServer.Close()

	secondClient, secondServer := peerAuthPipe(t)
	var replayNonce []byte
	secondServerErrors := make(chan error, 1)
	go func() {
		if err := writeFull(secondServer, challenge); err != nil {
			secondServerErrors <- err
			return
		}
		_, clientNonce, _, err := readPeerAuthResponse(secondServer)
		if err != nil {
			secondServerErrors <- err
			return
		}
		replayNonce = append([]byte(nil), clientNonce...)
		secondServerErrors <- writeFull(secondServer, recordedResult)
	}()
	err := AuthenticatePeerClient(secondClient, clientPreface, serverPreface, credentials)
	if serverErr := <-secondServerErrors; serverErr != nil {
		t.Fatal(serverErr)
	}
	_ = secondClient.Close()
	_ = secondServer.Close()
	if hmac.Equal(recordedNonce, replayNonce) {
		t.Fatal("client nonce was reused across peer authentication attempts")
	}
	if !errors.Is(err, ErrPeerAuthenticationFailed) {
		t.Fatalf("replayed server success error = %v", err)
	}
}

func readPeerAuthResponse(reader io.Reader) ([]byte, []byte, []byte, error) {
	header := make([]byte, peerAuthResponseHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, nil, nil, err
	}
	if !validPeerAuthRecord(header, peerAuthResponseType) {
		return nil, nil, nil, ErrInvalidPeerAuth
	}
	username := make([]byte, int(header[6]))
	clientNonce := make([]byte, peerAuthNonceSize)
	clientMAC := make([]byte, peerAuthMACSize)
	if _, err := io.ReadFull(reader, username); err != nil {
		return nil, nil, nil, err
	}
	if _, err := io.ReadFull(reader, clientNonce); err != nil {
		return nil, nil, nil, err
	}
	if _, err := io.ReadFull(reader, clientMAC); err != nil {
		return nil, nil, nil, err
	}
	return username, clientNonce, clientMAC, nil
}

func peerAuthTestPrefaces() (Preface, Preface) {
	return Preface{Version: Version, MaxPayload: MaxPayloadSize}, Preface{Version: Version, Flags: PrefaceFlagAuthRequired, MaxPayload: MaxPayloadSize}
}

func peerAuthTestChallenge() []byte {
	challenge := make([]byte, peerAuthChallengeSize)
	binary.BigEndian.PutUint32(challenge[0:4], peerAuthMagic)
	challenge[4] = peerAuthVersion
	challenge[5] = peerAuthChallengeType
	for index := peerAuthResponseHeaderSize; index < len(challenge); index++ {
		challenge[index] = byte(index)
	}
	return challenge
}

func peerAuthTestResponse(username []byte) []byte {
	response := make([]byte, peerAuthResponseHeaderSize+len(username)+peerAuthNonceSize+peerAuthMACSize)
	binary.BigEndian.PutUint32(response[0:4], peerAuthMagic)
	response[4] = peerAuthVersion
	response[5] = peerAuthResponseType
	response[6] = byte(len(username))
	copy(response[peerAuthResponseHeaderSize:], username)
	return response
}

func peerAuthTestResult(status byte) []byte {
	result := make([]byte, peerAuthResultSize)
	binary.BigEndian.PutUint32(result[0:4], peerAuthMagic)
	result[4] = peerAuthVersion
	result[5] = peerAuthResultType
	result[6] = status
	return result
}

func peerAuthMutate(record []byte, offset int, value byte) []byte {
	mutated := append([]byte(nil), record...)
	mutated[offset] = value
	return mutated
}

func assertPeerAuthTruncationError(t *testing.T, length int, err error) {
	t.Helper()
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("prefix length %d: error = %v, want EOF", length, err)
	}
}

func peerAuthPipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	deadline := time.Now().Add(time.Second)
	if err := clientConn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := serverConn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	return clientConn, serverConn
}
