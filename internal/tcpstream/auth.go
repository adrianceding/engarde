package tcpstream

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

const (
	peerAuthMagic              uint32 = 0x45474155
	peerAuthVersion                   = 1
	peerAuthChallengeType             = 1
	peerAuthResponseType              = 2
	peerAuthResultType                = 3
	peerAuthSuccess                   = 0
	peerAuthFailure                   = 1
	peerAuthChallengeSize             = 40
	peerAuthResponseHeaderSize        = 8
	peerAuthResultHeaderSize          = 8
	peerAuthResultSize                = peerAuthResultHeaderSize + sha256.Size
	peerAuthNonceSize                 = sha256.Size
	peerAuthMACSize                   = sha256.Size
)

var (
	ErrInvalidPeerAuth             = errors.New("invalid TCP peer authentication message")
	ErrPeerAuthenticationFailed    = errors.New("TCP peer authentication failed")
	ErrPeerAuthenticationRequired  = errors.New("TCP server requires peer authentication")
	ErrPeerAuthenticationDowngrade = errors.New("TCP server did not require configured peer authentication")
)

type PeerCredentials struct {
	Username string
	Password string
}

func AuthenticatePeerClient(readerWriter io.ReadWriter, clientPreface, serverPreface Preface, credentials PeerCredentials) error {
	username := []byte(credentials.Username)
	if len(username) == 0 || len(username) > 255 || credentials.Password == "" {
		return ErrInvalidPeerAuth
	}
	challenge := make([]byte, peerAuthChallengeSize)
	if _, err := io.ReadFull(readerWriter, challenge); err != nil {
		return err
	}
	if !validPeerAuthRecord(challenge, peerAuthChallengeType) {
		return ErrInvalidPeerAuth
	}
	clientNonce := make([]byte, peerAuthNonceSize)
	if _, err := io.ReadFull(rand.Reader, clientNonce); err != nil {
		return err
	}
	mac, err := peerAuthMAC(clientPreface, serverPreface, challenge, clientNonce, username, []byte(credentials.Password))
	if err != nil {
		return err
	}
	response := make([]byte, peerAuthResponseHeaderSize+len(username)+peerAuthNonceSize+peerAuthMACSize)
	binary.BigEndian.PutUint32(response[0:4], peerAuthMagic)
	response[4] = peerAuthVersion
	response[5] = peerAuthResponseType
	response[6] = byte(len(username))
	copy(response[peerAuthResponseHeaderSize:], username)
	copy(response[peerAuthResponseHeaderSize+len(username):], clientNonce)
	copy(response[len(response)-peerAuthMACSize:], mac[:])
	if err := writeFull(readerWriter, response); err != nil {
		return err
	}
	result := make([]byte, peerAuthResultSize)
	if _, err := io.ReadFull(readerWriter, result); err != nil {
		return err
	}
	if !validPeerAuthRecord(result, peerAuthResultType) {
		return ErrInvalidPeerAuth
	}
	if result[6] != peerAuthSuccess {
		return ErrPeerAuthenticationFailed
	}
	expectedProof, err := peerAuthServerProof(clientPreface, serverPreface, challenge, clientNonce, username, mac[:], result[6], []byte(credentials.Password))
	if err != nil {
		return err
	}
	if !hmac.Equal(result[peerAuthResultHeaderSize:], expectedProof[:]) {
		return ErrPeerAuthenticationFailed
	}
	return nil
}

func AuthenticatePeerServer(readerWriter io.ReadWriter, clientPreface, serverPreface Preface, users map[string]string) (string, error) {
	challenge := make([]byte, peerAuthChallengeSize)
	binary.BigEndian.PutUint32(challenge[0:4], peerAuthMagic)
	challenge[4] = peerAuthVersion
	challenge[5] = peerAuthChallengeType
	if _, err := io.ReadFull(rand.Reader, challenge[8:]); err != nil {
		return "", err
	}
	if err := writeFull(readerWriter, challenge); err != nil {
		return "", err
	}
	header := make([]byte, peerAuthResponseHeaderSize)
	if _, err := io.ReadFull(readerWriter, header); err != nil {
		return "", err
	}
	if !validPeerAuthRecord(header, peerAuthResponseType) {
		return "", ErrInvalidPeerAuth
	}
	username := make([]byte, int(header[6]))
	clientNonce := make([]byte, peerAuthNonceSize)
	actualMAC := make([]byte, peerAuthMACSize)
	if _, err := io.ReadFull(readerWriter, username); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(readerWriter, clientNonce); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(readerWriter, actualMAC); err != nil {
		return "", err
	}
	password, exists := users[string(username)]
	key := []byte(password)
	if !exists {
		dummy := sha256.Sum256(append([]byte("engarde-peer-auth-unknown-user:"), username...))
		key = dummy[:]
	}
	expectedMAC, err := peerAuthMAC(clientPreface, serverPreface, challenge, clientNonce, username, key)
	if err != nil {
		return "", err
	}
	macMatches := hmac.Equal(actualMAC, expectedMAC[:])
	authenticated := exists && macMatches
	status := byte(peerAuthFailure)
	if authenticated {
		status = peerAuthSuccess
	}
	var proof [peerAuthMACSize]byte
	if authenticated {
		proof, err = peerAuthServerProof(clientPreface, serverPreface, challenge, clientNonce, username, actualMAC, status, key)
	} else {
		_, err = io.ReadFull(rand.Reader, proof[:])
	}
	if err != nil {
		return "", err
	}
	if err := writePeerAuthResult(readerWriter, status, proof); err != nil {
		return "", err
	}
	if !authenticated {
		return "", ErrPeerAuthenticationFailed
	}
	return string(username), nil
}

func peerAuthServerProof(clientPreface, serverPreface Preface, challenge, clientNonce, username, clientMAC []byte, status byte, key []byte) ([peerAuthMACSize]byte, error) {
	var result [peerAuthMACSize]byte
	clientBytes, _, err := marshalPreface(clientPreface)
	if err != nil {
		return result, err
	}
	serverBytes, _, err := marshalPreface(serverPreface)
	if err != nil {
		return result, err
	}
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte("engarde-peer-auth-server-v1\x00"))
	_, _ = digest.Write(clientBytes[:])
	_, _ = digest.Write(serverBytes[:])
	_, _ = digest.Write(challenge)
	_, _ = digest.Write(clientNonce)
	_, _ = digest.Write([]byte{byte(len(username))})
	_, _ = digest.Write(username)
	_, _ = digest.Write(clientMAC)
	_, _ = digest.Write([]byte{status})
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func peerAuthMAC(clientPreface, serverPreface Preface, challenge, clientNonce, username, key []byte) ([peerAuthMACSize]byte, error) {
	var result [peerAuthMACSize]byte
	clientBytes, _, err := marshalPreface(clientPreface)
	if err != nil {
		return result, err
	}
	serverBytes, _, err := marshalPreface(serverPreface)
	if err != nil {
		return result, err
	}
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte("engarde-peer-auth-v1\x00"))
	_, _ = digest.Write(clientBytes[:])
	_, _ = digest.Write(serverBytes[:])
	_, _ = digest.Write(challenge)
	_, _ = digest.Write(clientNonce)
	_, _ = digest.Write([]byte{byte(len(username))})
	_, _ = digest.Write(username)
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func validPeerAuthRecord(record []byte, recordType byte) bool {
	if len(record) < peerAuthResponseHeaderSize ||
		binary.BigEndian.Uint32(record[0:4]) != peerAuthMagic ||
		record[4] != peerAuthVersion || record[5] != recordType || record[7] != 0 {
		return false
	}
	switch recordType {
	case peerAuthChallengeType:
		return len(record) == peerAuthChallengeSize && record[6] == 0
	case peerAuthResponseType:
		return len(record) == peerAuthResponseHeaderSize && record[6] != 0
	case peerAuthResultType:
		return len(record) == peerAuthResultSize && record[6] <= peerAuthFailure
	default:
		return false
	}
}

func writePeerAuthResult(writer io.Writer, status byte, proof [peerAuthMACSize]byte) error {
	result := make([]byte, peerAuthResultSize)
	binary.BigEndian.PutUint32(result[0:4], peerAuthMagic)
	result[4] = peerAuthVersion
	result[5] = peerAuthResultType
	result[6] = status
	copy(result[peerAuthResultHeaderSize:], proof[:])
	return writeFull(writer, result)
}
