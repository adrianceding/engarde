package tcpstream

import (
	"bytes"
	"testing"
)

func FuzzPeerAuthenticationParsers(f *testing.F) {
	challenge := peerAuthTestChallenge()
	result := peerAuthTestResult(peerAuthFailure)
	response := peerAuthTestResponse([]byte("client-a"))
	f.Add([]byte{})
	f.Add(challenge)
	f.Add(append(append([]byte(nil), challenge...), result...))
	f.Add(response)
	f.Add(peerAuthMutate(challenge, 7, 1))
	f.Add(peerAuthMutate(response, 6, 255))

	clientPreface, serverPreface := peerAuthTestPrefaces()
	credentials := PeerCredentials{Username: "client-a", Password: "peer-secret"}
	users := map[string]string{"client-a": "peer-secret"}

	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 1024 {
			return
		}

		var clientOutput bytes.Buffer
		clientStream := &peerAuthTestReadWriter{
			reader: bytes.NewReader(wire),
			writer: &clientOutput,
		}
		_ = AuthenticatePeerClient(clientStream, clientPreface, serverPreface, credentials)
		maximumClientOutput := peerAuthResponseHeaderSize + len(credentials.Username) + peerAuthNonceSize + peerAuthMACSize
		if clientOutput.Len() > maximumClientOutput {
			t.Fatalf("client parser wrote %d bytes, maximum is %d", clientOutput.Len(), maximumClientOutput)
		}

		var serverOutput bytes.Buffer
		serverStream := &peerAuthTestReadWriter{
			reader: bytes.NewReader(wire),
			writer: &serverOutput,
		}
		_, _ = AuthenticatePeerServer(serverStream, clientPreface, serverPreface, users)
		maximumServerOutput := peerAuthChallengeSize + peerAuthResultSize
		if serverOutput.Len() > maximumServerOutput {
			t.Fatalf("server parser wrote %d bytes, maximum is %d", serverOutput.Len(), maximumServerOutput)
		}
	})
}
