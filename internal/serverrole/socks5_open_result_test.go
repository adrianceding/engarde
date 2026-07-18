package serverrole

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/tcpstream"
)

func TestTCPServerPropagatesDestinationDialErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want tcpstream.OpenResult
	}{
		{name: "general failure", err: errors.New("dial failed"), want: tcpstream.OpenResultGeneralFailure},
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: connectionRefusedError}, want: tcpstream.OpenResultConnectionRefused},
		{name: "network unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: networkUnreachableError}, want: tcpstream.OpenResultNetworkUnreachable},
		{name: "host unreachable", err: &net.OpError{Op: "dial", Net: "tcp", Err: hostUnreachableError}, want: tcpstream.OpenResultHostUnreachable},
		{name: "DNS not found", err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "example.com", IsNotFound: true}}, want: tcpstream.OpenResultHostUnreachable},
		{name: "timeout", err: context.DeadlineExceeded, want: tcpstream.OpenResultTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

			previousDial := dialTCPDestination
			dialCount := 0
			dialTCPDestination = func(_ context.Context, address string, _ time.Duration) (net.Conn, error) {
				dialCount++
				if address != "example.com:443" {
					t.Fatalf("destination address = %q, want example.com:443", address)
				}
				return nil, test.err
			}
			t.Cleanup(func() { dialTCPDestination = previousDial })

			session, accepted := dialTCPServerTestSession(t, runtime, nil)
			destination, err := tcpstream.ParseDestination("example.com:443")
			if err != nil {
				t.Fatal(err)
			}
			streamID, err := tcpstream.NewStreamID()
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = session.OpenDestination(streamID, destination, time.Second)
			var openErr *tcpstream.OpenError
			if !errors.As(err, &openErr) || openErr == nil || openErr.Result != test.want {
				t.Fatalf("OPEN error = %v, want result %d", err, test.want)
			}
			_ = session.Close()
			select {
			case <-accepted:
			case <-time.After(time.Second):
				t.Fatal("server did not finish failed destination OPEN")
			}
			if dialCount != 1 {
				t.Fatalf("destination dial count = %d, want 1", dialCount)
			}
			runtime.mu.Lock()
			streams := len(runtime.streams)
			sessions := len(runtime.sessions)
			closed := len(runtime.closed)
			runtime.mu.Unlock()
			if streams != 0 || sessions != 0 || closed != 1 {
				t.Fatalf("streams/sessions/closed = %d/%d/%d, want 0/0/1", streams, sessions, closed)
			}
		})
	}
}
