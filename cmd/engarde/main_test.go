package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/adrianceding/engarde/internal/config"
	"github.com/adrianceding/engarde/internal/version"
)

func resetMainHooks(t *testing.T) {
	t.Helper()
	originalExit := exit
	originalGetWebFS := getWebFS
	originalListInterfaces := listInterfaces
	originalStartClient := startClient
	originalStartServer := startServer
	originalVersion := version.Version
	originalArgs := os.Args
	t.Cleanup(func() {
		exit = originalExit
		getWebFS = originalGetWebFS
		listInterfaces = originalListInterfaces
		startClient = originalStartClient
		startServer = originalStartServer
		version.Version = originalVersion
		os.Args = originalArgs
	})
}

func TestMainUsesExitCode(t *testing.T) {
	resetMainHooks(t)
	version.Version = "test"
	os.Args = []string{"engarde", "-v"}
	var gotCode int
	exit = func(code int) {
		gotCode = code
		panic("exit")
	}

	defer func() {
		if recovered := recover(); recovered != "exit" {
			t.Fatalf("recover = %v, want exit", recovered)
		}
		if gotCode != 0 {
			t.Fatalf("exit code = %d, want 0", gotCode)
		}
	}()
	main()
}

func TestRunVersionAndListInterfaces(t *testing.T) {
	resetMainHooks(t)
	version.Version = "test-version"
	var output bytes.Buffer
	if err := run([]string{"-v"}, &output); err != nil {
		t.Fatalf("run -v returned error: %v", err)
	}
	if output.String() != "engarde ver. test-version\r\n" {
		t.Fatalf("version output = %q", output.String())
	}

	output.Reset()
	listInterfaces = func(writer io.Writer) error {
		_, err := writer.Write([]byte("interfaces"))
		return err
	}
	if err := run([]string{"list-interfaces"}, &output); err != nil {
		t.Fatalf("run list-interfaces returned error: %v", err)
	}
	if output.String() != "engarde ver. test-version\r\ninterfaces" {
		t.Fatalf("list output = %q", output.String())
	}
}

func TestRunReportsErrors(t *testing.T) {
	resetMainHooks(t)
	version.Version = ""
	if err := run([]string{filepath.Join(t.TempDir(), "missing.yml")}, io.Discard); err == nil {
		t.Fatal("run missing config succeeded")
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "engarde.yml")
	if err := os.WriteFile(configPath, []byte("client:\n  listenAddr: 127.0.0.1:1\n  dstAddr: 127.0.0.1:2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	getWebFS = func() (fs.FS, error) { return nil, errors.New("assets") }
	if err := run([]string{configPath}, io.Discard); err == nil || !strings.Contains(err.Error(), "assets") {
		t.Fatalf("run getWebFS error = %v, want assets", err)
	}
}

func TestRunStartsSelectedRole(t *testing.T) {
	resetMainHooks(t)
	version.Version = "role-test"
	webFS := fstest.MapFS{"index.html": {Data: []byte("ok")}}
	getWebFS = func() (fs.FS, error) { return webFS, nil }

	dir := t.TempDir()
	clientConfig := filepath.Join(dir, "client.yml")
	if err := os.WriteFile(clientConfig, []byte("client:\n  listenAddr: 127.0.0.1:1\n  dstAddr: 127.0.0.1:2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	clientStarted := false
	startClient = func(ctx context.Context, cfg config.Client, version string, fsys fs.FS) error {
		clientStarted = true
		if cfg.ListenAddr != "127.0.0.1:1" || version != "role-test" || fsys == nil {
			t.Fatalf("client args = %#v %q %#v", cfg, version, fsys)
		}
		return nil
	}
	if err := run([]string{clientConfig}, io.Discard); err != nil {
		t.Fatalf("run client returned error: %v", err)
	}
	if !clientStarted {
		t.Fatal("client role was not started")
	}

	serverConfig := filepath.Join(dir, "server.yml")
	if err := os.WriteFile(serverConfig, []byte("server:\n  listenAddr: 127.0.0.1:3\n  allowUnsafeDynamicDestination: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	serverErr := errors.New("server")
	startServer = func(ctx context.Context, cfg config.Server, version string, fsys fs.FS) error {
		if cfg.ListenAddr != "127.0.0.1:3" || version != "role-test" || fsys == nil {
			t.Fatalf("server args = %#v %q %#v", cfg, version, fsys)
		}
		return serverErr
	}
	if err := run([]string{serverConfig}, io.Discard); !errors.Is(err, serverErr) {
		t.Fatalf("run server error = %v, want %v", err, serverErr)
	}
}

func TestRunExitCodeAndUnknownRole(t *testing.T) {
	resetMainHooks(t)
	if code := runExitCode([]string{filepath.Join(t.TempDir(), "missing.yml")}, io.Discard); code != 1 {
		t.Fatalf("runExitCode = %d, want 1", code)
	}
	if err := runRole(context.Background(), &config.Config{}, config.Role("bogus"), nil); err == nil || !strings.Contains(err.Error(), "unknown role") {
		t.Fatalf("runRole error = %v, want unknown role", err)
	}
}

func TestDefaultStartersReturnAddressErrors(t *testing.T) {
	if err := defaultStartClient(context.Background(), config.Client{ListenAddr: "bad listen", DstAddr: "127.0.0.1:1"}, "", nil); err == nil {
		t.Fatal("defaultStartClient succeeded with invalid listen address")
	}
	if err := defaultStartServer(context.Background(), config.Server{ListenAddr: "bad listen"}, "", nil); err == nil {
		t.Fatal("defaultStartServer succeeded with invalid listen address")
	}
}
