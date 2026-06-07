package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

type staticStatus struct {
	value any
	err   error
}

type marshalFail struct{}
type errorReadCloser struct{}

func (status staticStatus) Status() (any, error) {
	return status.value, status.err
}

func (marshalFail) MarshalJSON() ([]byte, error) { return nil, errors.New("marshal") }
func (errorReadCloser) Read([]byte) (int, error) { return 0, errors.New("read") }
func (errorReadCloser) Close() error             { return nil }

type actionRecorder struct {
	last string
}

func (recorder *actionRecorder) ToggleOverride(ifName string) string {
	recorder.last = ifName
	return "ok"
}

func (recorder *actionRecorder) Include(ifName string) string {
	recorder.last = ifName
	return "ok"
}

func (recorder *actionRecorder) Exclude(ifName string) string {
	recorder.last = ifName
	return "ok"
}

func (recorder *actionRecorder) ResetExclusions() string {
	return "ok"
}

func testFS() fs.FS {
	return fstest.MapFS{"index.html": {Data: []byte("index")}}
}

func TestGetListReturnsStatus(t *testing.T) {
	mux := NewMux(testFS(), staticStatus{value: ServerStatus{Type: "server", Version: "test"}}, nil, "", "")
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/get-list", nil)
	req.Header.Set("If-None-Match", "etag")

	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Header().Get("Cache-Control") == "" {
		t.Fatal("Cache-Control header was not set")
	}
	var response ServerStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "server" || response.Version != "test" {
		t.Fatalf("response = %#v", response)
	}
}

func TestBasicAuthProtectsHandlers(t *testing.T) {
	mux := NewMux(testFS(), staticStatus{value: ServerStatus{Type: "server"}}, nil, "engarde", "secret")

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/get-list", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized code = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/get-list", nil)
	req.SetBasicAuth("engarde", "secret")
	mux.ServeHTTP(authorized, req)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized code = %d, want %d", authorized.Code, http.StatusOK)
	}
}

func TestClientActionRoutes(t *testing.T) {
	recorder := &actionRecorder{}
	mux := NewMux(testFS(), staticStatus{value: ClientStatus{Type: "client"}}, recorder, "", "")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/swap-exclusion", strings.NewReader(`{"interface":"eth0"}`))
	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	if recorder.last != "eth0" {
		t.Fatalf("recorded interface = %q, want eth0", recorder.last)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ok" {
		t.Fatalf("status = %q, want ok", body.Status)
	}
}

func TestAllClientActionRoutesAndErrors(t *testing.T) {
	recorder := &actionRecorder{}
	mux := NewMux(testFS(), staticStatus{value: ClientStatus{Type: "client"}}, recorder, "", "")
	for _, route := range []string{"/api/v1/include", "/api/v1/exclude"} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, route, strings.NewReader(`{"interface":"eth1"}`))
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK || recorder.last != "eth1" {
			t.Fatalf("route %s code=%d last=%q", route, response.Code, recorder.last)
		}
	}

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/reset-exclusions", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("reset status = %d", response.Code)
	}

	for _, route := range []string{"/api/v1/include", "/api/v1/exclude", "/api/v1/swap-exclusion"} {
		response = httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, strings.NewReader(`not-json`)))
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("route %s invalid json status = %d", route, response.Code)
		}
	}
	response = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/include", nil)
	request.Body = errorReadCloser{}
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("read body error status = %d", response.Code)
	}
}

func TestGetListAndWriteJSONErrors(t *testing.T) {
	mux := NewMux(testFS(), staticStatus{err: errors.New("status")}, nil, "", "")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/get-list", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status error code = %d", response.Code)
	}

	mux = NewMux(testFS(), staticStatus{value: marshalFail{}}, nil, "", "")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/get-list", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("marshal error code = %d", response.Code)
	}
}

func TestFileServerServesIndex(t *testing.T) {
	mux := NewMux(testFS(), staticStatus{value: ServerStatus{Type: "server"}}, nil, "", "")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Body.String() != "index" {
		t.Fatalf("body = %q, want index", response.Body.String())
	}
}

func TestFileServerErrorsAndStaticFile(t *testing.T) {
	originalReadAll := readAll
	t.Cleanup(func() { readAll = originalReadAll })
	mux := NewMux(fstest.MapFS{"asset.txt": {Data: []byte("asset")}}, staticStatus{value: ServerStatus{}}, nil, "", "")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing index status = %d", response.Code)
	}

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/asset.txt", nil))
	if response.Code != http.StatusOK || response.Body.String() != "asset" {
		t.Fatalf("asset response code=%d body=%q", response.Code, response.Body.String())
	}

	readAll = func(r io.Reader) ([]byte, error) { return nil, errors.New("read index") }
	mux = NewMux(testFS(), staticStatus{value: ServerStatus{}}, nil, "", "")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("index read error status = %d", response.Code)
	}
}

func TestRunStartsAndStops(t *testing.T) {
	originalNewHTTPServer := newHTTPServer
	originalShutdownHTTPServer := shutdownHTTPServer
	t.Cleanup(func() { newHTTPServer = originalNewHTTPServer; shutdownHTTPServer = originalShutdownHTTPServer })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(server.URL, "http://")
	server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, addr, "", "", testFS(), staticStatus{value: ServerStatus{}}, nil) }()
	for i := 0; i < 100; i++ {
		_, err := http.Get("http://" + addr + "/api/v1/get-list")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRunShutdownErrorBranch(t *testing.T) {
	originalShutdownHTTPServer := shutdownHTTPServer
	t.Cleanup(func() { shutdownHTTPServer = originalShutdownHTTPServer })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownDone := make(chan struct{})
	shutdownHTTPServer = func(server *http.Server, ctx context.Context) error {
		close(shutdownDone)
		return errors.New("shutdown")
	}
	if err := Run(ctx, "127.0.0.1:1:bad", "", "", testFS(), staticStatus{}, nil); err == nil {
		t.Fatal("Run succeeded with invalid listen address")
	}
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown hook was not called")
	}
}

func TestRunListenError(t *testing.T) {
	if err := Run(context.Background(), "127.0.0.1:1:bad", "", "", testFS(), staticStatus{}, nil); err == nil {
		t.Fatal("Run succeeded with invalid listen address")
	}
}
