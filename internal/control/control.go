package control

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

var newHTTPServer = func(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler}
}
var listenAndServeHTTPServer = func(server *http.Server) error {
	return server.ListenAndServe()
}
var shutdownHTTPServer = func(server *http.Server, ctx context.Context) error {
	return server.Shutdown(ctx)
}
var readAll = io.ReadAll

type StatusProvider interface {
	Status() (any, error)
}

type ClientActions interface {
	ToggleOverride(ifName string) string
	Include(ifName string) string
	Exclude(ifName string) string
	ResetExclusions() string
}

func Run(ctx context.Context, listenAddr, username, password string, webFS fs.FS, status StatusProvider, actions ClientActions) error {
	server := newHTTPServer(listenAddr, NewMux(webFS, status, actions, username, password))

	stopShutdownWatcher := make(chan struct{})
	shutdownWatcherDone := make(chan struct{})
	go func() {
		defer close(shutdownWatcherDone)
		select {
		case <-ctx.Done():
		case <-stopShutdownWatcher:
			if ctx.Err() == nil {
				return
			}
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownHTTPServer(server, shutdownCtx); err != nil {
			log.WithError(err).Warn("Error shutting down management webserver")
		}
	}()

	log.Info("Management webserver listening on " + listenAddr)
	err := listenAndServeHTTPServer(server)
	close(stopShutdownWatcher)
	<-shutdownWatcherDone
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func NewMux(webFS fs.FS, status StatusProvider, actions ClientActions, username, password string) http.Handler {
	realm := "engarde"
	mux := http.NewServeMux()
	mux.HandleFunc("/", BasicAuth(fileServer(webFS), username, password, realm))
	mux.HandleFunc("/api/v1/get-list", NoCache(BasicAuth(getList(status), username, password, realm)))
	if actions != nil {
		mux.HandleFunc("/api/v1/include", NoCache(BasicAuth(include(actions), username, password, realm)))
		mux.HandleFunc("/api/v1/exclude", NoCache(BasicAuth(exclude(actions), username, password, realm)))
		mux.HandleFunc("/api/v1/swap-exclusion", NoCache(BasicAuth(swapExclusion(actions), username, password, realm)))
		mux.HandleFunc("/api/v1/reset-exclusions", NoCache(BasicAuth(resetExclusions(actions), username, password, realm)))
	}
	return mux
}

func BasicAuth(handler http.HandlerFunc, username, password, realm string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if username != "" && password != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(username)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(password)) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="`+realm+`"`)
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("Unauthorized.\n"))
				return
			}
		}
		handler(w, r)
	}
}

func fileServer(webFS fs.FS) http.HandlerFunc {
	server := http.FileServer(http.FS(webFS))
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			index, err := webFS.Open("index.html")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			defer index.Close()
			content, err := readAll(index)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
			return
		}
		server.ServeHTTP(w, r)
	}
}

func getList(status StatusProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response, err := status.Status()
		if err != nil {
			writeError(w, http.StatusInternalServerError)
			return
		}
		writeJSON(w, response)
	}
}

func include(actions ClientActions) http.HandlerFunc {
	return interfaceAction(func(ifName string) string { return actions.Include(ifName) })
}

func exclude(actions ClientActions) http.HandlerFunc {
	return interfaceAction(func(ifName string) string { return actions.Exclude(ifName) })
}

func swapExclusion(actions ClientActions) http.HandlerFunc {
	return interfaceAction(func(ifName string) string { return actions.ToggleOverride(ifName) })
}

func resetExclusions(actions ClientActions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, struct {
			Status string `json:"status"`
		}{actions.ResetExclusions()})
	}
}

func interfaceAction(action func(string) string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readAll(r.Body)
		if err != nil {
			writeError(w, http.StatusInternalServerError)
			return
		}
		var request struct {
			Iface string `json:"interface"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			writeError(w, http.StatusInternalServerError)
			return
		}
		writeJSON(w, struct {
			Status string `json:"status"`
		}{action(request.Iface)})
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	content, err := json.Marshal(value)
	if err != nil {
		writeError(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func writeError(w http.ResponseWriter, status int) {
	w.WriteHeader(status)
	_, _ = w.Write([]byte("Internal server error"))
}
