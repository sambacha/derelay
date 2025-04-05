package relay

import (
	"context"
	"errors"

	// "fmt" // Remove unused import
	"net/http"
	"time"

	"github.com/RabbyHub/derelay/config"
	"github.com/RabbyHub/derelay/log" // Add log import
	"github.com/gorilla/mux"
)

// Removed duplicate import block

type relayServer struct {
	httpServer *http.Server
	wsServer   *WsServer
}

func NewRelayServer(config *config.RelayConfig, wsServer *WsServer) *relayServer {
	r := mux.NewRouter()

	r.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong")) // Ignore both return values (bytes written, error)
	})

	// handle websocket connection
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		wsServer.NewClientConn(w, r)
	})

	s := &http.Server{
		Addr:           config.Listen,
		Handler:        r,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	return &relayServer{
		httpServer: s,
		wsServer:   wsServer,
	}
}

func (rs *relayServer) Run() {
	err := rs.httpServer.ListenAndServe()
	if err != nil && errors.Is(err, http.ErrServerClosed) {
		return
	}
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// Shutdown Gracefully shutdown the relay server
func (rs *relayServer) Shutdown() {
	// Use background context for shutdown, or potentially one passed down
	err := rs.httpServer.Shutdown(context.Background())
	if err != nil {
		log.Error("RelayServer HTTP shutdown error", err) // Use project logger
	}
	rs.wsServer.Shutdown()
}
