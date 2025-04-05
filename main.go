package relay

import (
	"errors" // Add import for errors.Is
	"flag"
	stdLog "log" // Alias standard log to avoid conflict
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RabbyHub/derelay/config"
	projLog "github.com/RabbyHub/derelay/log" // Use aliased import for project's log
	"github.com/RabbyHub/derelay/metrics"
	"github.com/RabbyHub/derelay/relay"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9" // Correct import path
)

func startMetricServer(config *config.MetricConfig) {
	r := mux.NewRouter()

	r.Handle("/metrics", metrics.Handler())

	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

	s := &http.Server{
		Addr:           config.Listen,
		Handler:        r,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		// Check error from ListenAndServe
		err := s.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Use aliased project logger
			projLog.Error("Metrics server ListenAndServe error", err)
		}
	}()
}

func parseCmdlineAndLoadConfig() config.Config {
	cmdlineConfig := config.Config{}
	configFilePath := flag.String("config", "", "config file")

	// define cmdline options
	flag.StringVar(&cmdlineConfig.RelayServerConfig.Listen, "relay.addr", "", "relay server listen address")
	flag.StringVar(&cmdlineConfig.RedisServerConfig.ServerAddr, "redis.server_addr", "", "redis server address")

	flag.Parse()

	// load file config
	fileConfig := config.LoadConfig(*configFilePath)

	// overwrite with cmdline config
	if listen := cmdlineConfig.RelayServerConfig.Listen; listen != "" {
		fileConfig.RelayServerConfig.Listen = listen
	}

	if serverAddr := cmdlineConfig.RedisServerConfig.ServerAddr; serverAddr != "" {
		fileConfig.RedisServerConfig.ServerAddr = serverAddr
	}

	return fileConfig
}

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := parseCmdlineAndLoadConfig()

	// Create the concrete Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisServerConfig.ServerAddr,
		Password: config.RedisServerConfig.Password,
		DB:       0, // Use the appropriate DB number if needed
	})
	// TODO: Add error handling for redis.NewClient if necessary, though it typically doesn't error

	// Pass the concrete client (which satisfies RelayRedisIO) to the constructor
	wsServer := relay.NewWSServer(&config, redisClient)
	relayServer := relay.NewRelayServer(&config.RelayServerConfig, wsServer)

	// start websocket server
	go func() {
		wsServer.Run()
	}()

	// Start relay server
	go func() {
		relayServer.Run()
	}()

	// Start metric and pprof server
	if config.MetricServerConfig.Enable {
		startMetricServer(&config.MetricServerConfig)
	}

	sig := <-sigChan
	waitSeconds := config.RelayServerConfig.GracefulShutdownWaitSeconds
	// Use aliased standard log for shutdown message
	stdLog.Printf("Sig %v received, shutting down, graceful shutdown wait: %v seconds\n", sig, waitSeconds)

	<-time.After(time.Duration(waitSeconds) * time.Second)

	relayServer.Shutdown()
}
