package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/server"
	"github.com/rs/zerolog"
)

type ValueWatchChannel chan *rdb.ValueEntry

func main() {
	// Setup logger
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	logger.Info().Msg("Starting up Redis-Go Server")

	// Setup graceful shutdown
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info().Msg("Initiating server shutdown")
		mainCancel()
	}()

	// Add logger to context, for cases where threading it through is cumbersome
	mainCtx = logger.WithContext(mainCtx)

	// Setup server state and start listening

	config := server.ReadConfig(logger)
	wg, err := server.RunServer(mainCtx, logger, config)

	if err != nil {
		logger.Fatal().Msgf("Error running server: %s", err)
	}

	wg.Wait()
	logger.Info().Msg("Server shutdown complete")
}
