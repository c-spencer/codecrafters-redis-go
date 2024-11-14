package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/server"
)

type ValueWatchChannel chan *rdb.ValueEntry

func main() {
	log.Println("Starting up Redis-Go Server")

	// Setup graceful shutdown
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Initiating server shutdown")
		mainCancel()
	}()

	// Setup server state and start listening

	config := server.ReadConfig()
	wg, err := server.RunServer(mainCtx, config)

	if err != nil {
		log.Fatalf("Error running server: %s", err)
	}

	wg.Wait()
	log.Printf("Server shutdown complete")
}
