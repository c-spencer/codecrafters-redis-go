package main

import (
	"context"
	"log"
	"os"

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
	// TODO: Figure out why this slows down the codecrafters tests so much
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Initiating server shutdown")
		mainCancel()
	}()

	// Setup server state and start listening

	config := server.ReadConfig()
	server, _ := server.NewServer(mainCtx, config)

	server.Start()
}
