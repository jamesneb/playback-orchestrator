package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jamesneb/playback-orchestrator/internal/clickhouse"
	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/jamesneb/playback-orchestrator/internal/orchestrate"
	storage "github.com/jamesneb/playback-orchestrator/internal/orchestrate/storage"
)

func main() {
	cfg := config.LoadConfig()

	ch, err := clickhouse.InitConnection(cfg.ClickhouseCFG)
	if err != nil {
		log.Fatalf("failed to init ClickHouse connection: %v", err)
	}
	chStore := clickhouse.NewClickhouseStore(ch, cfg.ClickhouseCFG)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM
	go handleShutdown(cancel)

	log.Println("Starting orchestrator loop...")
	startLoop(ctx, chStore, cfg.OrchestratorCFG)
}

// startLoop runs orchestrator.Run every 10 seconds until the context is cancelled.
func startLoop(ctx context.Context, store storage.Storage, cfg config.OrchestratorCFG) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down orchestrator loop...")
			return
		case <-ticker.C:
			if err := orchestrate.Run(store, cfg); err != nil {
				log.Printf("orchestrator run failed: %v", err)
			}
		}
	}
}

// handleShutdown cancels the context when SIGINT or SIGTERM is received.
func handleShutdown(cancelFunc context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Termination signal received. Exiting...")
	cancelFunc()
}
