package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jamesneb/playback-orchestrator/internal/clickhouse"
	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/jamesneb/playback-orchestrator/internal/orchestrate"
	"github.com/jamesneb/playback-orchestrator/internal/orchestrate/jobqueue"
	storage "github.com/jamesneb/playback-orchestrator/internal/orchestrate/storage"
	"github.com/jamesneb/playback-orchestrator/internal/redis"
)

func main() {
	cfg := config.LoadConfig()

	chStore, rdsQueue, err := setup(*cfg)
	if err != nil {
		log.Fatalf("setup failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM
	go handleShutdown(cancel)

	log.Println("Starting orchestrator loop...")
	startLoop(ctx, chStore, rdsQueue, cfg.OrchestratorCFG)
}

func setup(cfg config.Config) (storage.Storage, jobqueue.JobQueue, error) {
	ch, err := clickhouse.InitConnection(cfg.ClickhouseCFG)
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse init failed: %w", err)
	}
	redisClient, err := redis.InitConnection(cfg.RedisCFG)
	if err != nil {
		return nil, nil, fmt.Errorf("redis init failed: %w", err)
	}
	return clickhouse.NewClickhouseStore(ch, cfg.ClickhouseCFG),
		redis.NewRedisJobQueue(redisClient, cfg.RedisCFG),
		nil
}

// startLoop runs orchestrator.Run every 10 seconds until the context is cancelled.
func startLoop(ctx context.Context, store storage.Storage, queue jobqueue.JobQueue, cfg config.OrchestratorCFG) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down orchestrator loop...")
			return
		case <-ticker.C:
			if err := orchestrate.Run(store, queue, cfg); err != nil {
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
