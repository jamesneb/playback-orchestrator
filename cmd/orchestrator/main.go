package main

import (
	"log"

	"github.com/jamesneb/playback-orchestrator/internal/clickhouse"
	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/jamesneb/playback-orchestrator/internal/orchestrate"
)

func main() {
	cfg := config.LoadConfig()

	ch, err := clickhouse.InitConnection(cfg.ClickhouseCFG)
	if err != nil {
		log.Fatalf("failed to init ClickHouse connection: %v", err)
	}
	chStore := clickhouse.NewClickhouseStore(ch, cfg.ClickhouseCFG)

	log.Println("Starting orchestrator...")
	if err := orchestrate.Run(chStore, cfg.OrchestratorCFG); err != nil {
		log.Fatalf("orchestrator run failed: %v", err)
	}
}
