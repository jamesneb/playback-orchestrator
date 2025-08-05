package orchestrate

import (
	"context"
	"fmt"

	"github.com/jamesneb/playback-orchestrator/internal/config"
	orchestrate "github.com/jamesneb/playback-orchestrator/internal/orchestrate/storage"
)

func Run(store orchestrate.Storage, cfg config.OrchestratorCFG) error {
	ctx := context.Background()
	limit := cfg.RAW_SPANS_LIMIT

	spans, err := store.GetNewSpansForTenant(ctx, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch spans: %w", err)
	}

	fmt.Printf("Got %d spans\n", len(spans))
	return nil
}
