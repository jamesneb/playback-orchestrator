package orchestrate

import (
	"context"
	"fmt"

	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/jamesneb/playback-orchestrator/internal/domain/job"
	"github.com/jamesneb/playback-orchestrator/internal/orchestrate/jobqueue"
	orchestrate "github.com/jamesneb/playback-orchestrator/internal/orchestrate/storage"
)

func Run(ctx context.Context, store orchestrate.Storage, queue jobqueue.JobQueue, cfg config.OrchestratorCFG) error {
	limit := cfg.RAW_SPANS_LIMIT

	spans, err := store.GetNewSpansForTenant(ctx, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch spans: %w", err)
	}

	fmt.Printf("Got %d spans\n", len(spans))

	// For now, immediately post job
	// TODO: Orchestration logic
	for _, span := range spans {
		fmt.Println(span.ID)

		newJob := job.FromSpan(span)
		queue.Post(ctx, newJob)
	}
	return nil
}
