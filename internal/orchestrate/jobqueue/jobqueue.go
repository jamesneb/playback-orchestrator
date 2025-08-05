package jobqueue

import (
	"context"

	"github.com/jamesneb/playback-orchestrator/internal/domain/job"
)

type JobQueue interface {
	Post(ctx context.Context, job *job.Job) error
}
