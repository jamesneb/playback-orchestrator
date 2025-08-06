package job

import "github.com/jamesneb/playback-orchestrator/internal/domain/span"

func FromSpan(span span.Span) *Job {
	return &Job{Data: span.ID}
}
