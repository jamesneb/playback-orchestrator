package job_proto

import (
	"github.com/jamesneb/playback-orchestrator/internal/domain/job"
)

func FromDomain(j *job.Job) *Job {
	return &Job{
		Id: j.Data,
	}
}
