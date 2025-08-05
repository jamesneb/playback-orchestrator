package redis

import (
	"context"

	"github.com/jamesneb/playback-orchestrator/internal/config"
	jb "github.com/jamesneb/playback-orchestrator/internal/domain/job"
	job_proto "github.com/jamesneb/playback-orchestrator/internal/domain/job/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type RedisJobQueue struct {
	client *redis.Client
	cfg    config.RedisCFG
}

func NewRedisJobQueue(client *redis.Client, cfg config.RedisCFG) *RedisJobQueue {
	return &RedisJobQueue{client: client, cfg: cfg}
}

func (s *RedisJobQueue) Post(ctx context.Context, job *jb.Job) error {
	data, err := proto.Marshal(job_proto.FromDomain(job))
	if err != nil {
		return err
	}
	return s.client.LPush(ctx, s.cfg.JOB_QUEUE_NAME, data).Err()
}
