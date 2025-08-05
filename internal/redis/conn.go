package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/redis/go-redis/v9"
)

var (
	connInstance *redis.Client
	connOnce     sync.Once
	initErr      error
)

func InitConnection(cfg config.RedisCFG) (*redis.Client, error) {
	connOnce.Do(func() {
		dbIndex, err := strconv.Atoi(cfg.DB)
		if err != nil {
			initErr = fmt.Errorf("failed to parse Redis DB: %w", err)
			return
		}

		client := redis.NewClient(&redis.Options{
			Addr:     cfg.URL,
			Password: cfg.PASSWD,
			DB:       dbIndex,
		})

		if err := client.Ping(context.Background()).Err(); err != nil {
			initErr = fmt.Errorf("failed to connect to Redis: %w", err)
			return
		}

		connInstance = client
	})

	return connInstance, initErr
}
