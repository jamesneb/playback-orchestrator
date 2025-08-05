package clickhouse

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jamesneb/playback-orchestrator/internal/config"
)

var (
	connInstance clickhouse.Conn
	connOnce     sync.Once
)

func InitConnection(cfg config.ClickhouseCFG) (clickhouse.Conn, error) {
	var dialCount int
	var initErr error

	opts := &clickhouse.Options{
		Addr: []string{cfg.URL},
		Auth: clickhouse.Auth{
			Database: cfg.DB,
			Username: cfg.USERNAME,
			Password: cfg.PASSWD,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": cfg.MAX_EXEC_SEC,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * time.Duration(cfg.DIAL_TIMEOUT_SEC),
		MaxOpenConns:         cfg.MAX_OPEN_CONNS,
		MaxIdleConns:         cfg.MAX_IDLE_CONNS,
		ConnMaxLifetime:      time.Duration(cfg.CONN_MAX_LIFESPAN_MINUTE) * time.Minute,
		ConnOpenStrategy:     getConnOpenStrategy(cfg.CONN_OPEN_STRATEGY),
		BlockBufferSize:      uint8(cfg.BLOCK_BUFFER_SIZE),
		MaxCompressionBuffer: cfg.MAX_COMPRESSION_BUFFER,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: cfg.CLIENT_NAME, Version: cfg.CLIENT_VERSION},
			},
		},
	}

	connOnce.Do(func() {
		conn, err := clickhouse.Open(opts)
		if err != nil {
			initErr = fmt.Errorf("failed to open clickhouse connection: %w", err)
			return
		}

		if err := conn.Ping(context.Background()); err != nil {
			initErr = fmt.Errorf("failed to ping clickhouse: %w", err)
			return
		}

		connInstance = conn
		log.Println("ClickHouse connection established")
	})

	return connInstance, initErr
}

func getConnOpenStrategy(s string) clickhouse.ConnOpenStrategy {
	switch s {
	case "OpenInOrder":
		return clickhouse.ConnOpenInOrder
	case "RoundRobin":
		return clickhouse.ConnOpenRoundRobin
	default:
		log.Printf("Unknown connection strategy '%s', using OpenInOrder\n", s)
		return clickhouse.ConnOpenInOrder
	}
}
