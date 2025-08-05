package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jamesneb/playback-orchestrator/internal/config"
	"github.com/jamesneb/playback-orchestrator/internal/domain/span"
	"github.com/jamesneb/playback-orchestrator/internal/domain/tenant"
)

type ClickhouseStore struct {
	conn clickhouse.Conn
	cfg  config.ClickhouseCFG
}

func NewClickhouseStore(conn clickhouse.Conn, cfg config.ClickhouseCFG) *ClickhouseStore {
	return &ClickhouseStore{conn: conn, cfg: cfg}
}

func (s *ClickhouseStore) GetTenants(ctx context.Context) ([]tenant.Tenant, error) {
	return nil, errors.New("unimplemented")
}

func (s *ClickhouseStore) GetNewSpansForTenant(ctx context.Context, limit int) ([]span.Span, error) {
	fmt.Printf("%+v\n", s.cfg)
	query := fmt.Sprintf("SELECT span_id FROM %s LIMIT @limit", s.cfg.RAW_SPAN_TABLE_NAME)
	rows, err := s.conn.Query(ctx, query, clickhouse.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var spans []span.Span
	for rows.Next() {
		var span span.Span
		if err := rows.Scan(&span.ID); err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}
		spans = append(spans, span)
	}

	return spans, nil
}
