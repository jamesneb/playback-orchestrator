package orchestrate

import (
	"context"

	"github.com/jamesneb/playback-orchestrator/internal/domain/span"
	"github.com/jamesneb/playback-orchestrator/internal/domain/tenant"
)

type Storage interface {
	GetTenants(ctx context.Context) ([]tenant.Tenant, error)
	GetNewSpansForTenant(ctx context.Context, limit int) ([]span.Span, error)
}
