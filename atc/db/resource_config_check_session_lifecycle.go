package db

import (
	"context"

	sq "github.com/Masterminds/squirrel"
)

//go:generate counterfeiter . ResourceConfigCheckSessionLifecycle

type ResourceConfigCheckSessionLifecycle interface {
	CleanInactiveResourceConfigCheckSessions() error
	CleanExpiredResourceConfigCheckSessions() error
}

type resourceConfigCheckSessionLifecycle struct {
	conn Conn
}

func NewResourceConfigCheckSessionLifecycle(conn Conn) ResourceConfigCheckSessionLifecycle {
	return resourceConfigCheckSessionLifecycle{
		conn: conn,
	}
}

func (lifecycle resourceConfigCheckSessionLifecycle) CleanInactiveResourceConfigCheckSessions() error {
	usedByActiveUnpausedResources, _, err := sq.
		Select("rccs.id").
		From("resource_config_check_sessions rccs").
		Join("resource_configs rc ON rccs.resource_config_id = rc.id").
		Join("resources r ON r.resource_config_id = rc.id").
		Join("pipelines p ON p.id = r.pipeline_id").
		Where(sq.Expr("r.active AND NOT p.paused")).
		ToSql()
	if err != nil {
		return err
	}

	usedByActiveUnpausedResourceTypes, _, err := sq.
		Select("rccs.id").
		From("resource_config_check_sessions rccs").
		Join("resource_configs rc ON rccs.resource_config_id = rc.id").
		Join("resource_types rt ON rt.resource_config_id = rc.id").
		Join("pipelines p ON p.id = rt.pipeline_id").
		Where(sq.Expr("rt.active AND NOT p.paused")).
		ToSql()
	if err != nil {
		return err
	}

	ctx := context.WithValue(context.Background(), ctxQueryNameKey, "resourceConfigCheckSessionLifecycle-CleanInactiveResourceConfigCheckSessions")
	_, err = sq.Delete("resource_config_check_sessions").
		Where("id NOT IN (" + usedByActiveUnpausedResources + " UNION " + usedByActiveUnpausedResourceTypes + ")").
		PlaceholderFormat(sq.Dollar).
		RunWith(lifecycle.conn).
		ExecContext(ctx)

	return err
}

func (lifecycle resourceConfigCheckSessionLifecycle) CleanExpiredResourceConfigCheckSessions() error {
	ctx := context.WithValue(context.Background(), ctxQueryNameKey, "resourceConfigCheckSessionLifecycle-CleanExpiredResourceConfigCheckSessions")
	_, err := psql.Delete("resource_config_check_sessions").
		Where(sq.Expr("expires_at < NOW()")).
		RunWith(lifecycle.conn).
		ExecContext(ctx)

	return err
}
