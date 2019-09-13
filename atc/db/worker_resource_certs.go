package db

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"
)

type WorkerResourceCerts struct {
	WorkerName string
	CertsPath  string
}

type UsedWorkerResourceCerts struct {
	ID int
}

func (workerResourceCerts WorkerResourceCerts) Find(runner sq.BaseRunner) (*UsedWorkerResourceCerts, bool, error) {
	ctx := context.WithValue(context.Background(), ctxQueryNameKey, "workerResourceCerts-Find")

	var id int
	err := workerResourceCerts.findQuery().
		RunWith(runner).
		QueryRowContext(ctx).
		Scan(&id)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}

		return nil, false, err
	}

	return &UsedWorkerResourceCerts{ID: id}, true, nil
}

func (workerResourceCerts WorkerResourceCerts) FindOrCreate(tx Tx) (*UsedWorkerResourceCerts, error) {
	uwrc, found, err := workerResourceCerts.Find(tx)
	if err != nil {
		return nil, err
	}

	if found {
		return uwrc, err
	}

	return workerResourceCerts.create(tx)
}

func (workerResourceCerts WorkerResourceCerts) findQuery() sq.SelectBuilder {
	return psql.Select("id").
		From("worker_resource_certs").
		Where(sq.Eq{
			"worker_name": workerResourceCerts.WorkerName,
			"certs_path":  workerResourceCerts.CertsPath,
		})
}

func (workerResourceCerts WorkerResourceCerts) create(tx Tx) (*UsedWorkerResourceCerts, error) {
	ctx := context.WithValue(context.Background(), ctxQueryNameKey, "workerResourceCerts-create")
	var id int
	err := psql.Insert("worker_resource_certs").
		Columns(
			"worker_name",
			"certs_path",
		).
		Values(
			workerResourceCerts.WorkerName,
			workerResourceCerts.CertsPath,
		).
		Suffix("RETURNING id").
		RunWith(tx).
		QueryRowContext(ctx).
		Scan(&id)
	if err != nil {
		return nil, err
	}

	return &UsedWorkerResourceCerts{ID: id}, nil
}
