package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const exposureColumns = `id, workload_id, agent_id, port, openziti_service_id, openziti_bind_policy_id, openziti_dial_policy_id, url, status, created_at, updated_at`

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func scanExposure(row pgx.Row) (Exposure, error) {
	var exposure Exposure
	if err := row.Scan(
		&exposure.ID,
		&exposure.WorkloadID,
		&exposure.AgentID,
		&exposure.Port,
		&exposure.OpenZitiServiceID,
		&exposure.OpenZitiBindPolicyID,
		&exposure.OpenZitiDialPolicyID,
		&exposure.URL,
		&exposure.Status,
		&exposure.CreatedAt,
		&exposure.UpdatedAt,
	); err != nil {
		return Exposure{}, err
	}
	return exposure, nil
}

func scanExposureFromRows(rows pgx.Rows) (Exposure, error) {
	var exposure Exposure
	if err := rows.Scan(
		&exposure.ID,
		&exposure.WorkloadID,
		&exposure.AgentID,
		&exposure.Port,
		&exposure.OpenZitiServiceID,
		&exposure.OpenZitiBindPolicyID,
		&exposure.OpenZitiDialPolicyID,
		&exposure.URL,
		&exposure.Status,
		&exposure.CreatedAt,
		&exposure.UpdatedAt,
	); err != nil {
		return Exposure{}, err
	}
	return exposure, nil
}

func collectExposures(rows pgx.Rows) ([]Exposure, error) {
	exposures := make([]Exposure, 0)
	for rows.Next() {
		exposure, err := scanExposureFromRows(rows)
		if err != nil {
			return nil, err
		}
		exposures = append(exposures, exposure)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return exposures, nil
}

func (s *Store) CreateExposure(ctx context.Context, exposure Exposure) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO exposures (id, workload_id, agent_id, port, status) VALUES ($1, $2, $3, $4, $5)`,
		exposure.ID,
		exposure.WorkloadID,
		exposure.AgentID,
		exposure.Port,
		exposure.Status,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrExposureAlreadyExists
		}
		return fmt.Errorf("create exposure: %w", err)
	}
	return nil
}

func (s *Store) GetExposure(ctx context.Context, id uuid.UUID) (Exposure, error) {
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM exposures WHERE id = $1`, exposureColumns), id)
	exposure, err := scanExposure(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Exposure{}, ErrExposureNotFound
		}
		return Exposure{}, fmt.Errorf("get exposure: %w", err)
	}
	return exposure, nil
}

func (s *Store) GetExposureByWorkloadAndPort(ctx context.Context, workloadID uuid.UUID, port int32) (Exposure, error) {
	row := s.pool.QueryRow(
		ctx,
		fmt.Sprintf(`SELECT %s FROM exposures WHERE workload_id = $1 AND port = $2`, exposureColumns),
		workloadID,
		port,
	)
	exposure, err := scanExposure(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Exposure{}, ErrExposureNotFound
		}
		return Exposure{}, fmt.Errorf("get exposure by workload and port: %w", err)
	}
	return exposure, nil
}

func (s *Store) ListExposuresByWorkload(ctx context.Context, workloadID uuid.UUID, pageSize int32, cursor *PageCursor) (ListResult, error) {
	limit := normalizePageSize(pageSize)
	args := []any{workloadID}
	query := fmt.Sprintf("SELECT %s FROM exposures WHERE workload_id = $1", exposureColumns)
	if cursor != nil {
		args = append(args, cursor.AfterID)
		query += fmt.Sprintf(" AND id > $%d", len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list exposures by workload: %w", err)
	}
	defer rows.Close()

	exposures, err := collectExposures(rows)
	if err != nil {
		return ListResult{}, fmt.Errorf("list exposures by workload: %w", err)
	}

	result := ListResult{Exposures: exposures}
	if int32(len(result.Exposures)) > limit {
		nextID := result.Exposures[limit-1].ID
		result.Exposures = result.Exposures[:limit]
		result.NextCursor = &PageCursor{AfterID: nextID}
	}
	return result, nil
}

func (s *Store) ListExposuresByStatus(ctx context.Context, status ExposureStatus) ([]Exposure, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM exposures WHERE status = $1 ORDER BY created_at ASC`, exposureColumns), status)
	if err != nil {
		return nil, fmt.Errorf("list exposures by status: %w", err)
	}
	defer rows.Close()
	exposures, err := collectExposures(rows)
	if err != nil {
		return nil, fmt.Errorf("list exposures by status: %w", err)
	}
	return exposures, nil
}

func (s *Store) ListExposuresByWorkloadAll(ctx context.Context, workloadID uuid.UUID) ([]Exposure, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM exposures WHERE workload_id = $1 ORDER BY created_at ASC`, exposureColumns), workloadID)
	if err != nil {
		return nil, fmt.Errorf("list exposures by workload: %w", err)
	}
	defer rows.Close()
	exposures, err := collectExposures(rows)
	if err != nil {
		return nil, fmt.Errorf("list exposures by workload: %w", err)
	}
	return exposures, nil
}

func (s *Store) ListAllActiveWorkloadIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT workload_id FROM exposures WHERE status = $1`, ExposureStatusActive)
	if err != nil {
		return nil, fmt.Errorf("list active workload ids: %w", err)
	}
	defer rows.Close()

	workloadIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan workload id: %w", err)
		}
		workloadIDs = append(workloadIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list active workload ids: %w", err)
	}
	return workloadIDs, nil
}

func (s *Store) UpdateExposureProvisioned(ctx context.Context, id uuid.UUID, resources ExposureResourceIDs) error {
	cmd, err := s.pool.Exec(ctx,
		`UPDATE exposures SET openziti_service_id = $2, openziti_bind_policy_id = $3, openziti_dial_policy_id = $4, url = $5, status = $6, updated_at = NOW() WHERE id = $1`,
		id,
		resources.OpenZitiServiceID,
		resources.OpenZitiBindPolicyID,
		resources.OpenZitiDialPolicyID,
		resources.URL,
		ExposureStatusActive,
	)
	if err != nil {
		return fmt.Errorf("update exposure provisioned: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrExposureNotFound
	}
	return nil
}

func (s *Store) UpdateExposureStatus(ctx context.Context, id uuid.UUID, status ExposureStatus) error {
	cmd, err := s.pool.Exec(ctx, `UPDATE exposures SET status = $2, updated_at = NOW() WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update exposure status: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrExposureNotFound
	}
	return nil
}

func (s *Store) UpdateExposureFailed(ctx context.Context, id uuid.UUID, resources ExposureResourceIDs) error {
	cmd, err := s.pool.Exec(ctx,
		`UPDATE exposures SET openziti_service_id = $2, openziti_bind_policy_id = $3, openziti_dial_policy_id = $4, url = $5, status = $6, updated_at = NOW() WHERE id = $1`,
		id,
		resources.OpenZitiServiceID,
		resources.OpenZitiBindPolicyID,
		resources.OpenZitiDialPolicyID,
		resources.URL,
		ExposureStatusFailed,
	)
	if err != nil {
		return fmt.Errorf("update exposure failed: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrExposureNotFound
	}
	return nil
}

func (s *Store) DeleteExposure(ctx context.Context, id uuid.UUID) error {
	cmd, err := s.pool.Exec(ctx, `DELETE FROM exposures WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete exposure: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrExposureNotFound
	}
	return nil
}
