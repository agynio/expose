package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/agynio/expose/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpdateExposureProvisionedRejectsIncompleteResources(t *testing.T) {
	st := New(nil)
	err := st.UpdateExposureProvisioned(context.Background(), uuid.New(), ExposureResourceIDs{
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		URL:                  "http://exposed.agyn:8080",
	})
	if !errors.Is(err, ErrExposureResourcesIncomplete) {
		t.Fatalf("expected incomplete resources error, got %v", err)
	}
}

func TestExposureActiveResourcesConstraint(t *testing.T) {
	databaseURL := os.Getenv("EXPOSE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("EXPOSE_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations, exposures`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	if err := db.ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	exposureID := uuid.New()
	st := New(pool)
	if err := st.CreateExposure(ctx, Exposure{
		ID:         exposureID,
		WorkloadID: uuid.New(),
		AgentID:    uuid.New(),
		Port:       8080,
		Status:     ExposureStatusProvisioning,
	}); err != nil {
		t.Fatalf("create exposure: %v", err)
	}

	if err := st.UpdateExposureProvisioned(ctx, exposureID, ExposureResourceIDs{
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
		URL:                  "http://exposed.agyn:8080",
	}); err != nil {
		t.Fatalf("update exposure provisioned: %v", err)
	}

	stored, err := st.GetExposure(ctx, exposureID)
	if err != nil {
		t.Fatalf("get exposure: %v", err)
	}
	if stored.Status != ExposureStatusActive {
		t.Fatalf("expected active status, got %d", stored.Status)
	}

	_, err = pool.Exec(ctx, `INSERT INTO exposures (id, workload_id, agent_id, port, status) VALUES ($1, $2, $3, $4, $5)`, uuid.New(), uuid.New(), uuid.New(), 8081, ExposureStatusActive)
	if !isCheckViolation(err, "exposures_active_resources_check") {
		t.Fatalf("expected active resources check violation, got %v", err)
	}
}

func isCheckViolation(err error, constraintName string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && pgErr.ConstraintName == constraintName
}
