//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

func newTestATHAccountDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	db, err := OpenDB(filepath.Join(t.TempDir(), "ath-accounts.db"))
	if err != nil {
		t.Fatalf("OpenDB error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema error: %v", err)
	}
	ctx := store.WithCrossTenant(store.WithTenantID(context.Background(), store.MasterTenantID))
	return db, ctx
}

// seedChannelInstance inserts the minimal parent rows (agent + channel
// instance) the registry's foreign keys require.
func seedChannelInstance(t *testing.T, db *sql.DB, instanceID uuid.UUID) {
	t.Helper()
	agentID := uuid.New().String()
	if _, err := db.Exec(
		`INSERT INTO agents (id, agent_key, owner_id, model, tenant_id) VALUES (?, ?, ?, ?, ?)`,
		agentID, "agent-"+agentID, "tester", "test-model", store.MasterTenantID.String()); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO channel_instances (id, name, channel_type, agent_id, tenant_id, enabled, created_by)
		 VALUES (?, ?, ?, ?, ?, 1, 'tester')`,
		instanceID.String(), "zalo-"+instanceID.String(), "zalo_personal", agentID, store.MasterTenantID.String()); err != nil {
		t.Fatalf("seed channel instance: %v", err)
	}
}

func TestATHAccountRegistryMintsStableIdentityAndBumpsEpochOnRelogin(t *testing.T) {
	db, ctx := newTestATHAccountDB(t)
	s := NewSQLiteATHAccountStore(db)
	tenantID := store.MasterTenantID
	instanceID := uuid.New()
	seedChannelInstance(t, db, instanceID)

	first, err := s.EnsureAccount(ctx, tenantID, instanceID, "zalo_personal", "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.AccountEpoch != 1 || first.ProviderAccountID != "account-1" {
		t.Fatalf("first registration wrong: %+v", first)
	}

	// Same provider login re-registers idempotently with the same identity.
	again, err := s.EnsureAccount(ctx, tenantID, instanceID, "zalo_personal", "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID || again.AccountEpoch != 1 {
		t.Fatalf("same login must be idempotent: %+v vs %+v", first, again)
	}

	// A different provider account on the same instance bumps the epoch while
	// keeping the stable account UUID: old contexts are invalidated.
	relogin, err := s.EnsureAccount(ctx, tenantID, instanceID, "zalo_personal", "account-2")
	if err != nil {
		t.Fatal(err)
	}
	if relogin.ID != first.ID {
		t.Fatalf("stable account UUID must survive re-login: %s vs %s", first.ID, relogin.ID)
	}
	if relogin.ProviderAccountID != "account-2" || relogin.AccountEpoch != 2 {
		t.Fatalf("re-login must bump epoch: %+v", relogin)
	}

	// Another instance keeps an independent identity at epoch 1.
	otherInstance := uuid.New()
	seedChannelInstance(t, db, otherInstance)
	other, err := s.EnsureAccount(ctx, tenantID, otherInstance, "zalo_personal", "account-2")
	if err != nil {
		t.Fatal(err)
	}
	if other.ID == first.ID || other.AccountEpoch != 1 {
		t.Fatalf("per-instance registry isolation broken: %+v", other)
	}
}

func TestATHAccountRegistryRejectsIncompleteInput(t *testing.T) {
	db, ctx := newTestATHAccountDB(t)
	s := NewSQLiteATHAccountStore(db)
	if _, err := s.EnsureAccount(ctx, store.MasterTenantID, uuid.Nil, "zalo_personal", "account-1"); err == nil {
		t.Fatal("missing channel instance must fail")
	}
	if _, err := s.EnsureAccount(ctx, store.MasterTenantID, uuid.New(), "zalo_personal", ""); err == nil {
		t.Fatal("missing provider account must fail")
	}
	if _, err := s.EnsureAccount(ctx, store.MasterTenantID, uuid.New(), "", "account-1"); err == nil {
		t.Fatal("missing provider must fail")
	}
}
