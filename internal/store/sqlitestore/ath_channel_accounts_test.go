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

func TestATHAccountRegistryAdoptsConfiguredBinding(t *testing.T) {
	db, ctx := newTestATHAccountDB(t)
	s := NewSQLiteATHAccountStore(db)
	tenantID := store.MasterTenantID
	instanceID := uuid.New()
	seedChannelInstance(t, db, instanceID)

	athIssued := store.ATHAccountBinding{ID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 4}
	first, err := s.AlignAccount(ctx, tenantID, instanceID, "zalo_personal", athIssued)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != athIssued.ID || first.ProviderAccountID != "account-1" || first.AccountEpoch != 4 {
		t.Fatalf("registry must adopt the configured binding verbatim: %+v", first)
	}

	// Re-aligning the same configuration is idempotent.
	again, err := s.AlignAccount(ctx, tenantID, instanceID, "zalo_personal", athIssued)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("same configuration must be idempotent: %+v vs %+v", first, again)
	}

	// ATH re-issued the account (new epoch from an admin action): the row
	// realigns to the new configured truth; nothing is minted locally.
	reissued := athIssued
	reissued.AccountEpoch = 5
	reissued.ProviderAccountID = "account-2"
	realigned, err := s.AlignAccount(ctx, tenantID, instanceID, "zalo_personal", reissued)
	if err != nil {
		t.Fatal(err)
	}
	if realigned.ID != reissued.ID || realigned.ProviderAccountID != "account-2" || realigned.AccountEpoch != 5 {
		t.Fatalf("realignment wrong: %+v", realigned)
	}

	// Another instance keeps an independent row.
	otherInstance := uuid.New()
	seedChannelInstance(t, db, otherInstance)
	otherBinding := store.ATHAccountBinding{ID: uuid.New(), ProviderAccountID: "account-2", AccountEpoch: 1}
	other, err := s.AlignAccount(ctx, tenantID, otherInstance, "zalo_personal", otherBinding)
	if err != nil {
		t.Fatal(err)
	}
	if other.ID == reissued.ID || other.AccountEpoch != 1 {
		t.Fatalf("per-instance registry isolation broken: %+v", other)
	}
}

func TestATHAccountRegistryRejectsIncompleteInput(t *testing.T) {
	db, ctx := newTestATHAccountDB(t)
	s := NewSQLiteATHAccountStore(db)
	valid := store.ATHAccountBinding{ID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}
	if _, err := s.AlignAccount(ctx, store.MasterTenantID, uuid.Nil, "zalo_personal", valid); err == nil {
		t.Fatal("missing channel instance must fail")
	}
	if _, err := s.AlignAccount(ctx, store.MasterTenantID, uuid.New(), "zalo_personal", store.ATHAccountBinding{ID: uuid.New(), AccountEpoch: 1}); err == nil {
		t.Fatal("missing provider account must fail")
	}
	if _, err := s.AlignAccount(ctx, store.MasterTenantID, uuid.New(), "", valid); err == nil {
		t.Fatal("missing provider must fail")
	}
	unbound := valid
	unbound.ID = uuid.Nil
	if _, err := s.AlignAccount(ctx, store.MasterTenantID, uuid.New(), "zalo_personal", unbound); err == nil {
		t.Fatal("missing ATH-issued id must fail")
	}
	zeroEpoch := valid
	zeroEpoch.AccountEpoch = 0
	if _, err := s.AlignAccount(ctx, store.MasterTenantID, uuid.New(), "zalo_personal", zeroEpoch); err == nil {
		t.Fatal("zero epoch must fail")
	}
}
