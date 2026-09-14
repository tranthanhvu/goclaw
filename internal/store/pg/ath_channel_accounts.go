package pg

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// PGATHAccountStore implements store.ATHAccountStore backed by Postgres.
type PGATHAccountStore struct {
	db *sql.DB
}

func NewPGATHAccountStore(db *sql.DB) *PGATHAccountStore {
	return &PGATHAccountStore{db: db}
}

// EnsureAccount registers the current provider login for a channel instance.
// The upsert is the single race-safe mutation path: the same provider account
// re-registers idempotently; a different provider account bumps the epoch in
// the same statement, so concurrent callers observe one monotonic history.
func (s *PGATHAccountStore) EnsureAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (store.ATHChannelAccount, error) {
	if err := store.ValidateATHAccountInput(tenantID, channelInstanceID, provider, providerAccountID); err != nil {
		return store.ATHChannelAccount{}, err
	}
	id := store.GenNewID()
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO ath_channel_accounts (id, tenant_id, channel_instance_id, provider, provider_account_id, account_epoch)
		VALUES ($1, $2, $3, $4, $5, 1)
		ON CONFLICT (tenant_id, channel_instance_id, provider) DO UPDATE SET
			provider_account_id = EXCLUDED.provider_account_id,
			account_epoch = CASE
				WHEN ath_channel_accounts.provider_account_id = EXCLUDED.provider_account_id
				THEN ath_channel_accounts.account_epoch
				ELSE ath_channel_accounts.account_epoch + 1
			END,
			updated_at = now()
		RETURNING id, provider_account_id, account_epoch, created_at, updated_at`,
		id, tenantID, channelInstanceID, provider, providerAccountID)
	var account store.ATHChannelAccount
	account.TenantID = tenantID
	account.ChannelInstanceID = channelInstanceID
	account.Provider = provider
	if err := row.Scan(&account.ID, &account.ProviderAccountID, &account.AccountEpoch, &account.CreatedAt, &account.UpdatedAt); err != nil {
		return store.ATHChannelAccount{}, err
	}
	return account, nil
}
