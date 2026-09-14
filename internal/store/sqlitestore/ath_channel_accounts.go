//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// SQLiteATHAccountStore implements store.ATHAccountStore backed by SQLite.
type SQLiteATHAccountStore struct {
	db *sql.DB
}

func NewSQLiteATHAccountStore(db *sql.DB) *SQLiteATHAccountStore {
	return &SQLiteATHAccountStore{db: db}
}

// EnsureAccount registers the current provider login for a channel instance;
// the same provider account is idempotent, a different provider account bumps
// the epoch atomically in one upsert.
func (s *SQLiteATHAccountStore) EnsureAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (store.ATHChannelAccount, error) {
	if err := store.ValidateATHAccountInput(tenantID, channelInstanceID, provider, providerAccountID); err != nil {
		return store.ATHChannelAccount{}, err
	}
	id := store.GenNewID()
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO ath_channel_accounts (id, tenant_id, channel_instance_id, provider, provider_account_id, account_epoch)
		VALUES (?, ?, ?, ?, ?, 1)
		ON CONFLICT (tenant_id, channel_instance_id, provider) DO UPDATE SET
			provider_account_id = excluded.provider_account_id,
			account_epoch = CASE
				WHEN ath_channel_accounts.provider_account_id = excluded.provider_account_id
				THEN ath_channel_accounts.account_epoch
				ELSE ath_channel_accounts.account_epoch + 1
			END,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		RETURNING id, provider_account_id, account_epoch, created_at, updated_at`,
		id.String(), tenantID.String(), channelInstanceID.String(), provider, providerAccountID)
	var account store.ATHChannelAccount
	account.TenantID = tenantID
	account.ChannelInstanceID = channelInstanceID
	account.Provider = provider
	var createdAt, updatedAt string
	if err := row.Scan(&account.ID, &account.ProviderAccountID, &account.AccountEpoch, &createdAt, &updatedAt); err != nil {
		return store.ATHChannelAccount{}, err
	}
	if parsed, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		account.CreatedAt = parsed
	}
	if parsed, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		account.UpdatedAt = parsed
	}
	return account, nil
}
