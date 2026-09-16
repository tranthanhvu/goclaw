//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
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

// AlignAccount adopts the ATH-issued identity configured for the instance,
// mirroring the Postgres implementation exactly.
func (s *SQLiteATHAccountStore) AlignAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider string, configured store.ATHAccountBinding) (store.ATHChannelAccount, error) {
	if err := store.ValidateATHAccountInput(tenantID, channelInstanceID, provider, configured.ProviderAccountID); err != nil {
		return store.ATHChannelAccount{}, err
	}
	if configured.ID == uuid.Nil || configured.AccountEpoch < 1 {
		return store.ATHChannelAccount{}, errors.New("ath account: configured binding must carry the ATH-issued id and a positive epoch")
	}
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO ath_channel_accounts (id, tenant_id, channel_instance_id, provider, provider_account_id, account_epoch)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, channel_instance_id, provider) DO UPDATE SET
			id = excluded.id,
			provider_account_id = excluded.provider_account_id,
			account_epoch = excluded.account_epoch,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		RETURNING id, provider_account_id, account_epoch, created_at, updated_at`,
		configured.ID.String(), tenantID.String(), channelInstanceID.String(), provider, configured.ProviderAccountID, configured.AccountEpoch)
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
