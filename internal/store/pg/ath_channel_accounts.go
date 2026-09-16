package pg

import (
	"context"
	"database/sql"
	"errors"

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

// AlignAccount adopts the ATH-issued identity configured for the instance.
// The gateway is the single authority for the id and epoch; a configuration
// change (account re-issue or epoch bump on ATH) realigns the row in place.
func (s *PGATHAccountStore) AlignAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider string, configured store.ATHAccountBinding) (store.ATHChannelAccount, error) {
	if err := store.ValidateATHAccountInput(tenantID, channelInstanceID, provider, configured.ProviderAccountID); err != nil {
		return store.ATHChannelAccount{}, err
	}
	if configured.ID == uuid.Nil || configured.AccountEpoch < 1 {
		return store.ATHChannelAccount{}, errors.New("ath account: configured binding must carry the ATH-issued id and a positive epoch")
	}
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO ath_channel_accounts (id, tenant_id, channel_instance_id, provider, provider_account_id, account_epoch)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, channel_instance_id, provider) DO UPDATE SET
			id = EXCLUDED.id,
			provider_account_id = EXCLUDED.provider_account_id,
			account_epoch = EXCLUDED.account_epoch,
			updated_at = now()
		RETURNING id, provider_account_id, account_epoch, created_at, updated_at`,
		configured.ID, tenantID, channelInstanceID, provider, configured.ProviderAccountID, configured.AccountEpoch)
	var account store.ATHChannelAccount
	account.TenantID = tenantID
	account.ChannelInstanceID = channelInstanceID
	account.Provider = provider
	if err := row.Scan(&account.ID, &account.ProviderAccountID, &account.AccountEpoch, &account.CreatedAt, &account.UpdatedAt); err != nil {
		return store.ATHChannelAccount{}, err
	}
	return account, nil
}
