package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ATHChannelAccount is the stable, operator-registered identity of one provider
// login on a channel instance. ID is the stable account UUID bound into ATH
// connector assertions; ProviderAccountID is the verified provider-side login
// identity; AccountEpoch rises monotonically whenever a different provider
// account logs in on the same instance, invalidating contexts minted under the
// previous account.
type ATHChannelAccount struct {
	ID                uuid.UUID `json:"id"`
	TenantID          uuid.UUID `json:"tenant_id"`
	ChannelInstanceID uuid.UUID `json:"channel_instance_id"`
	Provider          string    `json:"provider"`
	ProviderAccountID string    `json:"provider_account_id"`
	AccountEpoch      int       `json:"account_epoch"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// ATHAccountStore persists channel account registrations. EnsureAccount is the
// only mutation path: re-registering the same provider account is idempotent;
// registering a different provider account on the same instance bumps the epoch.
type ATHAccountStore interface {
	EnsureAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (ATHChannelAccount, error)
}

// ValidateATHAccountInput guards the registry invariants at the store boundary.
func ValidateATHAccountInput(tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) error {
	if tenantID == uuid.Nil {
		return errors.New("ath account: tenant id is required")
	}
	if channelInstanceID == uuid.Nil {
		return errors.New("ath account: channel instance id is required")
	}
	if provider == "" {
		return errors.New("ath account: provider is required")
	}
	if providerAccountID == "" {
		return errors.New("ath account: provider account id is required")
	}
	if len(provider) > 40 {
		return fmt.Errorf("ath account: provider %q exceeds 40 characters", provider)
	}
	return nil
}
