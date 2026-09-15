package athconnector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AccountRecord is the registry's answer for one channel instance login.
type AccountRecord struct {
	ID                uuid.UUID
	ProviderAccountID string
	AccountEpoch      int
}

// AccountRegistry resolves (and registers) the stable account identity for a
// provider login on a channel instance. It is backed by the ATH channel
// account store in the wiring layer.
type AccountRegistry interface {
	EnsureAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (AccountRecord, error)
}

// Harness carries the connector enablement for one channel instance from the
// loader to the channel. The intake itself is armed lazily at channel Start,
// when the authenticated provider account identity becomes available; until
// then the restricted intake stays inert and generic policy applies.
type Harness struct {
	cfg      Config
	purpose  OnboardingPurpose
	hints    ApprovalHints
	accounts AccountRegistry
	now      func() time.Time
}

// NewHarness validates the enablement before any channel can carry it.
func NewHarness(cfg Config, purpose OnboardingPurpose, hints ApprovalHints, accounts AccountRegistry) (*Harness, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch purpose {
	case PurposeTenantContract, PurposeSalesInventory, PurposeManagementAccess:
	default:
		return nil, fmt.Errorf("athconnector: unsupported onboarding purpose %q", purpose)
	}
	if accounts == nil {
		return nil, errors.New("athconnector: account registry is required")
	}
	now := time.Now
	return &Harness{cfg: cfg, purpose: purpose, hints: hints, accounts: accounts, now: now}, nil
}

// ArmGroupIntake loads the signer key, builds the pinned control client, and
// registers the verified provider login in the account registry (minting the
// stable identity on first login, bumping the epoch on re-login). It fails
// closed on any error: the caller must leave the restricted intake unarmed.
func (h *Harness) ArmGroupIntake(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (GroupIntake, error) {
	key, err := LoadPrivateKey(h.cfg.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	signer := NewSigner(h.cfg.Issuer, h.cfg.Environment, h.cfg.KeyID, key, h.now)
	if signer == nil {
		return nil, errors.New("athconnector: signer construction failed")
	}
	client, err := NewControlClient(h.cfg, signer)
	if err != nil {
		return nil, err
	}
	account, err := h.accounts.EnsureAccount(ctx, tenantID, channelInstanceID, provider, providerAccountID)
	if err != nil {
		return nil, fmt.Errorf("athconnector: account registration failed: %w", err)
	}
	if account.ProviderAccountID != providerAccountID || account.AccountEpoch < 1 {
		return nil, errors.New("athconnector: account registry returned an inconsistent binding")
	}
	binding := AccountBinding{AccountID: account.ID, ProviderAccountID: account.ProviderAccountID, AccountEpoch: account.AccountEpoch}
	return NewGroupIntake(client, binding, h.purpose, h.hints)
}
