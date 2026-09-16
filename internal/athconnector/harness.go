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

// AccountRegistry records the ATH-issued account identity configured for a
// channel instance. The gateway owns the identity: AlignAccount adopts the
// configured values verbatim (keeping a local bookkeeping row) and never
// mints ids or bumps epochs on its own.
type AccountRegistry interface {
	AlignAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider string, configured AccountBinding) (AccountRecord, error)
}

// Harness carries the connector enablement for one channel instance from the
// loader to the channel. The intake itself is armed lazily at channel Start,
// when the authenticated provider account identity becomes available; until
// then the restricted intake stays inert and generic policy applies.
type Harness struct {
	cfg      Config
	account  AccountBinding
	purpose  OnboardingPurpose
	hints    ApprovalHints
	accounts AccountRegistry
	now      func() time.Time
}

// NewHarness validates the enablement before any channel can carry it.
func NewHarness(cfg Config, account AccountBinding, purpose OnboardingPurpose, hints ApprovalHints, accounts AccountRegistry) (*Harness, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := account.validate(); err != nil {
		return nil, fmt.Errorf("athconnector: configured account: %w", err)
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
	return &Harness{cfg: cfg, account: account, purpose: purpose, hints: hints, accounts: accounts, now: now}, nil
}

// ArmGroupIntake loads the signer key, builds the pinned control client, and
// registers the verified provider login in the account registry (minting the
// stable identity on first login, bumping the epoch on re-login). It fails
// closed on any error: the caller must leave the restricted intake unarmed.
func (h *Harness) ArmGroupIntake(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (GroupIntake, error) {
	client, binding, err := h.arm(ctx, tenantID, channelInstanceID, provider, providerAccountID)
	if err != nil {
		return nil, err
	}
	return NewGroupIntake(client, binding, h.purpose, h.hints)
}

// ArmCatalogWorker arms the group catalog refresh worker for the same
// verified login. The poll interval bounds idle claim attempts; source is the
// channel adapter over the authenticated provider session.
func (h *Harness) ArmCatalogWorker(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID, workerID string, source GroupCatalogSource, poll time.Duration) (*GroupCatalogWorker, error) {
	client, binding, err := h.arm(ctx, tenantID, channelInstanceID, provider, providerAccountID)
	if err != nil {
		return nil, err
	}
	return NewGroupCatalogWorker(client, binding, source, channelInstanceID, provider, workerID, poll)
}

// arm builds the control client and the current account binding.
func (h *Harness) arm(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (*ControlClient, AccountBinding, error) {
	key, err := LoadPrivateKey(h.cfg.PrivateKeyFile)
	if err != nil {
		return nil, AccountBinding{}, err
	}
	signer := NewSigner(h.cfg.Issuer, h.cfg.Environment, h.cfg.KeyID, key, h.now)
	if signer == nil {
		return nil, AccountBinding{}, errors.New("athconnector: signer construction failed")
	}
	client, err := NewControlClient(h.cfg, signer)
	if err != nil {
		return nil, AccountBinding{}, err
	}
	// The configured ATH-issued binding is the truth the runtime signs with;
	// a provider login under a different account fails closed at submission
	// (origin/binding mismatch) until the operator updates the registration.
	if providerAccountID != h.account.ProviderAccountID {
		return nil, AccountBinding{}, fmt.Errorf("athconnector: provider account %q does not match the registered account %q; update the registration", providerAccountID, h.account.ProviderAccountID)
	}
	record, err := h.accounts.AlignAccount(ctx, tenantID, channelInstanceID, provider, h.account)
	if err != nil {
		return nil, AccountBinding{}, fmt.Errorf("athconnector: account alignment failed: %w", err)
	}
	if record.ID != h.account.AccountID || record.AccountEpoch != h.account.AccountEpoch || record.ProviderAccountID != h.account.ProviderAccountID {
		return nil, AccountBinding{}, errors.New("athconnector: account registry diverged from the configured binding")
	}
	return client, h.account, nil
}
