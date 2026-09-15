package channels

import (
	"context"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// athAccountAdapter bridges the store-backed account registry into the
// connector harness, keeping the athconnector package free of store imports.
type athAccountAdapter struct {
	registry store.ATHAccountStore
}

func (a athAccountAdapter) EnsureAccount(ctx context.Context, tenantID, channelInstanceID uuid.UUID, provider, providerAccountID string) (athconnector.AccountRecord, error) {
	account, err := a.registry.EnsureAccount(ctx, tenantID, channelInstanceID, provider, providerAccountID)
	if err != nil {
		return athconnector.AccountRecord{}, err
	}
	return athconnector.AccountRecord{ID: account.ID, ProviderAccountID: account.ProviderAccountID, AccountEpoch: account.AccountEpoch}, nil
}

// ConnectorHarnessFactoryFor builds the per-instance harness factory used by
// the connector-role runtime. It resolves the enabled MCP entry whose
// connector registration owns the channel instance; instances without a
// registration get no harness and keep the generic behavior.
func ConnectorHarnessFactoryFor(mcpStore store.MCPServerStore, accounts store.ATHAccountStore) func(inst store.ChannelInstanceData) (*athconnector.Harness, error) {
	return func(inst store.ChannelInstanceData) (*athconnector.Harness, error) {
		if mcpStore == nil || accounts == nil || !inst.Enabled {
			return nil, nil
		}
		ctx := store.WithTenantID(context.Background(), inst.TenantID)
		servers, err := mcpStore.ListServers(ctx)
		if err != nil {
			return nil, err
		}
		for _, srv := range servers {
			if !srv.Enabled {
				continue
			}
			registration, err := athconnector.ParseRegistration(srv.Settings)
			if err != nil || registration == nil {
				continue
			}
			if registration.ChannelInstanceID != inst.ID {
				continue
			}
			hints := athconnector.ApprovalHints{ChannelDisplayName: inst.DisplayName}
			return athconnector.NewHarness(registration.Config(), registration.Purpose, hints, athAccountAdapter{registry: accounts})
		}
		return nil, nil
	}
}
