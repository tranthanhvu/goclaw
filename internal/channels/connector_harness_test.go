package channels

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// Embed the interface so the store satisfies it with only the method the
// factory uses; other methods panic via the nil embedded interface.
type harnessMCPStoreWithPanic struct {
	store.MCPServerStore
	servers []store.MCPServerData
}

func (s *harnessMCPStoreWithPanic) ListServers(context.Context) ([]store.MCPServerData, error) {
	return s.servers, nil
}

var _ store.MCPServerStore = (*harnessMCPStoreWithPanic)(nil)

func connectorSettingsJSON(instanceID uuid.UUID, enabled bool) []byte {
	settings := `{"mode":"ath-connector","gateway_url":"http://127.0.0.1:56321","connector_id":"` +
		uuid.New().String() + `","environment":"development","issuer":"goclaw-ath","key_id":"key-1",` +
		`"private_key_file":"/run/secrets/ath-ed25519","channel_instance_id":"` + instanceID.String() +
		`","purpose":"tenant_contract"}`
	return []byte(settings)
}

func TestConnectorHarnessFactoryResolvesOwnedInstanceOnly(t *testing.T) {
	ownedID, otherID := uuid.New(), uuid.New()
	mcp := &harnessMCPStoreWithPanic{servers: []store.MCPServerData{
		{BaseModel: store.BaseModel{ID: uuid.New()}, Enabled: true, Settings: connectorSettingsJSON(ownedID, true)},
	}}
	accounts := &stubATHAccountStore{}

	factory := ConnectorHarnessFactoryFor(mcp, accounts)

	harness, err := factory(store.ChannelInstanceData{BaseModel: store.BaseModel{ID: ownedID}, TenantID: uuid.New(), Enabled: true})
	if err != nil || harness == nil {
		t.Fatalf("owned enabled instance must resolve a harness: %v %v", harness, err)
	}

	// Another instance, a disabled entry, or a disabled instance gets nothing.
	if harness, err := factory(store.ChannelInstanceData{BaseModel: store.BaseModel{ID: otherID}, TenantID: uuid.New(), Enabled: true}); err != nil || harness != nil {
		t.Fatalf("unowned instance must stay on the generic path: %v %v", harness, err)
	}
	mcp.servers[0].Enabled = false
	if harness, err := factory(store.ChannelInstanceData{BaseModel: store.BaseModel{ID: ownedID}, TenantID: uuid.New(), Enabled: true}); err != nil || harness != nil {
		t.Fatalf("disabled MCP entry must not arm: %v %v", harness, err)
	}
	mcp.servers[0].Enabled = true
	if harness, err := factory(store.ChannelInstanceData{BaseModel: store.BaseModel{ID: ownedID}, TenantID: uuid.New(), Enabled: false}); err != nil || harness != nil {
		t.Fatalf("disabled instance must not arm: %v %v", harness, err)
	}
}

type stubATHAccountStore struct{}

func (s *stubATHAccountStore) EnsureAccount(_ context.Context, _, _ uuid.UUID, _, providerAccountID string) (store.ATHChannelAccount, error) {
	return store.ATHChannelAccount{ID: uuid.New(), ProviderAccountID: providerAccountID, AccountEpoch: 1}, nil
}

var _ = athconnector.RuntimeRoleConnector
