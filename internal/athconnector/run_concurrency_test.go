package athconnector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// concurrentGatewayFake serves approved contexts keyed by conversation id.
type concurrentGatewayFake struct {
	mu       sync.Mutex
	statuses map[string]string
	convs    map[string]string
}

func (f *concurrentGatewayFake) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		conversation, _ := body["conversation_id"].(string)
		f.mu.Lock()
		status := f.statuses[conversation]
		f.convs[conversation] = status
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if status == "approved" {
			_, _ = w.Write([]byte(`{"request_id":"r","data":{"context_id":"` + uuid.New().String() +
				`","request_id":null,"status":"approved","revision":3,"binding_revision":2,"authorization_generation":5,"read_generation":4,"allowed_tools":["contract.read_bound"]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"request_id":"r","data":{"context_id":null,"request_id":null,"status":"` + status + `","revision":null,"binding_revision":null,"authorization_generation":null,"read_generation":null,"allowed_tools":[]}}`))
	}
}

func newOriginFor(t *testing.T, account string, epoch int, conversation string) *Origin {
	t.Helper()
	origin, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), account, epoch, "group", conversation, "actor-"+conversation, "msg-"+conversation)
	if err != nil {
		t.Fatal(err)
	}
	return origin
}

func TestConnectorRunsIsolateConcurrentScopesPerConversation(t *testing.T) {
	fake := &concurrentGatewayFake{statuses: map[string]string{
		"group-tenant":  "approved",
		"group-sales":   "approved",
		"group-pending": "pending",
	}, convs: map[string]string{}}
	_, signerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	binding := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}
	coordinator, err := NewRunCoordinator(client, binding, PurposeTenantContract, "ch")
	if err != nil {
		t.Fatal(err)
	}

	// Same account, two groups resolved concurrently: independent scope keys,
	// independent policies — one group's access never widens the other's.
	const workers = 8
	origins := []*Origin{newOriginFor(t, "account-1", 1, "group-tenant"), newOriginFor(t, "account-1", 1, "group-sales")}
	var wg sync.WaitGroup
	policies := make([]*RunPolicy, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			decision, err := coordinator.Resolve(context.Background(), origins[i%len(origins)])
			if err != nil {
				t.Errorf("worker %d: %v", i, err)
				return
			}
			policies[i] = decision.Policy
		}(i)
	}
	wg.Wait()
	byConversation := map[string]*RunPolicy{}
	for _, p := range policies {
		if p == nil {
			t.Fatal("missing policy")
		}
		byConversation[p.Origin.ConversationID()] = p
	}
	if len(byConversation) != 2 {
		t.Fatalf("two conversations must produce two distinct scopes, got %d", len(byConversation))
	}
	tenant, sales := byConversation["group-tenant"], byConversation["group-sales"]
	if tenant.ScopeKey == sales.ScopeKey || tenant.Scope.ContextID == sales.Scope.ContextID {
		t.Fatal("conversations must never share a scope key or context")
	}
	if len(tenant.ToolAllow()) != 2 || tenant.ToolAllow()[0] != OnboardingToolName {
		t.Fatalf("tool set must stay exclusive: %v", tenant.ToolAllow())
	}

	// Same group, two actors: the scope is the conversation, so both actors
	// share one namespace — membership of the approved group is the proof.
	a1 := newOriginFor(t, "account-1", 1, "group-tenant")
	a1Two, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 1, "group", "group-tenant", "actor-2", "msg-2")
	if err != nil {
		t.Fatal(err)
	}
	p1, err := coordinator.Resolve(context.Background(), a1)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := coordinator.Resolve(context.Background(), a1Two)
	if err != nil {
		t.Fatal(err)
	}
	if p1.Policy.ScopeKey != p2.Policy.ScopeKey {
		t.Fatal("same conversation must share one scope namespace for all members")
	}

	// Pending group stays data-free even while the others are approved.
	pending, err := coordinator.Resolve(context.Background(), newOriginFor(t, "account-1", 1, "group-pending"))
	if err != nil {
		t.Fatal(err)
	}
	if !pending.Policy.DataFree || pending.Policy.CredentialCache != nil {
		t.Fatal("pending scope must stay data-free")
	}
}

func TestConnectorRunsRejectForeignAccountAndInstance(t *testing.T) {
	fake := &concurrentGatewayFake{statuses: map[string]string{"group-1": "approved"}, convs: map[string]string{}}
	_, signerKey, _ := ed25519.GenerateKey(rand.Reader)
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	binding := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}
	coordinator, err := NewRunCoordinator(client, binding, PurposeTenantContract, "ch")
	if err != nil {
		t.Fatal(err)
	}

	// An origin minted for another provider account never reaches the gateway.
	if _, err := coordinator.Resolve(context.Background(), newOriginFor(t, "account-2", 1, "group-1")); err == nil {
		t.Fatal("foreign account must fail closed")
	}
	// A stale epoch (re-login without registration update) fails closed too.
	staleEpoch, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 2, "group", "group-1", "a", "m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Resolve(context.Background(), staleEpoch); err == nil {
		t.Fatal("stale epoch must fail closed")
	}
}
