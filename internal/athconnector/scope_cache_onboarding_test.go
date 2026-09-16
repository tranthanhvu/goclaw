package athconnector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func scopeTestClient(t *testing.T, handler http.HandlerFunc) (*ControlClient, *httptest.Server) {
	t.Helper()
	_, signerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := NewSigner("goclaw-ath", "development", "key-1", signerKey, time.Now)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func scopeTestOrigin(t *testing.T) (*Origin, AccountBinding) {
	t.Helper()
	binding := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 2}
	origin, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 2, "group", "group-77", "actor-1", "msg-9")
	if err != nil {
		t.Fatal(err)
	}
	return origin, binding
}

func TestInspectContextResolvesApprovedAndUnlinkedVariants(t *testing.T) {
	origin, binding := scopeTestOrigin(t)
	var gotAction string
	client, _ := scopeTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotAction = fmt.Sprint(body["conversation_id"])
		w.Header().Set("Content-Type", "application/json")
		if gotAction == "group-77" {
			_, _ = w.Write([]byte(`{"request_id":"r1","data":{"context_id":"` + uuid.New().String() + `","request_id":null,"status":"approved","revision":3,"binding_revision":2,"authorization_generation":5,"read_generation":4,"allowed_tools":["contract.read_bound"]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"request_id":"r2","data":{"context_id":null,"request_id":null,"status":"unlinked","revision":null,"binding_revision":null,"authorization_generation":null,"read_generation":null,"allowed_tools":[]}}`))
	}))

	approved, err := client.InspectContext(context.Background(), binding, origin)
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != ScopeApproved || approved.BindingRevision != 2 || approved.AuthorizationGeneration != 5 || approved.ReadGeneration != 4 {
		t.Fatalf("approved scope wrong: %+v", approved)
	}
	if len(approved.AllowedTools) != 1 || approved.AllowedTools[0] != "contract.read_bound" {
		t.Fatalf("allowed tools wrong: %+v", approved.AllowedTools)
	}
	key := ScopeKey(client.Config().ConnectorID, binding.AccountID, origin.ConversationID(), approved)
	if !strings.Contains(key, ":b2:a5:r4") {
		t.Fatalf("scope key must pin binding and generations: %s", key)
	}

	fallback, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 2, "group", "group-88", "actor-1", "msg-10")
	if err != nil {
		t.Fatal(err)
	}
	unlinked, err := client.InspectContext(context.Background(), binding, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if unlinked.Status != ScopeUnlinked || unlinked.ContextID != uuid.Nil {
		t.Fatalf("unlinked scope wrong: %+v", unlinked)
	}
	dataFreeKey := ScopeKey(client.Config().ConnectorID, binding.AccountID, "group-88", unlinked)
	if !strings.HasSuffix(dataFreeKey, ":datafree") {
		t.Fatalf("unapproved scope must use the data-free namespace: %s", dataFreeKey)
	}

	// Origin minted for another account or epoch never reaches the gateway.
	foreign, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-2", 2, "group", "group-77", "actor-1", "msg-11")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.InspectContext(context.Background(), binding, foreign); err == nil {
		t.Fatal("origin/account mismatch must fail closed")
	}
}

func TestApprovedScopeRequiresFullIdentity(t *testing.T) {
	origin, binding := scopeTestOrigin(t)
	client, _ := scopeTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"r1","data":{"context_id":"` + uuid.New().String() + `","request_id":null,"status":"approved","revision":1,"binding_revision":0,"authorization_generation":1,"read_generation":1,"allowed_tools":[]}}`))
	}))
	if _, err := client.InspectContext(context.Background(), binding, origin); err == nil {
		t.Fatal("approved scope without binding identity must fail")
	}
}

func TestRevalidateScopeReportsValidity(t *testing.T) {
	origin, binding := scopeTestOrigin(t)
	client, _ := scopeTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"r1","data":{"context_id":"` + uuid.New().String() + `","request_id":null,"status":"revoked","revision":2,"binding_revision":2,"authorization_generation":6,"read_generation":4,"allowed_tools":[],"valid":false}}`))
	}))
	valid, scope, err := client.RevalidateScope(context.Background(), binding, origin, uuid.New(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if valid || scope.Status != ScopeRevoked {
		t.Fatalf("revocation must invalidate: valid=%v status=%s", valid, scope.Status)
	}
	if AcknowledgmentForScope(scope) != AckDenied {
		t.Fatal("revoked scope must acknowledge denied")
	}
}

func TestScopeCacheSingleFlightAndRotation(t *testing.T) {
	origin, binding := scopeTestOrigin(t)
	scope := &RunScope{Status: ScopeApproved, ContextID: uuid.New(), BindingRevision: 1, AuthorizationGeneration: 1, ReadGeneration: 1}

	var mu sync.Mutex
	issued := 0
	inflight := 0
	maxInflight := 0
	client, _ := scopeTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, PathToken) {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		mu.Lock()
		issued++
		inflight++
		if inflight > maxInflight {
			maxInflight = inflight
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond) // widen the single-flight window
		mu.Lock()
		inflight--
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"r","data":{"credential_id":"` + uuid.New().String() + `","plaintext_credential":"agw1.secret.part.` + fmt.Sprint(issued) + `.sig","expires_at":"` + time.Now().Add(120*time.Second).Format(time.RFC3339) + `","replayed":false}}`))
	}))

	cache, err := NewScopeClientCache(client)
	if err != nil {
		t.Fatal(err)
	}
	const waiters = 8
	var wg sync.WaitGroup
	creds := make([]Credential, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cred, err := cache.Credential(context.Background(), "scope-key", binding, origin, scope)
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			creds[i] = cred
		}(i)
	}
	wg.Wait()
	if issued != 1 {
		t.Fatalf("single-flight must collapse concurrent issuance, got %d issues", issued)
	}
	for i := 1; i < waiters; i++ {
		if creds[i].CredentialID != creds[0].CredentialID {
			t.Fatalf("waiters must share one credential: %v vs %v", creds[i].CredentialID, creds[0].CredentialID)
		}
	}

	// A near-expiry credential rotates; Drop forces a fresh issuance.
	cache.Drop("scope-key")
	cred, err := cache.Credential(context.Background(), "scope-key", binding, origin, scope)
	if err != nil {
		t.Fatal(err)
	}
	if issued != 2 || cred.CredentialID == creds[0].CredentialID {
		t.Fatalf("drop must force a new issuance: issued=%d", issued)
	}

	// Data-free scopes never issue.
	if _, err := cache.Credential(context.Background(), "k", binding, origin, &RunScope{Status: ScopePending}); err == nil {
		t.Fatal("unapproved scope must not issue credentials")
	}
	_ = maxInflight
}

func TestOnboardingAcknowledgmentsAndBoundedHints(t *testing.T) {
	fresh := uuid.New()
	if ResolveAcknowledgment(&ApprovalRequestResult{Status: "pending", RequestID: &fresh}) != AckSubmitted {
		t.Fatal("fresh pending must acknowledge submitted")
	}
	if ResolveAcknowledgment(&ApprovalRequestResult{Status: "pending", RequestID: nil}) != AckAwaiting {
		t.Fatal("deduped pending must acknowledge awaiting")
	}
	if ResolveAcknowledgment(&ApprovalRequestResult{Status: "approved"}) != AckApproved {
		t.Fatal("approval must acknowledge approved")
	}
	if ResolveAcknowledgment(nil) != AckUnconfirmed || ResolveAcknowledgment(&ApprovalRequestResult{Status: "weird"}) != AckUnconfirmed {
		t.Fatal("unknown states must stay unconfirmed")
	}
	if AcknowledgmentForScope(&RunScope{Status: ScopeRejected}) != AckDenied {
		t.Fatal("rejected must acknowledge denied")
	}

	origin, _ := scopeTestOrigin(t)
	requests := 0
	tool, err := NewOnboardingTool(fakeRequester{func(ctx context.Context, o *Origin, key string, hints OnboardingToolInput) (*ApprovalRequestResult, error) {
		requests++
		if o != origin {
			t.Fatal("runtime origin must be carried, not model-chosen")
		}
		if len(key) == 0 || len(key) > 300 {
			t.Fatalf("event key must be bounded: %q", key)
		}
		return &ApprovalRequestResult{Status: "pending", RequestID: &fresh}, nil
	}}, func() (*Origin, error) { return origin, nil }, func() string { return "onboarding:tool-event-1" })
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, err := tool.Request(context.Background(), OnboardingToolInput{BuildingHint: "Tòa A"})
	if err != nil || outcome != AckSubmitted || requests != 1 {
		t.Fatalf("tool request wrong: outcome=%v err=%v requests=%d", outcome, err, requests)
	}
	// Oversized hints fail closed before any request.
	if _, _, err := tool.Request(context.Background(), OnboardingToolInput{RoomHint: strings.Repeat("x", 121)}); err == nil || requests != 1 {
		t.Fatal("oversized hints must fail closed without a submission")
	}
	// Gateway failure stays unconfirmed.
	failing, _ := NewOnboardingTool(fakeRequester{func(ctx context.Context, o *Origin, key string, hints OnboardingToolInput) (*ApprovalRequestResult, error) {
		return nil, context.DeadlineExceeded
	}}, func() (*Origin, error) { return origin, nil }, func() string { return "e" })
	if outcome, _, err := failing.Request(context.Background(), OnboardingToolInput{}); err == nil || outcome != AckUnconfirmed {
		t.Fatal("submission failure must stay unconfirmed")
	}
}

type fakeRequester struct {
	inner func(ctx context.Context, origin *Origin, eventKey string, hints OnboardingToolInput) (*ApprovalRequestResult, error)
}

func (f fakeRequester) RequestAccess(ctx context.Context, origin *Origin, eventKey string, hints OnboardingToolInput) (*ApprovalRequestResult, error) {
	return f.inner(ctx, origin, eventKey, hints)
}
