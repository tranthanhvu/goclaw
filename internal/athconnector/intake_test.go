package athconnector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestSubmitApprovalRequestSendsBoundScopeAndBody(t *testing.T) {
	signer, _ := newTestSigner(t)
	accountID := uuid.New()
	var gotBody map[string]any
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("body decode: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"req-2","data":{"context_id":"` + uuid.New().String() + `","request_id":"` + uuid.New().String() + `","status":"pending"}}`))
	}))
	defer server.Close()

	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	binding := AccountBinding{AccountID: accountID, ProviderAccountID: "account-1", AccountEpoch: 3}
	origin, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 3, "group", "group-101", "actor-1", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.SubmitApprovalRequest(context.Background(), binding, origin, OnboardingEventKey("message-1"), PurposeTenantContract, ApprovalHints{ChannelDisplayName: "Zalo chính"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "pending" || result.RequestID == nil {
		t.Fatalf("result not parsed: %+v", result)
	}
	if gotBody["account_id"] != accountID.String() || gotBody["account_epoch"] != float64(3) {
		t.Fatalf("account scope missing from body: %#v", gotBody)
	}
	if gotBody["peer_kind"] != "group" || gotBody["conversation_id"] != "group-101" {
		t.Fatalf("conversation scope missing from body: %#v", gotBody)
	}
	if gotBody["event_key"] != "onboarding:message-1" || gotBody["purpose"] != "tenant_contract" {
		t.Fatalf("event identity missing from body: %#v", gotBody)
	}
	hints, _ := gotBody["hints"].(map[string]any)
	if hints == nil || hints["channel_display_name"] != "Zalo chính" {
		t.Fatalf("hints not delivered: %#v", gotBody["hints"])
	}
	claims := bearerClaims(t, gotAuth)
	if claims["account_id"] != accountID.String() || claims["conversation_id"] != "group-101" || claims["connector_id"] != client.Config().ConnectorID.String() {
		t.Fatalf("assertion scope not bound to trusted tuple: %#v", claims)
	}
}

func TestSubmitApprovalRequestFailsClosedOnBindingMismatch(t *testing.T) {
	signer, _ := newTestSigner(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("submission must not reach the gateway when the binding does not match the origin")
		w.WriteHeader(http.StatusTeapot)
	}))
	defer server.Close()
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	originOtherAccount, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-2", 3, "group", "group-101", "actor-1", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	binding := AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 3}
	if _, err := client.SubmitApprovalRequest(context.Background(), binding, originOtherAccount, "onboarding:message-1", PurposeTenantContract, ApprovalHints{}); err == nil {
		t.Fatal("origin minted for another account must fail closed")
	}
	originStaleEpoch, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 2, "group", "group-101", "actor-1", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SubmitApprovalRequest(context.Background(), binding, originStaleEpoch, "onboarding:message-1", PurposeTenantContract, ApprovalHints{}); err == nil {
		t.Fatal("stale account epoch must fail closed")
	}
}

func TestGroupIntakeServiceValidatesConstruction(t *testing.T) {
	signer, _ := newTestSigner(t)
	client, err := NewControlClient(connectorTestConfig("https://ath.example.test"), signer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGroupIntake(nil, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}, PurposeTenantContract, ApprovalHints{}); err == nil {
		t.Fatal("missing control client must fail")
	}
	if _, err := NewGroupIntake(client, AccountBinding{ProviderAccountID: "account-1", AccountEpoch: 1}, PurposeTenantContract, ApprovalHints{}); err == nil {
		t.Fatal("unbound account must fail")
	}
	if _, err := NewGroupIntake(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 1}, OnboardingPurpose("root"), ApprovalHints{}); err == nil {
		t.Fatal("unknown purpose must fail")
	}
	service, err := NewGroupIntake(client, AccountBinding{AccountID: uuid.New(), ProviderAccountID: "account-1", AccountEpoch: 4}, PurposeSalesInventory, ApprovalHints{})
	if err != nil {
		t.Fatal(err)
	}
	if !service.Enabled() || service.ProviderAccountID() != "account-1" || service.AccountEpoch() != 4 {
		t.Fatalf("service accessors wrong: %+v", service)
	}
}
