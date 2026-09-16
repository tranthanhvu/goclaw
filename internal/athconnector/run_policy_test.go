package athconnector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func coordinatorFixture(t *testing.T, status string, allowedTools string) (*RunCoordinator, *Origin) {
	t.Helper()
	origin, binding := scopeTestOrigin(t)
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch status {
		case "approved":
			_, _ = w.Write([]byte(`{"request_id":"r","data":{"context_id":"` + uuid.New().String() +
				`","request_id":null,"status":"approved","revision":3,"binding_revision":2,"authorization_generation":5,"read_generation":4,"allowed_tools":[` + allowedTools + `]}}`))
		case "pending":
			_, _ = w.Write([]byte(`{"request_id":"r","data":{"context_id":"` + uuid.New().String() +
				`","request_id":"` + uuid.New().String() + `","status":"pending","revision":1,"binding_revision":1,"authorization_generation":1,"read_generation":1,"allowed_tools":[]}}`))
		default:
			_, _ = w.Write([]byte(`{"request_id":"r","data":{"context_id":"` + uuid.New().String() +
				`","request_id":null,"status":"` + status + `","revision":2,"binding_revision":2,"authorization_generation":6,"read_generation":4,"allowed_tools":[]}}`))
		}
	}
	client, _ := scopeTestClient(t, handler)
	coordinator, err := NewRunCoordinator(client, binding, PurposeTenantContract, "Zalo chính")
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, origin
}

func TestRunCoordinatorApprovedScopeBuildsRestrictedPolicy(t *testing.T) {
	coordinator, origin := coordinatorFixture(t, "approved", `"contract.read_bound"`)
	decision, err := coordinator.Resolve(context.Background(), origin)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Denied || decision.Policy.DataFree {
		t.Fatal("approved scope must run with data access")
	}
	tools := decision.Policy.ToolAllow()
	if tools[0] != OnboardingToolName || tools[1] != "contract.read_bound" || len(tools) != 2 {
		t.Fatalf("tool set must be exactly the onboarding tool plus gateway-issued tools: %v", tools)
	}
	if decision.Policy.CredentialCache == nil {
		t.Fatal("approved policy must carry the credential cache")
	}
	if decision.Policy.ScopeKey == "" || decision.Policy.Prompt != ScopedPrompt {
		t.Fatal("scoped prompt/scope key missing")
	}
}

func TestRunCoordinatorUnapprovedScopesStayDataFree(t *testing.T) {
	for _, status := range []string{"unlinked", "pending"} {
		coordinator, origin := coordinatorFixture(t, status, "")
		decision, err := coordinator.Resolve(context.Background(), origin)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Denied {
			t.Fatalf("%s must still run data-free, not deny", status)
		}
		if !decision.Policy.DataFree || decision.Policy.Prompt != DataFreePrompt {
			t.Fatalf("%s policy must be data-free: %+v", status, decision.Policy)
		}
		if tools := decision.Policy.ToolAllow(); len(tools) != 1 || tools[0] != OnboardingToolName {
			t.Fatalf("%s must only expose the onboarding tool: %v", status, tools)
		}
		if decision.Policy.CredentialCache != nil {
			t.Fatalf("%s must not carry credentials", status)
		}
	}
}

func TestRunCoordinatorTerminalStatusesDenyWithoutRun(t *testing.T) {
	for _, status := range []string{"rejected", "revoked", "reapproval_required"} {
		coordinator, origin := coordinatorFixture(t, status, "")
		decision, err := coordinator.Resolve(context.Background(), origin)
		if err != nil {
			t.Fatal(err)
		}
		if !decision.Denied || decision.Reply == "" {
			t.Fatalf("%s must deny with a reply", status)
		}
		if !decision.Policy.DataFree {
			t.Fatalf("%s policy stays data-free", status)
		}
	}
}

func TestRunCoordinatorFailsClosedOnGatewayOutage(t *testing.T) {
	origin, binding := scopeTestOrigin(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, _ := scopeTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	coordinator, err := NewRunCoordinator(client, binding, PurposeTenantContract, "ch")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Resolve(context.Background(), origin); err == nil {
		t.Fatal("gateway outage must fail closed: no policy, no run")
	}
	if _, err := coordinator.Resolve(context.Background(), nil); err == nil {
		t.Fatal("missing trusted origin must fail closed")
	}
}
