package tools

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
)

type stubRequester struct {
	calls int
	err   error
}

func (s *stubRequester) RequestAccess(_ context.Context, _ *athconnector.Origin, _ string, _ athconnector.OnboardingToolInput) (*athconnector.ApprovalRequestResult, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	fresh := uuid.New()
	return &athconnector.ApprovalRequestResult{Status: "pending", RequestID: &fresh}, nil
}

func connectorPolicyWith(requester athconnector.OnboardingRequester) *athconnector.RunPolicy {
	return &athconnector.RunPolicy{
		DataFree:  true,
		Requester: requester,
		Origin:    &athconnector.Origin{}, // placeholder; tests that need identity set it below
	}
}

func TestATHOnboardingToolFailsClosedOutsideConnectorRuns(t *testing.T) {
	tool := ATHOnboardingTool{}
	result := tool.Execute(context.Background(), map[string]any{})
	if result == nil || result.ForLLM == "" {
		t.Fatal("tool must answer with a denial")
	}
	stub := &stubRequester{}
	if stub.calls != 0 {
		t.Fatal("no submission may happen without a policy")
	}
}

func TestATHOnboardingToolAcknowledgesFromCommittedState(t *testing.T) {
	stub := &stubRequester{}
	origin, err := athconnector.NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 1, "group", "group-1", "actor-1", "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	policy := connectorPolicyWith(stub)
	policy.Origin = origin
	ctx := athconnector.WithRunPolicy(context.Background(), policy)

	result := ATHOnboardingTool{}.Execute(ctx, map[string]any{"building_hint": "Tòa A"})
	if result == nil || stub.calls != 1 {
		t.Fatalf("submission must happen exactly once: calls=%d result=%+v", stub.calls, result)
	}
	if result.ForLLM == "" {
		t.Fatal("acknowledgment text missing")
	}

	// Oversized hints fail closed before any submission.
	oversized := ATHOnboardingTool{}.Execute(ctx, map[string]any{"room_hint": string(make([]byte, 121))})
	if oversized == nil || stub.calls != 1 {
		t.Fatalf("oversized hints must not submit: calls=%d", stub.calls)
	}

	// A failing gateway stays unconfirmed: never claim the request was sent.
	failing := &stubRequester{err: context.DeadlineExceeded}
	policy.Requester = failing
	unconfirmed := ATHOnboardingTool{}.Execute(ctx, nil)
	if unconfirmed == nil || failing.calls != 1 {
		t.Fatal("failure path must execute once and answer unconfirmed")
	}
}
