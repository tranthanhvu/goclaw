package personal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels/zalo/personal/protocol"
	"github.com/nextlevelbuilder/goclaw/internal/config"
)

type fakeIntake struct {
	armed     bool
	account   string
	epoch     int
	mu        sync.Mutex
	submitted []*athconnector.Origin
	eventKeys []string
	result    *athconnector.ApprovalRequestResult
	err       error
}

func (f *fakeIntake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.submitted)
}

func (f *fakeIntake) submission(i int) *athconnector.Origin {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.submitted[i]
}

// waitForSubmissions polls until the detached onboarding goroutine recorded n
// submissions; the submission runs off the listen loop.
func waitForSubmissions(t *testing.T, f *fakeIntake, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d submissions, got %d", n, f.count())
}

func (f *fakeIntake) Enabled() bool { return f.armed }

func (f *fakeIntake) ProviderAccountID() string { return f.account }

func (f *fakeIntake) AccountEpoch() int { return f.epoch }

func (f *fakeIntake) Submit(_ context.Context, origin *athconnector.Origin, eventKey string) (*athconnector.ApprovalRequestResult, error) {
	f.mu.Lock()
	f.submitted = append(f.submitted, origin)
	f.eventKeys = append(f.eventKeys, eventKey)
	f.mu.Unlock()
	return f.result, f.err
}

func newIntakeTestChannel(t *testing.T) *Channel {
	t.Helper()
	ch, err := New(config.ZaloPersonalConfig{GroupPolicy: "allowlist", AllowFrom: []string{"someone-else"}}, bus.New(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestConnectorIntakeConsumesDeniedUnknownGroupOnceMentioned(t *testing.T) {
	ch := newIntakeTestChannel(t)
	instanceID := uuid.New()
	ch.SetChannelInstanceID(instanceID)
	ch.SetTenantID(uuid.New())
	intake := &fakeIntake{armed: true, account: "account-1", epoch: 2}
	newRequest := uuid.New()
	intake.result = &athconnector.ApprovalRequestResult{ContextID: uuid.New(), RequestID: &newRequest, Status: "pending"}
	ch.SetGroupIntake(intake)
	ch.mu.Lock()
	ch.sess = &protocol.Session{UID: "account-1"}
	ch.mu.Unlock()

	ctx := context.Background()
	if !ch.maybeHandleConnectorOnboarding(ctx, "sender-1", "group-404", false, "msg-1") {
		t.Fatal("unmentioned message in a denied group must be consumed silently, not fall through to the generic path")
	}
	if intake.count() != 0 {
		t.Fatal("no submission before the mention trigger")
	}
	if !ch.maybeHandleConnectorOnboarding(ctx, "sender-1", "group-404", true, "msg-1") {
		t.Fatal("mentioned denied group must be consumed by the restricted intake")
	}
	waitForSubmissions(t, intake, 1)
	origin := intake.submission(0)
	if origin.TenantID() != ch.TenantID() || origin.ChannelInstanceID() != instanceID {
		t.Fatal("origin not bound to loader identity")
	}
	if origin.ProviderAccountID() != "account-1" || origin.AccountEpoch() != 2 {
		t.Fatal("origin not bound to the registered account epoch")
	}
	if origin.ConversationID() != "group-404" || origin.ActorID() != "sender-1" || origin.ProviderMessageID() != "msg-1" {
		t.Fatal("origin not bound to the verified provider event")
	}
	if intake.eventKeys[0] != athconnector.OnboardingEventKey("msg-1") {
		t.Fatalf("event key %q", intake.eventKeys[0])
	}
}
func TestConnectorIntakeLeavesAllowedGroupsOnGenericPath(t *testing.T) {
	ch := newIntakeTestChannel(t)
	ch.SetChannelInstanceID(uuid.New())
	intake := &fakeIntake{armed: true, account: "account-1", epoch: 1}
	ch.SetGroupIntake(intake)
	ch.mu.Lock()
	ch.sess = &protocol.Session{UID: "account-1"}
	ch.mu.Unlock()

	// Open policy: every group is allowed, so the intake must not consume it.
	ch.config.GroupPolicy = "open"
	if ch.maybeHandleConnectorOnboarding(context.Background(), "sender-1", "group-1", true, "msg-1") {
		t.Fatal("policy-allowed group must continue on the generic path")
	}
	if intake.count() != 0 {
		t.Fatal("allowed group must never reach the restricted intake")
	}
}

func TestConnectorIntakeFailsClosedWithoutVerifiedAccount(t *testing.T) {
	ch := newIntakeTestChannel(t)
	ch.SetChannelInstanceID(uuid.New())
	intake := &fakeIntake{armed: true, account: "account-1", epoch: 1}
	ch.SetGroupIntake(intake)
	// No session installed: provenance cannot be proven.

	if !ch.maybeHandleConnectorOnboarding(context.Background(), "sender-1", "group-404", true, "msg-1") {
		t.Fatal("denied group must still be consumed (never fall through to the generic path)")
	}
	if intake.count() != 0 {
		t.Fatal("event without provable provenance must never be submitted")
	}

	unarmed := newIntakeTestChannel(t)
	unarmed.SetChannelInstanceID(uuid.New())
	if unarmed.maybeHandleConnectorOnboarding(context.Background(), "sender-1", "group-404", true, "msg-1") {
		t.Fatal("unarmed connector must leave the decision to the generic policy")
	}
}

func TestConnectorIntakeDebouncesRepeatedTriggers(t *testing.T) {
	ch := newIntakeTestChannel(t)
	ch.SetChannelInstanceID(uuid.New())
	ch.SetTenantID(uuid.New())
	intake := &fakeIntake{armed: true, account: "account-1", epoch: 1}
	ch.SetGroupIntake(intake)
	ch.mu.Lock()
	ch.sess = &protocol.Session{UID: "account-1"}
	ch.mu.Unlock()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if !ch.maybeHandleConnectorOnboarding(ctx, "sender-1", "group-404", true, "msg-debounce") {
			t.Fatal("denied group must be consumed")
		}
	}
	waitForSubmissions(t, intake, 1)
	time.Sleep(50 * time.Millisecond)
	if intake.count() != 1 {
		t.Fatalf("debounce must collapse bursts, got %d submissions", intake.count())
	}
}
