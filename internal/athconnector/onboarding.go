package athconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// OnboardingOutcome is the durable acknowledgment the runtime may send; it is
// decided exclusively by the gateway's committed state, never by intent.
type OnboardingOutcome string

const (
	// AckSubmitted: a fresh pending request was just committed.
	AckSubmitted OnboardingOutcome = "submitted"
	// AckAwaiting: an existing pending request already covers this group.
	AckAwaiting OnboardingOutcome = "awaiting"
	// AckApproved: the group is already approved (direct-link merged).
	AckApproved OnboardingOutcome = "approved"
	// AckDenied: rejected/revoked/reapproval_required — access is not granted.
	AckDenied OnboardingOutcome = "denied"
	// AckUnconfirmed: the submission did not commit; a retry is safe.
	AckUnconfirmed OnboardingOutcome = "unconfirmed"
)

// ResolveAcknowledgment maps a committed submission result to the durable
// acknowledgment outcome.
func ResolveAcknowledgment(result *ApprovalRequestResult) OnboardingOutcome {
	if result == nil {
		return AckUnconfirmed
	}
	switch result.Status {
	case "approved":
		return AckApproved
	case "pending":
		if result.RequestID == nil {
			return AckAwaiting
		}
		return AckSubmitted
	default:
		return AckUnconfirmed
	}
}

// AcknowledgmentForScope maps the live scope of a data attempt to its
// outcome: linked-but-forbidden answers denied, terminal lifecycle statuses
// answer denied without creating a new request, and anything else falls back
// to the pending path.
func AcknowledgmentForScope(scope *RunScope) OnboardingOutcome {
	if scope == nil {
		return AckUnconfirmed
	}
	switch scope.Status {
	case ScopeApproved:
		return AckDenied // linked but the requested capability is out of scope
	case ScopeRejected, ScopeRevoked, ScopeReapprovalRequire:
		return AckDenied
	default:
		return AckUnconfirmed // route through the pending submission path
	}
}

// OnboardingHintsBounds guard the local tool's model-facing inputs: only
// bounded display hints, never identity, grants, or free-form notes.
const (
	OnboardingHintMaxLen = 120
)

// OnboardingToolInput is the strict schema the model may supply.
type OnboardingToolInput struct {
	BuildingHint string `json:"building_hint,omitempty"`
	RoomHint     string `json:"room_hint,omitempty"`
}

// ValidateOnboardingHints bounds every hint; anything over the limit fails
// closed rather than being truncated silently.
func ValidateOnboardingHints(hints OnboardingToolInput) error {
	for name, value := range map[string]string{
		"building_hint": hints.BuildingHint,
		"room_hint":     hints.RoomHint,
	} {
		trimmed := strings.TrimSpace(value)
		if len(trimmed) > OnboardingHintMaxLen {
			return fmt.Errorf("athconnector: %s exceeds %d characters", name, OnboardingHintMaxLen)
		}
	}
	return nil
}

// OnboardingRequester submits a data-access request for one trusted scope.
// Implemented by the control client path; kept as an interface so the local
// tool stays testable without a gateway.
type OnboardingRequester interface {
	RequestAccess(ctx context.Context, origin *Origin, eventKey string, hints OnboardingToolInput) (*ApprovalRequestResult, error)
}

// OnboardingTool is the local, non-MCP tool exposed to connector runs. It
// carries only bounded hints: the trusted origin, purpose, and event identity
// come from the runtime, never from the model.
type OnboardingTool struct {
	requester OnboardingRequester
	origin    func() (*Origin, error)
	newEvent  func() string
}

// NewOnboardingTool binds the tool to a requester and the run's trusted
// origin. origin/newEvent are supplied per run by the runtime.
func NewOnboardingTool(requester OnboardingRequester, origin func() (*Origin, error), newEvent func() string) (*OnboardingTool, error) {
	if requester == nil || origin == nil || newEvent == nil {
		return nil, errors.New("athconnector: onboarding tool requires a requester, origin and event source")
	}
	return &OnboardingTool{requester: requester, origin: origin, newEvent: newEvent}, nil
}

// Request maps validated model hints plus runtime identity to a committed
// submission and its durable acknowledgment.
func (t *OnboardingTool) Request(ctx context.Context, hints OnboardingToolInput) (OnboardingOutcome, *ApprovalRequestResult, error) {
	if err := ValidateOnboardingHints(hints); err != nil {
		return AckUnconfirmed, nil, err
	}
	origin, err := t.origin()
	if err != nil {
		return AckUnconfirmed, nil, err
	}
	eventKey := t.newEvent()
	if eventKey == "" {
		return AckUnconfirmed, nil, errors.New("athconnector: onboarding event identity unavailable")
	}
	result, err := t.requester.RequestAccess(ctx, origin, eventKey, hints)
	if err != nil {
		// Uncommitted: never acknowledge as sent; the caller may safely retry.
		return AckUnconfirmed, nil, err
	}
	return ResolveAcknowledgment(result), result, nil
}

// ControlRequester adapts the control client to the onboarding tool with the
// run's fixed purpose and channel display name.
type ControlRequester struct {
	Client             *ControlClient
	Binding            AccountBinding
	Purpose            OnboardingPurpose
	ChannelDisplayName string
}

// RequestAccess submits an approval request carrying only bounded hints.
func (r *ControlRequester) RequestAccess(ctx context.Context, origin *Origin, eventKey string, hints OnboardingToolInput) (*ApprovalRequestResult, error) {
	if r.Client == nil {
		return nil, errors.New("athconnector: control client unavailable")
	}
	approvalHints := ApprovalHints{
		BuildingHint:       strings.TrimSpace(hints.BuildingHint),
		RoomHint:           strings.TrimSpace(hints.RoomHint),
		ChannelDisplayName: r.ChannelDisplayName,
	}
	return r.Client.SubmitApprovalRequest(ctx, r.Binding, origin, eventKey, r.Purpose, approvalHints)
}

// RunOnboardingForPolicy executes one onboarding submission for a connector
// run. Identity comes exclusively from the runtime policy; the event key is
// stable per run (the trusted provider message), so retries are idempotent at
// the gateway.
func RunOnboardingForPolicy(ctx context.Context, policy *RunPolicy, hints OnboardingToolInput) (OnboardingOutcome, *ApprovalRequestResult, error) {
	if policy == nil || policy.Requester == nil || policy.Origin == nil {
		return AckUnconfirmed, nil, errors.New("athconnector: no onboarding capability for this run")
	}
	if err := ValidateOnboardingHints(hints); err != nil {
		return AckUnconfirmed, nil, err
	}
	result, err := policy.Requester.RequestAccess(ctx, policy.Origin, OnboardingEventKey(policy.Origin.ProviderMessageID()), hints)
	if err != nil {
		return AckUnconfirmed, nil, err
	}
	return ResolveAcknowledgment(result), result, nil
}
