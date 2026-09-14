package athconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// OnboardingPurpose names the access shape an onboarding message asks the ATH
// admin to consider. The gateway rejects unknown values.
type OnboardingPurpose string

const (
	PurposeTenantContract   OnboardingPurpose = "tenant_contract"
	PurposeSalesInventory   OnboardingPurpose = "sales_inventory"
	PurposeManagementAccess OnboardingPurpose = "management_access"
)

// maxEventKeyLength matches the bounded identifier contract on the gateway.
const maxEventKeyLength = 300

// AccountBinding is the persisted, operator-registered identity of one provider
// login: the stable account UUID ATH knows, the verified provider account ID,
// and the current account epoch. A re-login under a different provider account
// must produce a new binding (and epoch) before any submission is accepted.
type AccountBinding struct {
	AccountID         uuid.UUID
	ProviderAccountID string
	AccountEpoch      int
}

func (b AccountBinding) validate() error {
	if b.AccountID == uuid.Nil {
		return errors.New("athconnector: account binding is missing its stable account id")
	}
	if b.ProviderAccountID == "" {
		return errors.New("athconnector: account binding is missing the provider account id")
	}
	if b.AccountEpoch < 1 {
		return fmt.Errorf("athconnector: account epoch must be positive, got %d", b.AccountEpoch)
	}
	return nil
}

// ApprovalHints are bounded, non-authoritative display hints attached to an
// onboarding submission. They never grant or widen access.
type ApprovalHints struct {
	GroupDisplayName   string `json:"group_display_name,omitempty"`
	BuildingHint       string `json:"building_hint,omitempty"`
	RoomHint           string `json:"room_hint,omitempty"`
	ChannelDisplayName string `json:"channel_display_name,omitempty"`
}

// ApprovalRequestResult is the committed outcome of a submission.
type ApprovalRequestResult struct {
	ContextID uuid.UUID
	RequestID *uuid.UUID
	Status    string // "pending" or "approved"
}

// OnboardingEventKey derives the idempotency key for one onboarding event from
// the verified provider message identity. Retries of the same event reuse the
// key; new messages produce new keys.
func OnboardingEventKey(providerMessageID string) string {
	return "onboarding:" + providerMessageID
}

type approvalRequestBody struct {
	AccountID      string            `json:"account_id"`
	AccountEpoch   int               `json:"account_epoch"`
	PeerKind       string            `json:"peer_kind"`
	ConversationID string            `json:"conversation_id"`
	EventKey       string            `json:"event_key"`
	Purpose        OnboardingPurpose `json:"purpose"`
	Hints          ApprovalHints     `json:"hints"`
}

// SubmitApprovalRequest submits one onboarding data request for an authentic,
// policy-denied group conversation. The binding must match the origin exactly:
// the verified provider account and the epoch at message time. Any mismatch —
// including an origin minted for another account or a stale epoch after
// re-login — fails closed before a network call is made.
func (c *ControlClient) SubmitApprovalRequest(ctx context.Context, binding AccountBinding, origin *Origin, eventKey string, purpose OnboardingPurpose, hints ApprovalHints) (*ApprovalRequestResult, error) {
	if origin == nil {
		return nil, errors.New("athconnector: trusted origin is required")
	}
	if err := binding.validate(); err != nil {
		return nil, err
	}
	if origin.ProviderAccountID() != binding.ProviderAccountID {
		return nil, fmt.Errorf("athconnector: refusing submission: origin account %q does not match registered account %q", origin.ProviderAccountID(), binding.ProviderAccountID)
	}
	if origin.AccountEpoch() != binding.AccountEpoch {
		return nil, fmt.Errorf("athconnector: refusing submission: origin epoch %d does not match registered epoch %d (account re-login?)", origin.AccountEpoch(), binding.AccountEpoch)
	}
	switch purpose {
	case PurposeTenantContract, PurposeSalesInventory, PurposeManagementAccess:
	default:
		return nil, fmt.Errorf("athconnector: unsupported onboarding purpose %q", purpose)
	}
	eventKey = strings.TrimSpace(eventKey)
	if eventKey == "" || len(eventKey) > maxEventKeyLength {
		return nil, errors.New("athconnector: event key is required and bounded")
	}
	body := approvalRequestBody{
		AccountID:      binding.AccountID.String(),
		AccountEpoch:   binding.AccountEpoch,
		PeerKind:       origin.ConversationKind(),
		ConversationID: origin.ConversationID(),
		EventKey:       eventKey,
		Purpose:        purpose,
		Hints:          hints,
	}
	scope := AssertionScope{
		ConnectorID:    c.cfg.ConnectorID,
		AccountID:      binding.AccountID,
		AccountEpoch:   binding.AccountEpoch,
		PeerKind:       origin.ConversationKind(),
		ConversationID: origin.ConversationID(),
	}
	var out struct {
		ContextID uuid.UUID  `json:"context_id"`
		RequestID *uuid.UUID `json:"request_id"`
		Status    string     `json:"status"`
	}
	if err := c.Call(ctx, PathApprovalRequests, "approval_request.submit", scope, body, &out); err != nil {
		return nil, err
	}
	if out.Status != "pending" && out.Status != "approved" {
		return nil, fmt.Errorf("athconnector: unexpected submission status %q", out.Status)
	}
	return &ApprovalRequestResult{ContextID: out.ContextID, RequestID: out.RequestID, Status: out.Status}, nil
}

// GroupIntake is the channel-facing restricted onboarding surface. The zalo
// personal channel consults it only for authentic conversations the generic
// group policy would deny or pair; it is inert when the connector is not
// enabled for the instance.
type GroupIntake interface {
	Enabled() bool
	ProviderAccountID() string
	AccountEpoch() int
	Submit(ctx context.Context, origin *Origin, eventKey string) (*ApprovalRequestResult, error)
}

// GroupIntakeService binds one registered account and purpose to a control
// client. It is constructed only by the isolated connector service assembly.
type GroupIntakeService struct {
	client  *ControlClient
	binding AccountBinding
	purpose OnboardingPurpose
	hints   ApprovalHints
}

// NewGroupIntake validates the binding and purpose before any message can
// reach the intake path.
func NewGroupIntake(client *ControlClient, binding AccountBinding, purpose OnboardingPurpose, hints ApprovalHints) (*GroupIntakeService, error) {
	if client == nil {
		return nil, errors.New("athconnector: control client is required for group intake")
	}
	if err := binding.validate(); err != nil {
		return nil, err
	}
	switch purpose {
	case PurposeTenantContract, PurposeSalesInventory, PurposeManagementAccess:
	default:
		return nil, fmt.Errorf("athconnector: unsupported onboarding purpose %q", purpose)
	}
	return &GroupIntakeService{client: client, binding: binding, purpose: purpose, hints: hints}, nil
}

// Enabled reports whether the intake is armed.
func (g *GroupIntakeService) Enabled() bool { return g != nil }

// ProviderAccountID returns the registered provider account identity.
func (g *GroupIntakeService) ProviderAccountID() string { return g.binding.ProviderAccountID }

// AccountEpoch returns the current registered account epoch.
func (g *GroupIntakeService) AccountEpoch() int { return g.binding.AccountEpoch }

// Submit forwards one onboarding event; the client cross-checks the origin
// against the registered binding before any network call.
func (g *GroupIntakeService) Submit(ctx context.Context, origin *Origin, eventKey string) (*ApprovalRequestResult, error) {
	return g.client.SubmitApprovalRequest(ctx, g.binding, origin, eventKey, g.purpose, g.hints)
}
