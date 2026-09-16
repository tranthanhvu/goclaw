package athconnector

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ScopeStatus mirrors the gateway context lifecycle.
type ScopeStatus string

const (
	ScopeUnlinked          ScopeStatus = "unlinked"
	ScopePending           ScopeStatus = "pending"
	ScopeApproved          ScopeStatus = "approved"
	ScopeRejected          ScopeStatus = "rejected"
	ScopeRevoked           ScopeStatus = "revoked"
	ScopeReapprovalRequire ScopeStatus = "reapproval_required"
)

// DataFree reports whether the status only ever permits a data-free run.
func (s ScopeStatus) DataFree() bool { return s != ScopeApproved }

// RunScope is the live authorization scope for one trusted conversation,
// resolved from the gateway before any historical or business context loads.
type RunScope struct {
	ContextID               uuid.UUID
	RequestID               *uuid.UUID
	Status                  ScopeStatus
	Revision                int
	BindingRevision         int
	AuthorizationGeneration int
	ReadGeneration          int
	AllowedTools            []string
}

type contextOutput struct {
	ContextID               *uuid.UUID `json:"context_id"`
	RequestID               *uuid.UUID `json:"request_id"`
	Status                  string     `json:"status"`
	Revision                *int       `json:"revision"`
	BindingRevision         *int       `json:"binding_revision"`
	AuthorizationGeneration *int       `json:"authorization_generation"`
	ReadGeneration          *int       `json:"read_generation"`
	AllowedTools            []string   `json:"allowed_tools"`
}

func conversationScopeBody(binding AccountBinding, origin *Origin) map[string]any {
	return map[string]any{
		"account_id":      binding.AccountID.String(),
		"account_epoch":   binding.AccountEpoch,
		"peer_kind":       origin.ConversationKind(),
		"conversation_id": origin.ConversationID(),
	}
}

// InspectContext resolves the live scope for a trusted conversation. The
// caller must do this before loading history, memory, or any business data:
// unapproved statuses only ever permit data-free runs.
func (c *ControlClient) InspectContext(ctx context.Context, binding AccountBinding, origin *Origin) (*RunScope, error) {
	if err := binding.validate(); err != nil {
		return nil, err
	}
	if origin == nil {
		return nil, errors.New("athconnector: trusted origin is required for scope resolution")
	}
	if origin.ProviderAccountID() != binding.ProviderAccountID || origin.AccountEpoch() != binding.AccountEpoch {
		return nil, errors.New("athconnector: refusing scope resolution: origin does not match the registered binding")
	}
	var out contextOutput
	scope := AssertionScope{
		ConnectorID:    c.cfg.ConnectorID,
		AccountID:      binding.AccountID,
		AccountEpoch:   binding.AccountEpoch,
		PeerKind:       origin.ConversationKind(),
		ConversationID: origin.ConversationID(),
	}
	if err := c.Call(ctx, PathContext, "context.inspect", scope, conversationScopeBody(binding, origin), &out); err != nil {
		return nil, err
	}
	runScope, err := runScopeFromOutput(out)
	if err != nil {
		return nil, err
	}
	return runScope, nil
}

// RevalidateScope re-checks the live scope bound to one credential right
// before prompt load, after tool responses, and before final delivery.
func (c *ControlClient) RevalidateScope(ctx context.Context, binding AccountBinding, origin *Origin, contextID, credentialID uuid.UUID) (bool, *RunScope, error) {
	if err := binding.validate(); err != nil {
		return false, nil, err
	}
	if origin == nil || contextID == uuid.Nil || credentialID == uuid.Nil {
		return false, nil, errors.New("athconnector: revalidation requires the origin, context and credential identity")
	}
	body := conversationScopeBody(binding, origin)
	body["context_id"] = contextID.String()
	body["credential_id"] = credentialID.String()
	var out struct {
		contextOutput
		Valid bool `json:"valid"`
	}
	scope := AssertionScope{
		ConnectorID:    c.cfg.ConnectorID,
		AccountID:      binding.AccountID,
		AccountEpoch:   binding.AccountEpoch,
		PeerKind:       origin.ConversationKind(),
		ConversationID: origin.ConversationID(),
	}
	if err := c.Call(ctx, PathRevalidate, "context.revalidate", scope, body, &out); err != nil {
		return false, nil, err
	}
	runScope, err := runScopeFromOutput(out.contextOutput)
	if err != nil {
		return false, nil, err
	}
	return out.Valid, runScope, nil
}

func runScopeFromOutput(out contextOutput) (*RunScope, error) {
	status := ScopeStatus(out.Status)
	switch status {
	case ScopeUnlinked, ScopePending, ScopeApproved, ScopeRejected, ScopeRevoked, ScopeReapprovalRequire:
	default:
		return nil, fmt.Errorf("athconnector: unknown scope status %q", out.Status)
	}
	scope := RunScope{Status: status, AllowedTools: out.AllowedTools, RequestID: out.RequestID}
	if out.ContextID != nil {
		scope.ContextID = *out.ContextID
	}
	if out.Revision != nil {
		scope.Revision = *out.Revision
	}
	if out.BindingRevision != nil {
		scope.BindingRevision = *out.BindingRevision
	}
	if out.AuthorizationGeneration != nil {
		scope.AuthorizationGeneration = *out.AuthorizationGeneration
	}
	if out.ReadGeneration != nil {
		scope.ReadGeneration = *out.ReadGeneration
	}
	if status == ScopeApproved {
		if scope.ContextID == uuid.Nil || scope.BindingRevision < 1 || scope.AuthorizationGeneration < 1 || scope.ReadGeneration < 1 {
			return nil, errors.New("athconnector: approved scope is missing binding or generation identity")
		}
	}
	if len(scope.AllowedTools) > 9 {
		return nil, errors.New("athconnector: allowed tool list exceeds the gateway contract")
	}
	return &scope, nil
}

// ScopeKey builds the session namespace for a connector run. Approved scopes
// pin the full authorization identity: a revoke, remap, or read-generation
// bump rotates the key, so history from an earlier namespace is never loaded
// into a newer scope. Unapproved statuses share one data-free namespace.
func ScopeKey(connectorID, accountID uuid.UUID, conversationID string, scope *RunScope) string {
	identity := fmt.Sprintf("%s:%s:%s", connectorID, accountID, conversationID)
	if scope == nil || scope.Status != ScopeApproved {
		return identity + ":datafree"
	}
	return fmt.Sprintf("%s:b%d:a%d:r%d", identity, scope.BindingRevision, scope.AuthorizationGeneration, scope.ReadGeneration)
}
