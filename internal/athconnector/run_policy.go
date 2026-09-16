package athconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// OnboardingToolName is the single local tool exposed to connector runs; it
// is never discovered through MCP and never model-renamable.
const OnboardingToolName = "ath_request_access"

// DataFreePrompt is the static, data-free system instruction for unapproved
// scopes. It must never contain business data, prior history, or credentials.
const DataFreePrompt = "You are the onboarding assistant for this group. You have no access to business data. " +
	"If the user asks for data or access, tell them their request can be sent to the admin and call the " +
	OnboardingToolName + " tool once with only the optional building/room hints they mentioned. " +
	"If a request was already sent, tell them it is awaiting admin approval. Do not invent statuses."

// ScopedPrompt is the system instruction for approved scopes; the allowed
// business capabilities are exactly the gateway-issued tool list.
const ScopedPrompt = "You answer only with the access granted to this group. Use only the provided tools; " +
	"never assume access to other buildings, rooms, invoices, or financial data. " +
	"If a request exceeds the granted access, say it is not permitted and offer to send an access request to the admin via the " +
	OnboardingToolName + " tool. Never reveal tokens, credentials, or internal identifiers."

// RunPolicy is the restrictive runtime policy for one connector run.
type RunPolicy struct {
	Scope    *RunScope
	Origin   *Origin
	Binding  AccountBinding
	ScopeKey string
	// DataFree marks every status except approved.
	DataFree bool
	// Tools is the exclusive tool set for the run.
	Tools []string
	// Prompt is the static system fragment for the run.
	Prompt string
	// CredentialCache is non-nil only for approved scopes.
	CredentialCache *ScopeClientCache
	// Requester submits onboarding requests for the local tool; the runtime
	// sets it, the model never can.
	Requester OnboardingRequester
}

// ToolAllow returns the exclusive tool allow list for the run.
func (p *RunPolicy) ToolAllow() []string { return p.Tools }

// RunCoordinator resolves live scopes for inbound trusted origins and builds
// the restrictive run policy. One instance serves the connector-role runtime.
type RunCoordinator struct {
	client             *ControlClient
	binding            AccountBinding
	cache              *ScopeClientCache
	purpose            OnboardingPurpose
	channelDisplayName string
}

// NewRunCoordinator binds the coordinator to the registration's control
// client and account binding.
func NewRunCoordinator(client *ControlClient, binding AccountBinding, purpose OnboardingPurpose, channelDisplayName string) (*RunCoordinator, error) {
	if client == nil {
		return nil, errors.New("athconnector: run coordinator requires a control client")
	}
	if err := binding.validate(); err != nil {
		return nil, err
	}
	cache, err := NewScopeClientCache(client)
	if err != nil {
		return nil, err
	}
	return &RunCoordinator{client: client, binding: binding, cache: cache, purpose: purpose, channelDisplayName: channelDisplayName}, nil
}

// CoordinatorDecision is the resolved policy plus the optional terminal
// denial the caller must deliver instead of running the agent.
type CoordinatorDecision struct {
	Policy *RunPolicy
	Denied bool
	Reply  string
}

// Resolve performs the live scope resolution for a trusted origin and builds
// the run policy. Terminal lifecycle statuses produce a denial reply without
// a run; every failure fails closed with an error (the caller drops the run).
func (r *RunCoordinator) Resolve(ctx context.Context, origin *Origin) (*CoordinatorDecision, error) {
	if origin == nil {
		return nil, errors.New("athconnector: run resolution requires a trusted origin")
	}
	scope, err := r.client.InspectContext(ctx, r.binding, origin)
	if err != nil {
		return nil, fmt.Errorf("athconnector: scope resolution failed: %w", err)
	}
	scopeKey := ScopeKey(r.client.Config().ConnectorID, r.binding.AccountID, origin.ConversationID(), scope)
	dataFree := scope.Status.DataFree()
	tools := []string{OnboardingToolName}
	if !dataFree {
		for _, name := range scope.AllowedTools {
			if name = strings.TrimSpace(name); name != "" {
				tools = append(tools, name)
			}
		}
	}
	decision := &CoordinatorDecision{
		Policy: &RunPolicy{
			Scope:    scope,
			Origin:   origin,
			Binding:  r.binding,
			ScopeKey: scopeKey,
			DataFree: dataFree,
			Tools:    tools,
			Prompt:   ScopedPrompt,
		},
	}
	decision.Policy.Requester = r.Requester()
	if dataFree {
		decision.Policy.Prompt = DataFreePrompt
	}
	switch scope.Status {
	case ScopeApproved:
		decision.Policy.CredentialCache = r.cache
		return decision, nil
	case ScopeUnlinked, ScopePending:
		return decision, nil
	case ScopeRejected, ScopeRevoked, ScopeReapprovalRequire:
		decision.Denied = true
		decision.Reply = "Access for this group is not granted. Please contact the admin."
		return decision, nil
	default:
		return nil, fmt.Errorf("athconnector: unhandled scope status %q", scope.Status)
	}
}

// Credential returns a live data credential for an approved scope.
func (r *RunCoordinator) Credential(ctx context.Context, policy *RunPolicy) (Credential, error) {
	if policy == nil || policy.CredentialCache == nil {
		return Credential{}, errors.New("athconnector: credentials exist only for approved scoped runs")
	}
	return r.cache.Credential(ctx, policy.ScopeKey, r.binding, policy.Origin, policy.Scope)
}

// Requester exposes the onboarding submission path for the local tool.
func (r *RunCoordinator) Requester() OnboardingRequester {
	return &ControlRequester{Client: r.client, Binding: r.binding, Purpose: r.purpose, ChannelDisplayName: r.channelDisplayName}
}

// Revalidate re-checks the credential-bound scope before prompt load, after
// tool responses, and before final delivery.
func (r *RunCoordinator) Revalidate(ctx context.Context, policy *RunPolicy, credentialID uuid.UUID) (bool, error) {
	if policy == nil || policy.Scope == nil || policy.Scope.ContextID == uuid.Nil {
		return false, errors.New("athconnector: revalidation requires an approved scope")
	}
	valid, _, err := r.client.RevalidateScope(ctx, r.binding, policy.Origin, policy.Scope.ContextID, credentialID)
	return valid, err
}

// runPolicyKey carries the connector run policy through the tool-execution
// context; only the runtime sets it.
type runPolicyCtxKey struct{}

// WithRunPolicy attaches the connector run policy to the context so the local
// onboarding tool can act with runtime-supplied identity.
func WithRunPolicy(ctx context.Context, policy *RunPolicy) context.Context {
	return context.WithValue(ctx, runPolicyCtxKey{}, policy)
}

// RunPolicyFromContext returns the connector run policy, or nil outside
// connector runs (the local tool fails closed then).
func RunPolicyFromContext(ctx context.Context) *RunPolicy {
	policy, _ := ctx.Value(runPolicyCtxKey{}).(*RunPolicy)
	return policy
}
