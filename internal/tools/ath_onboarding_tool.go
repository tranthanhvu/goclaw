package tools

import (
	"context"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// ATHOnboardingTool is the local, non-MCP onboarding tool for connector runs.
// It is registered only in the connector-role runtime and fails closed in any
// other run: without a runtime-attached connector policy there is nothing to
// request. The model may only supply the two bounded hints; identity, purpose,
// and event idempotency come from the trusted origin in the policy.
type ATHOnboardingTool struct{}

func (ATHOnboardingTool) Name() string { return athconnector.OnboardingToolName }

func (ATHOnboardingTool) Description() string {
	return "Send this group's access request to the admin. Only call when the user asks for data or access that is currently unavailable. Optional hints: building_hint, room_hint — short text the user actually said."
}

func (ATHOnboardingTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"building_hint": map[string]any{
				"type":        "string",
				"description": "Optional building name the user mentioned (max 120 chars).",
			},
			"room_hint": map[string]any{
				"type":        "string",
				"description": "Optional room identifier the user mentioned (max 120 chars).",
			},
		},
		"additionalProperties": false,
	}
}

func (t ATHOnboardingTool) Execute(ctx context.Context, args map[string]any) *Result {
	policy := athconnector.RunPolicyFromContext(ctx)
	if policy == nil || policy.Requester == nil {
		return ErrorResult("Access requests are not available in this conversation.")
	}
	hints := athconnector.OnboardingToolInput{
		BuildingHint: stringArg(args, "building_hint"),
		RoomHint:     stringArg(args, "room_hint"),
	}
	outcome, _, err := athconnector.RunOnboardingForPolicy(ctx, policy, hints)
	if err != nil {
		// Uncommitted: never claim the request was sent; retrying is safe.
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingUnconfirmed))
	}
	switch outcome {
	case athconnector.AckSubmitted:
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingSubmitted))
	case athconnector.AckAwaiting:
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingPending))
	case athconnector.AckApproved:
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingApproved))
	case athconnector.AckDenied:
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingDenied))
	default:
		return NewResult(i18n.T(localeFrom(ctx), i18n.MsgConnectorOnboardingUnconfirmed))
	}
}

func stringArg(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	return ""
}

func localeFrom(ctx context.Context) string {
	if locale := store.LocaleFromContext(ctx); locale != "" {
		return locale
	}
	return i18n.LocaleVI
}

var _ Tool = ATHOnboardingTool{}
