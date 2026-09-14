package personal

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/zalo/personal/protocol"
	"github.com/nextlevelbuilder/goclaw/internal/i18n"
)

// connectorIntakeEnabled reports whether the ATH connector restricted intake is
// armed for this channel. It is set once by the isolated connector service
// assembly before Start; the generic runtime leaves it nil.
func (c *Channel) connectorIntakeEnabled() bool {
	return c.connectorIntake != nil && c.connectorIntake.Enabled()
}

// onboardingDebounce bounds how often one denied group can trigger the
// onboarding path (submission + reply), mirroring pairingDebounce.
const onboardingDebounce = 60 * time.Second

// onboardingSubmitTimeout bounds the detached control-plane submission.
const onboardingSubmitTimeout = 15 * time.Second

// buildGroupOrigin freezes the trusted source tuple for one authentic group
// message: tenant and channel instance identity from the loader, the
// authenticated account and epoch from the registered binding, and the
// provider-verified conversation/actor/message IDs from the adapter. A missing
// session account or instance identity fails closed — the message never
// continues as a trusted event.
func (c *Channel) buildGroupOrigin(threadID, senderID, providerMessageID string) (*athconnector.Origin, error) {
	sess := c.session()
	if sess == nil || sess.UID == "" {
		return nil, errors.New("authenticated provider account unavailable")
	}
	if c.ChannelInstanceID() == uuid.Nil {
		return nil, errors.New("channel instance identity unavailable")
	}
	return athconnector.NewZaloPersonalOrigin(
		c.TenantID(),
		c.ChannelInstanceID(),
		sess.UID,
		c.connectorIntake.AccountEpoch(),
		athconnector.ConversationKindGroup,
		threadID,
		senderID,
		providerMessageID,
	)
}

// maybeHandleConnectorOnboarding runs the restricted intake for an authentic
// group the generic policy would deny or pair. It reports true when the event
// was consumed by the onboarding path. Unknown provenance or a failed
// submission is dropped without a confirmation reply: the sender may safely
// retry, and the gateway dedups by event key.
//
// Submission and reply run on a bounded goroutine so a slow or unavailable ATH
// control plane never stalls the sequential zalo listen loop (head-of-line
// blocking for DMs and allowed groups).
func (c *Channel) maybeHandleConnectorOnboarding(ctx context.Context, senderID, threadID string, mentioned bool, providerMessageID string) bool {
	if !c.connectorIntakeEnabled() {
		return false
	}
	if c.CheckGroupPolicy(ctx, senderID, threadID, c.config.GroupPolicy) == channels.PolicyAllow {
		return false // linked group: continue the generic flow
	}
	if c.RequireMention() && !mentioned {
		return true // not triggered: drop like the generic denied path
	}
	origin, err := c.buildGroupOrigin(threadID, senderID, providerMessageID)
	if err != nil {
		slog.Warn("zalo_personal connector origin unavailable; dropping event", "group_id", threadID, "error", err)
		return true
	}
	// Debounce per conversation, mirroring the generic pairing reply: each
	// mention in a denied group otherwise forces a control-plane request and a
	// bot reply at will; the server caps submission growth, the debounce caps
	// noise and load. Mark synchronously so bursts collapse before the first
	// detached submission completes.
	if !c.CanSendPairingNotif("group:"+threadID, onboardingDebounce) {
		return true
	}
	c.MarkPairingNotifSent("group:" + threadID)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Warn("zalo_personal connector onboarding panicked", "group_id", threadID, "panic", r)
			}
		}()
		submitCtx, cancel := context.WithTimeout(ctx, onboardingSubmitTimeout)
		defer cancel()
		result, err := c.connectorIntake.Submit(submitCtx, origin, athconnector.OnboardingEventKey(providerMessageID))
		if err != nil || result == nil {
			slog.Warn("zalo_personal connector submission not confirmed; no confirmation sent", "group_id", threadID, "error", err)
			return
		}
		c.sendOnboardingReply(threadID, result)
	}()
	return true
}

// sendOnboardingReply confirms the committed onboarding outcome to the group.
// The reply text is chosen by durable state, never by unconfirmed intent.
func (c *Channel) sendOnboardingReply(threadID string, result *athconnector.ApprovalRequestResult) {
	key := i18n.MsgConnectorOnboardingSubmitted
	switch {
	case result.Status == "approved":
		key = i18n.MsgConnectorOnboardingApproved
	case result.RequestID == nil:
		key = i18n.MsgConnectorOnboardingPending
	}
	sess := c.session()
	if sess == nil {
		return
	}
	sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := protocol.SendMessage(sendCtx, sess, threadID, protocol.ThreadTypeGroup, i18n.T(i18n.LocaleVI, key)); err != nil {
		slog.Warn("zalo_personal onboarding reply failed", "group_id", threadID, "error", err)
	}
}
