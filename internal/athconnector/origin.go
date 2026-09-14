package athconnector

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ProviderZaloPersonal identifies the only channel currently able to carry
// ATH-trusted provenance. The constant is fixed at construction so consumers
// can rely on an exact-source match instead of string comparison at call sites.
const ProviderZaloPersonal = "zalo_personal"

// ConversationKindGroup and ConversationKindDirect restrict which peer shapes a
// trusted origin may describe. Tenant and sales traffic is group-only;
// management direct context is a separate opt-in.
const (
	ConversationKindGroup  = "group"
	ConversationKindDirect = "direct"
)

// Origin is the immutable trusted source tuple for one inbound provider event.
// It is constructed only from loader- and adapter-verified identity (tenant,
// channel instance, authenticated account, provider conversation and message
// IDs) and can never be minted from model output or public metadata. All
// fields are unexported; consumers read them through accessors so a forged
// struct literal cannot bypass the constructor validation.
type Origin struct {
	tenantID          uuid.UUID
	channelInstanceID uuid.UUID
	provider          string
	providerAccountID string
	accountEpoch      int
	conversationKind  string
	conversationID    string
	actorID           string
	providerMessageID string
}

// NewZaloPersonalOrigin validates and freezes the trusted tuple for a Zalo
// personal channel event. Empty account, conversation, actor, or provider
// message identity fails closed: an event without verifiable provenance must
// never reach the restricted intake.
func NewZaloPersonalOrigin(tenantID, channelInstanceID uuid.UUID, providerAccountID string, accountEpoch int, conversationKind, conversationID, actorID, providerMessageID string) (*Origin, error) {
	if tenantID == uuid.Nil {
		return nil, errors.New("athconnector: tenant id is required for trusted origin")
	}
	if channelInstanceID == uuid.Nil {
		return nil, errors.New("athconnector: channel instance id is required for trusted origin")
	}
	if providerAccountID == "" {
		return nil, errors.New("athconnector: authenticated provider account is required for trusted origin")
	}
	if accountEpoch < 1 {
		return nil, fmt.Errorf("athconnector: account epoch must be positive, got %d", accountEpoch)
	}
	switch conversationKind {
	case ConversationKindGroup, ConversationKindDirect:
	default:
		return nil, fmt.Errorf("athconnector: unsupported conversation kind %q", conversationKind)
	}
	if conversationID == "" {
		return nil, errors.New("athconnector: provider conversation id is required for trusted origin")
	}
	if actorID == "" {
		return nil, errors.New("athconnector: provider actor id is required for trusted origin")
	}
	if providerMessageID == "" {
		return nil, errors.New("athconnector: provider message id is required for trusted origin")
	}
	return &Origin{
		tenantID:          tenantID,
		channelInstanceID: channelInstanceID,
		provider:          ProviderZaloPersonal,
		providerAccountID: providerAccountID,
		accountEpoch:      accountEpoch,
		conversationKind:  conversationKind,
		conversationID:    conversationID,
		actorID:           actorID,
		providerMessageID: providerMessageID,
	}, nil
}

// TenantID returns the tenant owning the channel instance.
func (o *Origin) TenantID() uuid.UUID { return o.tenantID }

// ChannelInstanceID returns the registered channel instance identity.
func (o *Origin) ChannelInstanceID() uuid.UUID { return o.channelInstanceID }

// Provider returns the fixed provider tag, e.g. zalo_personal.
func (o *Origin) Provider() string { return o.provider }

// ProviderAccountID returns the authenticated provider account ID attached by
// the adapter, never from message metadata.
func (o *Origin) ProviderAccountID() string { return o.providerAccountID }

// AccountEpoch returns the monotonic account epoch; re-login under a different
// account invalidates contexts minted under earlier epochs.
func (o *Origin) AccountEpoch() int { return o.accountEpoch }

// ConversationKind reports group or direct.
func (o *Origin) ConversationKind() string { return o.conversationKind }

// ConversationID returns the verified provider conversation (group) ID.
func (o *Origin) ConversationID() string { return o.conversationID }

// ActorID returns the verified sender identity within the conversation.
func (o *Origin) ActorID() string { return o.actorID }

// ProviderMessageID returns the promoted provider event identity used for
// debounce/dedup, replacing any unverified metadata.message_id value.
func (o *Origin) ProviderMessageID() string { return o.providerMessageID }

// originWire is the serializable form of Origin. Inbound messages cross the
// bus boundary as JSON, so the trusted tuple must round-trip losslessly while
// still refusing forged values on decode.
type originWire struct {
	TenantID          uuid.UUID `json:"tenant_id"`
	ChannelInstanceID uuid.UUID `json:"channel_instance_id"`
	Provider          string    `json:"provider"`
	ProviderAccountID string    `json:"provider_account_id"`
	AccountEpoch      int       `json:"account_epoch"`
	ConversationKind  string    `json:"conversation_kind"`
	ConversationID    string    `json:"conversation_id"`
	ActorID           string    `json:"actor_id"`
	ProviderMessageID string    `json:"provider_message_id"`
}

// MarshalJSON serializes the trusted tuple.
func (o *Origin) MarshalJSON() ([]byte, error) {
	return json.Marshal(originWire{
		TenantID:          o.tenantID,
		ChannelInstanceID: o.channelInstanceID,
		Provider:          o.provider,
		ProviderAccountID: o.providerAccountID,
		AccountEpoch:      o.accountEpoch,
		ConversationKind:  o.conversationKind,
		ConversationID:    o.conversationID,
		ActorID:           o.actorID,
		ProviderMessageID: o.providerMessageID,
	})
}

// UnmarshalJSON rebuilds an Origin through the validating constructor. A wire
// value naming another provider, or missing any required identity, fails the
// whole decode: unknown provenance never propagates past the bus boundary.
func (o *Origin) UnmarshalJSON(data []byte) error {
	var w originWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	if w.Provider != ProviderZaloPersonal {
		return fmt.Errorf("athconnector: refusing origin with unsupported provider %q", w.Provider)
	}
	origin, err := NewZaloPersonalOrigin(w.TenantID, w.ChannelInstanceID, w.ProviderAccountID, w.AccountEpoch, w.ConversationKind, w.ConversationID, w.ActorID, w.ProviderMessageID)
	if err != nil {
		return err
	}
	*o = *origin
	return nil
}
