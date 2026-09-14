package athconnector

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestZaloPersonalOriginCarriesImmutableProviderIdentity(t *testing.T) {
	tenantID := uuid.New()
	instanceID := uuid.New()
	origin, err := NewZaloPersonalOrigin(tenantID, instanceID, "account-1", 3, "group", "group-101", "actor-1", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	if origin.TenantID() != tenantID || origin.ChannelInstanceID() != instanceID || origin.Provider() != "zalo_personal" {
		t.Fatal("trusted source tuple changed")
	}
	if origin.ProviderAccountID() != "account-1" || origin.AccountEpoch() != 3 || origin.ConversationID() != "group-101" || origin.ProviderMessageID() != "message-1" {
		t.Fatal("provider tuple changed")
	}
	if _, err := NewZaloPersonalOrigin(tenantID, instanceID, "", 1, "group", "group-101", "actor-1", "message-1"); err == nil {
		t.Fatal("missing authenticated provider account must fail")
	}
	if _, err := NewZaloPersonalOrigin(tenantID, instanceID, "account-1", 1, "group", "group-101", "actor-1", ""); err == nil {
		t.Fatal("missing provider event identity must fail")
	}
}

func TestOriginRoundTripsFailClosed(t *testing.T) {
	origin, err := NewZaloPersonalOrigin(uuid.New(), uuid.New(), "account-1", 3, "group", "group-101", "actor-1", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Origin
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	roundTripped, err := NewZaloPersonalOrigin(decoded.TenantID(), decoded.ChannelInstanceID(), decoded.ProviderAccountID(), decoded.AccountEpoch(), decoded.ConversationKind(), decoded.ConversationID(), decoded.ActorID(), decoded.ProviderMessageID())
	if err != nil {
		t.Fatal(err)
	}
	if *roundTripped != decoded {
		t.Fatal("round trip changed the trusted tuple")
	}
	var forged map[string]any
	if err := json.Unmarshal(encoded, &forged); err != nil {
		t.Fatal(err)
	}
	forged["provider"] = "telegram"
	forgedBytes, _ := json.Marshal(forged)
	if err := json.Unmarshal(forgedBytes, new(Origin)); err == nil {
		t.Fatal("wire value naming another provider must fail the decode")
	}
	forged["provider"] = "zalo_personal"
	forged["provider_message_id"] = ""
	forgedBytes, _ = json.Marshal(forged)
	if err := json.Unmarshal(forgedBytes, new(Origin)); err == nil {
		t.Fatal("wire value without provider event identity must fail the decode")
	}
}
