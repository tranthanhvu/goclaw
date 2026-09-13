package athconnector

import (
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
