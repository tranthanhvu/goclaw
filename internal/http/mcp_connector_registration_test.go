package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/athconnector"
	"github.com/nextlevelbuilder/goclaw/internal/security"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

func init() { security.SetAllowLoopbackForTest(true) }

type connectorRegistrationStore struct {
	store.MCPServerStore
	created []*store.MCPServerData
	got     *store.MCPServerData
	updates []map[string]any
}

func (s *connectorRegistrationStore) CreateServer(_ context.Context, server *store.MCPServerData) error {
	s.created = append(s.created, server)
	return nil
}

func (s *connectorRegistrationStore) GetServer(_ context.Context, _ uuid.UUID) (*store.MCPServerData, error) {
	return s.got, nil
}

func (s *connectorRegistrationStore) UpdateServer(_ context.Context, _ uuid.UUID, updates map[string]any) error {
	s.updates = append(s.updates, updates)
	return nil
}

func validConnectorSettings() string {
	return `{"mode":"ath-connector","gateway_url":"http://127.0.0.1:9443","connector_id":"` +
		uuid.New().String() + `","environment":"production","issuer":"goclaw-ath","key_id":"key-1",` +
		`"private_key_file":"/run/secrets/ath-ed25519","channel_instance_id":"` + uuid.New().String() +
		`","purpose":"tenant_contract"}`
}

func runConnectorCreate(t *testing.T, h *MCPHandler, settings string, role string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"name":"ath-connector","transport":"streamable-http","url":"http://127.0.0.1:9443","settings":` + settings + `}`
	req := httptest.NewRequest(http.MethodPost, "/v1/mcp/servers", strings.NewReader(body))
	req = req.WithContext(store.WithRole(req.Context(), role))
	rec := httptest.NewRecorder()
	h.handleCreateServer(rec, req)
	return rec
}

func TestConnectorRegistrationRequiresOperatorRole(t *testing.T) {
	h := NewMCPHandler(&connectorRegistrationStore{}, nil, nil)

	if rec := runConnectorCreate(t, h, validConnectorSettings(), "member"); rec.Code != http.StatusForbidden {
		t.Fatalf("member must be forbidden, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := runConnectorCreate(t, h, validConnectorSettings(), "viewer"); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer must be forbidden, got %d", rec.Code)
	}
}

func TestConnectorRegistrationValidatesAndNeverEchoesSecretReference(t *testing.T) {
	st := &connectorRegistrationStore{}
	h := NewMCPHandler(st, nil, nil)

	// Redirectable gateway origin must fail validation.
	bad := `{"mode":"ath-connector","gateway_url":"http://127.0.0.1:9443/redirectable","connector_id":"` +
		uuid.New().String() + `","environment":"production","issuer":"goclaw-ath","key_id":"key-1",` +
		`"private_key_file":"/run/secrets/ath-ed25519","channel_instance_id":"` + uuid.New().String() +
		`","purpose":"tenant_contract"}`
	if rec := runConnectorCreate(t, h, bad, "admin"); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-exact origin must be rejected, got %d: %s", rec.Code, rec.Body.String())
	}

	rec := runConnectorCreate(t, h, validConnectorSettings(), "operator")
	if rec.Code != http.StatusCreated {
		t.Fatalf("operator registration must be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "/run/secrets/ath-ed25519") {
		t.Fatalf("created response must not echo the signer secret reference: %s", rec.Body.String())
	}
	if len(st.created) != 1 || string(st.created[0].Settings) == "" {
		t.Fatal("persisted settings missing")
	}
	if !strings.Contains(string(st.created[0].Settings), "private_key_file") {
		t.Fatal("persisted settings must keep the secret reference for the connector service")
	}
}

func TestConnectorSettingsAreRedactedOnRead(t *testing.T) {
	settings := json.RawMessage(validConnectorSettings())
	h := NewMCPHandler(&connectorRegistrationStore{got: &store.MCPServerData{Settings: settings}}, nil, nil)

	id := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/servers/"+id.String(), nil)
	req.SetPathValue("id", id.String())
	rec := httptest.NewRecorder()
	h.handleGetServer(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get failed: %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "/run/secrets/ath-ed25519") {
		t.Fatalf("read must strip the signer secret reference: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "ath-connector") {
		t.Fatalf("redaction must keep the non-secret registration fields: %s", rec.Body.String())
	}
}

func TestConnectorUpdateWithoutSecretKeepsStoredReference(t *testing.T) {
	full := json.RawMessage(validConnectorSettings())
	st := &connectorRegistrationStore{got: &store.MCPServerData{Settings: full}}
	h := NewMCPHandler(st, nil, nil)

	var redacted map[string]any
	if err := json.Unmarshal(athconnector.RedactSettings(full), &redacted); err != nil {
		t.Fatal(err)
	}
	delete(redacted, "private_key_file") // the UI round-trips exactly what the API returned
	body, err := json.Marshal(map[string]any{"settings": redacted})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	req := httptest.NewRequest(http.MethodPut, "/v1/mcp/servers/"+id.String(), bytes.NewReader(body))
	req.SetPathValue("id", id.String())
	req = req.WithContext(store.WithRole(req.Context(), "admin"))
	rec := httptest.NewRecorder()
	h.handleUpdateServer(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update must succeed without the write-only secret, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(st.updates) != 1 {
		t.Fatal("update not recorded")
	}
	settings, _ := st.updates[0]["settings"].(map[string]any)
	if settings["private_key_file"] != "/run/secrets/ath-ed25519" {
		t.Fatalf("stored secret reference must survive edits that omit it: %#v", settings)
	}
}
