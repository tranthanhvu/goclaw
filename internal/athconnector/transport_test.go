package athconnector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestSigner(t *testing.T) (*Signer, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := NewSigner("goclaw-ath", "development", "key-1", privateKey, time.Now)
	if signer == nil {
		t.Fatal("signer construction failed")
	}
	return signer, ed25519.PublicKey(publicKey)
}

func connectorTestConfig(gatewayURL string) Config {
	return Config{
		Enabled: true, RuntimeRole: RuntimeRoleConnector, GatewayURL: gatewayURL,
		ConnectorID: uuid.New(), Environment: "development", Issuer: "goclaw-ath",
		KeyID: "key-1", PrivateKeyFile: "/run/secrets/ath-ed25519",
	}
}

func bearerClaims(t *testing.T, authorization string) map[string]any {
	t.Helper()
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok {
		t.Fatalf("authorization is not bearer: %q", authorization)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("authorization is not a compact JWS: %q", token)
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("claims segment invalid: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		t.Fatalf("claims invalid: %v", err)
	}
	return claims
}

func TestControlClientSignsPinnedRouteAndParsesEnvelope(t *testing.T) {
	signer, _ := newTestSigner(t)
	var gotPath, gotAuth, gotContentType string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotBody = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"req-1","data":{"ok":true}}`))
	}))
	defer server.Close()

	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	scope := AssertionScope{ConnectorID: client.Config().ConnectorID, AccountID: uuid.New(), AccountEpoch: 1, PeerKind: "group", ConversationID: "group-101"}
	if err := client.Call(context.Background(), PathApprovalRequests, "approval_request.submit", scope, map[string]string{"ping": "1"}, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Fatal("envelope data not parsed")
	}
	if gotPath != PathApprovalRequests {
		t.Fatalf("request hit %q, want pinned path", gotPath)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content type %q", gotContentType)
	}
	claims := bearerClaims(t, gotAuth)
	if claims["action"] != "approval_request.submit" || claims["path"] != PathApprovalRequests || claims["method"] != "POST" {
		t.Fatalf("route binding missing from assertion: %#v", claims)
	}
	if claims["aud"] != "ath-connector-control:development" {
		t.Fatalf("audience not environment-pinned: %v", claims["aud"])
	}
	var body map[string]string
	if err := json.Unmarshal(gotBody, &body); err != nil || body["ping"] != "1" {
		t.Fatalf("body not delivered verbatim: %s", gotBody)
	}
}

func TestControlClientRefusesRedirects(t *testing.T) {
	signer, _ := newTestSigner(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://evil.example.test/")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Call(context.Background(), PathContext, "context.inspect", AssertionScope{ConnectorID: client.Config().ConnectorID, AccountID: uuid.New(), AccountEpoch: 1, PeerKind: "group", ConversationID: "g"}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("redirect must fail closed, got: %v", err)
	}
}

func TestControlClientMapsFailureEnvelope(t *testing.T) {
	signer, _ := newTestSigner(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"request_id":"req-9","error":{"code":"invalid_assertion","message":"signature rejected"}}`))
	}))
	defer server.Close()
	client, err := NewControlClient(connectorTestConfig(server.URL), signer)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Call(context.Background(), PathContext, "context.inspect", AssertionScope{ConnectorID: client.Config().ConnectorID, AccountID: uuid.New(), AccountEpoch: 1, PeerKind: "group", ConversationID: "g"}, map[string]string{}, nil)
	controlErr, ok := err.(*ControlError)
	if !ok {
		t.Fatalf("expected ControlError, got %v", err)
	}
	if controlErr.StatusCode != http.StatusUnauthorized || controlErr.ErrorCode != "invalid_assertion" || controlErr.Message != "signature rejected" || controlErr.RequestID != "req-9" {
		t.Fatalf("failure envelope not mapped: %+v", controlErr)
	}
}

func TestNewControlClientRejectsGenericRuntime(t *testing.T) {
	signer, _ := newTestSigner(t)
	cfg := connectorTestConfig("https://ath.example.test")
	cfg.RuntimeRole = RuntimeRoleGateway
	if _, err := NewControlClient(cfg, signer); err == nil {
		t.Fatal("generic gateway runtime must not build a control client")
	}
	if _, err := NewControlClient(connectorTestConfig("https://ath.example.test"), nil); err == nil {
		t.Fatal("missing signer must fail")
	}
}
