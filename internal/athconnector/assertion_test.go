package athconnector

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSignerBindsAssertionToActionBodyAndTrustedScope(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	signer := NewSigner("issuer-1", "development", "key-1", privateKey, func() time.Time { return now })
	scope := AssertionScope{
		ConnectorID: uuid.New(), AccountID: uuid.New(), AccountEpoch: 2,
		PeerKind: "group", ConversationID: "group-101",
	}
	token, err := signer.Sign("POST", "/v1/connector/context", "context.inspect", []byte(`{"account_id":"a"}`), scope)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid compact JWS: %q", token)
	}
	decode := func(part string, dst any) {
		value, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || json.Unmarshal(value, dst) != nil {
			t.Fatalf("invalid JWS JSON segment")
		}
	}
	var header map[string]any
	var claims map[string]any
	decode(parts[0], &header)
	decode(parts[1], &claims)
	if header["alg"] != "EdDSA" || header["typ"] != "ath-connector+jwt" || header["kid"] != "key-1" {
		t.Fatalf("unexpected header: %#v", header)
	}
	if claims["action"] != "context.inspect" || claims["path"] != "/v1/connector/context" || claims["method"] != "POST" {
		t.Fatalf("route binding missing: %#v", claims)
	}
	if claims["aud"] != "ath-connector-control:development" || claims["iss"] != "issuer-1" {
		t.Fatalf("trust binding missing: %#v", claims)
	}
	signed := []byte(parts[0] + "." + parts[1])
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if !ed25519.Verify(publicKey, signed, signature) {
		t.Fatal("signature verification failed")
	}
	if int64(claims["exp"].(float64)-claims["iat"].(float64)) > 60 || len(claims["jti"].(string)) != 32 {
		t.Fatal("assertion lifetime or nonce is outside contract")
	}
}
